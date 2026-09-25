/**
 * Tool registration.
 *
 * These tests mount the real `defineTool` and a recording `tools` registry, so
 * they prove the schemas are accepted by the SDK's own validator rather than
 * merely that some object was pushed onto an array.
 *
 * The cleanup assertion matters as much as the registration one: a row that
 * registers nine tools and removes none of them breaks the next mount with a
 * duplicate-name failure.
 */

import { join } from "node:path";

import { afterEach, beforeEach, describe, expect, it } from "vitest";

import { CronjobService } from "../src/cronjob/service.js";
import { CRONJOB_TOOL_NAMES } from "../src/cronjob/tools.js";
import toolCronjob, { apply, inject, name } from "../src/cronjob/tool-cronjob.js";
import { createScripts, definitionYaml, fakeRunner, fakeTimers, makeTempHome, type TempHome } from "./helpers.js";

let home: TempHome;

beforeEach(async () => {
  home = await makeTempHome();
});

afterEach(async () => {
  await home.cleanup();
});

/** A `tools` registry that records definitions and honours disposers. */
function recordingRegistry() {
  const registered = new Map<string, { definition: Record<string, unknown>; disposed: boolean }>();
  const order: string[] = [];
  return {
    registered,
    order,
    register(definition: unknown) {
      const typed = definition as { name: string };
      if (registered.get(typed.name)?.disposed === false) {
        throw new Error(`duplicate tool name: ${typed.name}`);
      }
      registered.set(typed.name, { definition: definition as Record<string, unknown>, disposed: false });
      order.push(typed.name);
      let done = false;
      return () => {
        if (done) return;
        done = true;
        const entry = registered.get(typed.name);
        if (entry !== undefined) entry.disposed = true;
      };
    },
  };
}

function buildContext() {
  const timers = fakeTimers();
  const service = new CronjobService({
    dshHome: home.path,
    timers: timers.timers,
    agents: { findLive: () => undefined },
    runner: fakeRunner(() => ({ stdout: "ok" })),
    now: () => new Date("2026-03-01T08:00:00Z"),
  });
  const registry = recordingRegistry();
  const effects: (() => void)[] = [];
  const ctx = {
    cronjobs: service,
    tools: registry,
    effect(callback: () => () => void) {
      effects.push(callback());
    },
  };
  return { service, registry, ctx, effects };
}

describe("tool-cronjob plugin shape", () => {
  it("declares the row identity and its required services", () => {
    expect(name).toBe("tool-cronjob");
    // Both the service and the tools registry must be waited for, otherwise the
    // property reads below are illegal.
    expect([...inject].sort()).toEqual(["cronjobs", "tools"]);
    expect(toolCronjob.name).toBe(name);
  });

  it("registers exactly the documented tool names, in order", () => {
    const { ctx, registry } = buildContext();
    apply(ctx);
    expect(registry.order).toEqual([...CRONJOB_TOOL_NAMES]);
  });

  it("gives every tool a description and an output schema", () => {
    const { ctx, registry } = buildContext();
    apply(ctx);
    for (const [toolName, entry] of registry.registered) {
      expect(entry.definition["description"], toolName).toBeTypeOf("string");
      expect((entry.definition["description"] as string).length, toolName).toBeGreaterThan(20);
      expect(entry.definition["output"], toolName).toBeDefined();
      expect(entry.definition["execute"], toolName).toBeTypeOf("function");
    }
  });

  it("marks each job-scoped tool's id argument required", () => {
    const { ctx, registry } = buildContext();
    apply(ctx);
    for (const toolName of CRONJOB_TOOL_NAMES) {
      // `defineTool` compiles the DSL into standard JSON Schema, so requiredness
      // is an object-level `required` array of property names, not a per-property
      // flag — asserting the compiled shape is what proves the schema is legal.
      const parameters = registry.registered.get(toolName)?.definition["parameters"] as
        | { type?: string; properties?: Record<string, unknown>; required?: string[] }
        | undefined;
      expect(parameters?.type, toolName).toBe("object");
      if (toolName === "cronjob_list") {
        expect(Object.keys(parameters?.properties ?? {}), toolName).toEqual([]);
        continue;
      }
      expect(parameters?.properties?.["cronjobId"], toolName).toBeDefined();
      expect(parameters?.required, toolName).toContain("cronjobId");
    }
  });

  it("declares exactly the extra required arguments each tool needs", () => {
    const { ctx, registry } = buildContext();
    apply(ctx);
    const requiredOf = (toolName: string): string[] => {
      const parameters = registry.registered.get(toolName)?.definition["parameters"] as
        | { required?: string[] }
        | undefined;
      return [...(parameters?.required ?? [])].sort();
    };
    // The required set is the tool's real contract: an argument the handler
    // needs but does not require would arrive undefined at execution time.
    expect(requiredOf("cronjob_validate")).toEqual(["cronjobId", "yaml"]);
    expect(requiredOf("cronjob_upsert")).toEqual(["cronjobId", "yaml"]);
    expect(requiredOf("cronjob_list")).toEqual([]);
    expect(requiredOf("cronjob_get")).toEqual(["cronjobId"]);
    expect(requiredOf("cronjob_enable")).toEqual(["cronjobId", "enabled"]);
    expect(requiredOf("cronjob_delete")).toEqual(["cronjobId"]);
    expect(requiredOf("cronjob_run_now")).toEqual(["cronjobId"]);
    expect(requiredOf("cronjob_runs")).toEqual(["cronjobId"]);
    expect(requiredOf("cronjob_log")).toEqual(["cronjobId", "runId"]);
  });

  it("unregisters every tool when the effect is disposed", () => {
    const { ctx, registry, effects } = buildContext();
    apply(ctx);
    expect(registry.registered.size).toBe(CRONJOB_TOOL_NAMES.length);

    for (const dispose of effects) dispose();
    const stillLive = [...registry.registered.values()].filter((e) => !e.disposed);
    expect(stillLive).toEqual([]);
  });

  it("can be mounted twice without a duplicate-name collision", () => {
    // This is the failure the ctx.effect teardown exists to prevent: a returned
    // disposer is keyed to the apply identity and is not re-collected.
    const { ctx, registry, effects } = buildContext();
    apply(ctx);
    for (const dispose of effects) dispose();
    expect(() => apply(ctx)).not.toThrow();
    expect(registry.registered.size).toBe(CRONJOB_TOOL_NAMES.length);
  });
});

describe("tool execution through the registry", () => {
  /** Invoke a registered tool the way the runtime does. */
  async function call(
    registry: ReturnType<typeof recordingRegistry>,
    toolName: string,
    args: Record<string, unknown>,
  ): Promise<Record<string, unknown>> {
    const definition = registry.registered.get(toolName)?.definition as {
      execute(args: unknown, exec: unknown): Promise<unknown>;
    };
    const controller = new AbortController();
    return (await definition.execute(args, { signal: controller.signal })) as Record<string, unknown>;
  }

  it("validates a definition through the tool", async () => {
    const { ctx, registry, service } = buildContext();
    await service.start();
    apply(ctx);

    const ok = await call(registry, "cronjob_validate", {
      cronjobId: "job",
      yaml: definitionYaml({ cronjobId: "job" }),
    });
    expect(ok["ok"]).toBe(true);

    const bad = await call(registry, "cronjob_validate", {
      cronjobId: "job",
      yaml: definitionYaml({ cronjobId: "job", scheduleTime: "bogus" }),
    });
    expect(bad["ok"]).toBe(false);
    expect(JSON.stringify(bad)).toContain("invalid_cron");
  });

  it("saves, lists, reads and deletes through the tools", async () => {
    const { ctx, registry, service } = buildContext();
    await service.start();
    apply(ctx);

    const saved = await call(registry, "cronjob_upsert", {
      cronjobId: "job",
      yaml: definitionYaml({ cronjobId: "job" }),
    });
    expect(saved["ok"]).toBe(true);

    const listed = await call(registry, "cronjob_list", {});
    expect(listed["count"]).toBe(1);

    const got = await call(registry, "cronjob_get", { cronjobId: "job" });
    expect(got["ok"]).toBe(true);

    const deleted = await call(registry, "cronjob_delete", { cronjobId: "job" });
    expect(deleted["ok"]).toBe(true);
    expect((await call(registry, "cronjob_get", { cronjobId: "job" }))["ok"]).toBe(false);
  });

  it("reports a not-found job as a tool error, not a crash", async () => {
    const { ctx, registry, service } = buildContext();
    await service.start();
    apply(ctx);

    const result = await call(registry, "cronjob_get", { cronjobId: "ghost" });
    expect(result["ok"]).toBe(false);
    expect(JSON.stringify(result)).toContain("cronjob_not_found");
  });

  it("runs a job now and reports the run status", async () => {
    const { ctx, registry, service } = buildContext();
    await service.start();
    await createScripts(service.storage.scriptsDir, ["collect.py"]);
    apply(ctx);
    await call(registry, "cronjob_upsert", {
      cronjobId: "job",
      yaml: definitionYaml({ cronjobId: "job" }),
    });

    const result = await call(registry, "cronjob_run_now", { cronjobId: "job" });
    const run = result["run"] as { status: string };
    expect(run.status).toBe("succeeded");

    const runs = await call(registry, "cronjob_runs", { cronjobId: "job" });
    expect(runs["count"]).toBe(1);
  });

  it("refuses a cancelled call", async () => {
    const { ctx, registry, service } = buildContext();
    await service.start();
    apply(ctx);
    const definition = registry.registered.get("cronjob_list")?.definition as {
      execute(args: unknown, exec: unknown): Promise<unknown>;
    };
    const controller = new AbortController();
    controller.abort();
    const result = (await definition.execute({}, { signal: controller.signal })) as Record<string, unknown>;
    expect(result["ok"]).toBe(false);
    expect(JSON.stringify(result)).toContain("cancelled");
  });

  it("returns a structured error when the service is not mounted", async () => {
    const { ctx, registry, effects } = buildContext();
    apply(ctx);
    // Disposing clears the late-bound handle, which is the state after unmount.
    for (const dispose of effects) dispose();
    const definition = registry.registered.get("cronjob_list")?.definition as {
      execute(args: unknown, exec: unknown): Promise<unknown>;
    };
    const result = (await definition.execute({}, { signal: new AbortController().signal })) as Record<
      string,
      unknown
    >;
    expect(result["ok"]).toBe(false);
    expect(JSON.stringify(result)).toContain("service_unavailable");
  });
});

describe("definitions stay in step with the tool surface", () => {
  it("keeps every CRONJOB_TOOL_NAME reachable from the handler table", async () => {
    const { CRONJOB_TOOL_NAMES: names, CRONJOB_HANDLERS } = await import("../src/cronjob/tools.js");
    // A name without a handler would register a tool that throws on every call.
    for (const toolName of names) {
      expect(CRONJOB_HANDLERS[toolName], toolName).toBeTypeOf("function");
    }
    expect(Object.keys(CRONJOB_HANDLERS).sort()).toEqual([...names].sort());
  });
});

describe("example definitions on disk", () => {
  it("loads the shipped example through the service", async () => {
    const { readFile } = await import("node:fs/promises");
    const { fileURLToPath } = await import("node:url");
    const here = join(fileURLToPath(import.meta.url), "..");
    const text = await readFile(join(here, "..", "examples", "daily-report.yaml"), "utf8");

    const { ctx, service } = buildContext();
    await service.start();
    const result = await service.upsert("daily-report", text);
    expect(result.ok).toBe(true);
    expect(service.get("daily-report")?.nodeCount).toBe(2);
    // Referencing ctx keeps the fixture honest about what it built.
    expect(ctx.cronjobs).toBe(service);
  });
});

/** Invoke a registered tool the way the runtime does. */
async function callUntyped(
  registry: ReturnType<typeof recordingRegistry>,
  toolName: string,
  args: Record<string, unknown>,
): Promise<Record<string, unknown>> {
  const definition = registry.registered.get(toolName)?.definition as {
    execute(args: unknown, exec: unknown): Promise<unknown>;
  };
  return (await definition.execute(args, { signal: new AbortController().signal })) as Record<
    string,
    unknown
  >;
}

describe("log retrieval end to end", () => {
  it("reads a run's log through the tool after a real run", async () => {
    const { ctx, registry, service } = buildContext();
    await service.start();
    await createScripts(service.storage.scriptsDir, ["collect.py"]);
    apply(ctx);

    await callUntyped(registry, "cronjob_upsert", {
      cronjobId: "job",
      yaml: definitionYaml({ cronjobId: "job" }),
    });
    const run = await callUntyped(registry, "cronjob_run_now", { cronjobId: "job" });
    const runId = (run["run"] as { runId: string }).runId;

    const log = await callUntyped(registry, "cronjob_log", { cronjobId: "job", runId });
    expect(log["ok"]).toBe(true);
    expect(String(log["log"])).toContain("run.succeeded");
  });

  it("reports a missing log rather than returning empty text", async () => {
    const { ctx, registry, service } = buildContext();
    await service.start();
    apply(ctx);
    const result = await callUntyped(registry, "cronjob_log", {
      cronjobId: "job",
      runId: "no-such-run",
    });
    expect(result["ok"]).toBe(false);
    expect(JSON.stringify(result)).toContain("log_not_found");
  });
});
