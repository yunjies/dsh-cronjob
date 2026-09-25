/**
 * Mounting against the real Cordis runtime.
 *
 * Every other suite stubs the seam it exercises. This one does not: it composes
 * the two plugin rows onto an actual `Context` and asserts what the runtime
 * does in response — that `apply` is accepted synchronously, that the service
 * is resolvable by name, and that disposal removes it again.
 *
 * That matters because the failures here are specific to Cordis and invisible
 * to a stub: an `apply` that returns a Promise is rejected, a service published
 * without `provide` is unresolvable, and an effect that never returns a
 * disposer leaks into the next mount.
 */

import { join } from "node:path";

import { Context } from "@deepseek-ai/cordis";
import { afterEach, beforeEach, describe, expect, it } from "vitest";

import { apply as applyService, inject as serviceInject, name as serviceName } from "../src/cronjob/index.js";
import {
  apply as applyTools,
  inject as toolInject,
  name as toolName,
} from "../src/cronjob/tool-cronjob.js";
import { makeTempHome, type TempHome } from "./helpers.js";

let home: TempHome;
let fibers: { dispose(): Promise<void> }[];

beforeEach(async () => {
  home = await makeTempHome();
  fibers = [];
});

afterEach(async () => {
  // Dispose every fiber before pruning: `apply` starts the service
  // asynchronously, so removing the tree under it would surface as a cleanup
  // error and mask the assertion's real result.
  for (const fiber of fibers) {
    // A test may dispose its fiber explicitly; a second `dispose()` returns
    // undefined rather than a promise, so teardown must tolerate both. It is
    // best-effort here so cleanup can never mask the assertion's result.
    const result = fiber?.dispose?.();
    if (result !== undefined) await result.catch(() => undefined);
  }
  await new Promise((resolve) => setTimeout(resolve, 50));
  await home.cleanup();
});

/** Mount a row the way a composition does: with its declared `inject`. */
async function mountRow(
  root: Context,
  name: string,
  apply: (ctx: never) => void,
  inject: readonly string[] = [],
): Promise<{ dispose(): Promise<void>; state: number; await(): Promise<void> }> {
  const fiber = await root.plugin({ name, inject: [...inject], apply } as never);
  const typed = fiber as unknown as { dispose(): Promise<void>; state: number; await(): Promise<void> };
  if (typed === undefined) throw new Error(`mountRow(${name}) returned no fiber`);
  fibers.push(typed);
  return typed;
}

/** Mount a helper service and track its fiber for teardown. */
async function mountRaw(root: Context, plugin: unknown): Promise<void> {
  const fiber = await root.plugin(plugin as never);
  if (fiber !== undefined) fibers.push(fiber as unknown as { dispose(): Promise<void> });
}

/** Let the async startup inside `ctx.effect` settle. */
async function settle(): Promise<void> {
  await new Promise((resolve) => setTimeout(resolve, 50));
}

/** A timer service stand-in; the real one is `@cordisjs/plugin-timer`. */
function timerPlugin() {
  return {
    name: "timer-stub",
    apply(ctx: Context & { provide(name: string, value: unknown): void }) {
      ctx.provide("timer", {
        timeout(callback: () => void, delay: number) {
          const handle = setTimeout(callback, delay);
          if (typeof handle.unref === "function") handle.unref();
          return () => clearTimeout(handle);
        },
      });
    },
  };
}

describe("plugin row shape", () => {
  it("names the two rows distinctly", () => {
    // Two rows sharing a name would collide in a composition.
    expect(serviceName).toBe("cronjob");
    expect(toolName).toBe("tool-cronjob");
    expect(serviceName).not.toBe(toolName);
  });

  it("declares the services each row consumes", () => {
    expect([...serviceInject]).toEqual([]);
    expect([...toolInject].sort()).toEqual(["cronjobs", "tools"]);
  });

  it("does not return a Promise from apply", () => {
    // Cordis collects a plugin's effect synchronously and rejects a returned
    // Promise; a leaked promise here fails the mount at runtime.
    const root = new Context();
    const result = applyService(root as never, { dshHome: home.path });
    expect(result).toBeUndefined();
  });
});

describe("mounting the service row", () => {
  it("publishes a resolvable cronjobs service", async () => {
    const root = new Context();
    await mountRaw(root, timerPlugin());
    await mountRow(root, serviceName, (ctx) => applyService(ctx, { dshHome: home.path }));
    await settle();

    const service = (root as unknown as { cronjobs?: unknown }).cronjobs;
    expect(service).toBeDefined();
    expect((service as { list(): unknown[] }).list()).toEqual([]);
  });

  it("removes the service when the fiber stops", async () => {
    const root = new Context();
    await mountRaw(root, timerPlugin());
    const fiber = await mountRow(root, serviceName, (ctx) => applyService(ctx, { dshHome: home.path }));
    await settle();

    expect((root as unknown as { cronjobs?: unknown }).cronjobs).toBeDefined();
    await fiber.dispose();
    // A leaked registration would survive the unmount and collide on remount.
    expect((root as unknown as { cronjobs?: unknown }).cronjobs).toBeUndefined();
  });

  it("creates the storage layout under the given DSH home", async () => {
    const root = new Context();
    await mountRaw(root, timerPlugin());
    await mountRow(root, serviceName, (ctx) => applyService(ctx, { dshHome: home.path }));
    await settle();

    const { access } = await import("node:fs/promises");
    for (const dir of ["definitions", "artifacts", "runs", "logs", "notifications", "locks"]) {
      await expect(access(join(home.path, "cronjobs", dir))).resolves.toBeUndefined();
    }
  });
});

describe("mounting both rows together", () => {
  /** Register a minimal `tools` service so the tool row can activate. */
  function toolsPlugin() {
    const registered = new Map<string, unknown>();
    return {
      registered,
      plugin: {
        name: "tools-stub",
        apply(ctx: Context & { provide(name: string, value: unknown): void }) {
          ctx.provide("tools", {
            register(definition: unknown) {
              const name = (definition as { name: string }).name;
              registered.set(name, definition);
              let done = false;
              return () => {
                if (done) return;
                done = true;
                registered.delete(name);
              };
            },
          });
        },
      },
    };
  }

  it("activates the tool row once both its services exist", async () => {
    const root = new Context();
    const tools = toolsPlugin();
    await mountRaw(root, timerPlugin());
    await mountRaw(root, tools.plugin);
    await mountRow(root, serviceName, (ctx) => applyService(ctx, { dshHome: home.path }));
    await mountRow(root, toolName, (ctx) => applyTools(ctx), toolInject);
    await settle();

    // Nine tools, registered against the real registry seam.
    expect(tools.registered.size).toBe(9);
    expect([...tools.registered.keys()].sort()).toEqual(
      [
        "cronjob_delete",
        "cronjob_enable",
        "cronjob_get",
        "cronjob_list",
        "cronjob_log",
        "cronjob_run_now",
        "cronjob_runs",
        "cronjob_upsert",
        "cronjob_validate",
      ].sort(),
    );
  });

  it("waits rather than half-activating when a service is missing", async () => {
    const root = new Context();
    await mountRaw(root, timerPlugin());
    await mountRow(root, serviceName, (ctx) => applyService(ctx, { dshHome: home.path }));
    await settle();

    // No `tools` service: the row must stay pending instead of running apply
    // and reading an undefined registry.
    const fiber = await mountRow(root, toolName, (ctx) => applyTools(ctx), toolInject);
    await settle();
    // State 0 is PENDING (waiting on an injected service): the row must not
    // have run `apply`, which would read an undefined registry.
    expect(fiber.state).toBe(0);
  });

  it("leaves no tool registered after the tool row unloads", async () => {
    const root = new Context();
    const tools = toolsPlugin();
    await mountRaw(root, timerPlugin());
    await mountRaw(root, tools.plugin);
    await mountRow(root, serviceName, (ctx) => applyService(ctx, { dshHome: home.path }));
    const fiber = await mountRow(root, toolName, (ctx) => applyTools(ctx), toolInject);
    await settle();
    expect(tools.registered.size).toBe(9);

    await fiber.dispose();
    expect(tools.registered.size).toBe(0);
  });
});

describe("the shipped composition overlay", () => {
  it("names the rows that actually exist, on the right planes", async () => {
    const { readFile } = await import("node:fs/promises");
    const { fileURLToPath } = await import("node:url");
    const { join: joinPath } = await import("node:path");
    const YAML = await import("yaml");

    const here = joinPath(fileURLToPath(import.meta.url), "..");
    const text = await readFile(joinPath(here, "..", "examples", "cordis.overlay.yml"), "utf8");
    const rows = YAML.parse(text) as { id: string; name: string }[];

    // A row id that drifted from the plugin names would mount nothing, and the
    // failure would be silent until someone wondered why no tools appeared.
    expect(rows.map((row) => row.id).sort()).toEqual(["cronjob", "tool-cronjob"]);
    expect(rows.find((row) => row.id === "cronjob")?.name).toBe("@deepseek-ai/dsh-cronjob");
    // The consumer row must name the subpath entry, not the provider.
    expect(rows.find((row) => row.id === "tool-cronjob")?.name).toBe(
      "@deepseek-ai/dsh-cronjob/tool-cronjob",
    );
  });

  it("exports both entries the overlay names", async () => {
    const { readFile } = await import("node:fs/promises");
    const { fileURLToPath } = await import("node:url");
    const { join: joinPath } = await import("node:path");
    const here = joinPath(fileURLToPath(import.meta.url), "..");
    const manifest = JSON.parse(
      await readFile(joinPath(here, "..", "package.json"), "utf8"),
    ) as { exports: Record<string, unknown> };

    expect(Object.keys(manifest.exports)).toContain(".");
    expect(Object.keys(manifest.exports)).toContain("./tool-cronjob");
  });
});
