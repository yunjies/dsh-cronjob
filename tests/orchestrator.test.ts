/**
 * Orchestration: claim ordering, re-validation, terminal states and cleanup.
 *
 * The scenarios chosen are the ones where a plausible implementation is wrong:
 * two concurrent fires, a definition broken between load and fire, a node that
 * fails midway, and a lock that must be released on every path.
 */

import { writeFile } from "node:fs/promises";
import { join } from "node:path";

import { afterEach, beforeEach, describe, expect, it } from "vitest";

import { CronjobStorage } from "../src/cronjob/storage.js";
import { Orchestrator } from "../src/cronjob/orchestrator.js";
import { PythonExecutor } from "../src/cronjob/executors/python.js";
import { SubagentExecutor } from "../src/cronjob/executors/subagent.js";
import { makeTempHome, definitionYaml, fakeRunner, createScripts, type TempHome } from "./helpers.js";

let home: TempHome;
let storage: CronjobStorage;

beforeEach(async () => {
  home = await makeTempHome();
  storage = new CronjobStorage({ root: join(home.path, "cronjobs") });
  await storage.ensureLayout();
  // The executor resolves scripts through realpath, so every path a test
  // definition names must exist under the artifact root.
  await createScripts(storage.scriptsDir, ["collect.py", "a.py", "b.py"]);
});

afterEach(async () => {
  await home.cleanup();
});

function build(
  options: {
    readonly respond?: Parameters<typeof fakeRunner>[0];
    readonly maxGlobalConcurrency?: number;
  } = {},
): Orchestrator {
  return new Orchestrator({
    storage,
    python: new PythonExecutor({
      scriptsRoot: storage.scriptsDir,
      runner: fakeRunner(options.respond ?? (() => ({ stdout: "ok" }))),
    }),
    subagent: new SubagentExecutor({ providers: [] }),
    ...(options.maxGlobalConcurrency === undefined
      ? {}
      : { maxGlobalConcurrency: options.maxGlobalConcurrency }),
  });
}

describe("requestRun", () => {
  it("runs a well-formed job to success and records the terminal state", async () => {
    await storage.writeDefinition("job", definitionYaml({ cronjobId: "job" }));
    const outcome = await build().requestRun("job", "manual");

    expect(outcome.status).toBe("succeeded");
    const state = await storage.readRunState("job", outcome.runId);
    expect(state?.status).toBe("succeeded");
    expect(state?.nodes["collect"]).toBe("succeeded");
  });

  it("rejects a definition that is invalid at fire time", async () => {
    // The file is broken after it would have been loaded, which is exactly the
    // window that re-validation exists to cover.
    await storage.writeDefinition("job", definitionYaml({ cronjobId: "job", scheduleTime: "bogus" }));
    const outcome = await build().requestRun("job", "cron");

    expect(outcome.status).toBe("rejected");
    expect(outcome.errorCode).toBe("invalid_cron");
  });

  it("rejects a missing definition", async () => {
    const outcome = await build().requestRun("absent", "cron");
    expect(outcome.status).toBe("rejected");
    expect(outcome.errorCode).toBe("definition_missing");
  });

  it("skips a disabled job without executing it", async () => {
    await storage.writeDefinition("job", definitionYaml({ cronjobId: "job", enabled: false }));
    let ran = false;
    const orchestrator = new Orchestrator({
      storage,
      python: new PythonExecutor({
        scriptsRoot: storage.scriptsDir,
        runner: fakeRunner(() => {
          ran = true;
          return {};
        }),
      }),
      subagent: new SubagentExecutor({ providers: [] }),
    });
    const outcome = await orchestrator.requestRun("job", "cron");
    expect(outcome.status).toBe("skipped");
    expect(outcome.errorCode).toBe("job_disabled");
    expect(ran).toBe(false);
  });

  it("stops at the first failing node and records the failure", async () => {
    await storage.writeDefinition(
      "job",
      definitionYaml({
        cronjobId: "job",
        nodes: [
          "    - nodeId: first",
          "      nodeType: pythonScript",
          '      scriptPath: "a.py"',
          "    - nodeId: second",
          "      nodeType: pythonScript",
          '      scriptPath: "b.py"',
        ].join("\n"),
      }),
    );
    const seen: string[] = [];
    const orchestrator = new Orchestrator({
      storage,
      python: new PythonExecutor({
        scriptsRoot: storage.scriptsDir,
        runner: fakeRunner((request) => {
          seen.push(request.args[0] ?? "");
          return request.args[0]?.endsWith("a.py") ? { code: 3, stderr: "boom" } : {};
        }),
      }),
      subagent: new SubagentExecutor({ providers: [] }),
    });

    const outcome = await orchestrator.requestRun("job", "cron");
    expect(outcome.status).toBe("failed");
    expect(outcome.errorCode).toBe("node_exit_nonzero");
    // The second node must not have been reached.
    expect(seen).toHaveLength(1);
  });

  it("lets exactly one of two concurrent fires execute", async () => {
    await storage.writeDefinition("job", definitionYaml({ cronjobId: "job" }));
    let concurrent = 0;
    let peak = 0;
    const orchestrator = new Orchestrator({
      storage,
      python: new PythonExecutor({
        scriptsRoot: storage.scriptsDir,
        runner: fakeRunner(async () => {
          concurrent += 1;
          peak = Math.max(peak, concurrent);
          await new Promise((resolve) => setTimeout(resolve, 20));
          concurrent -= 1;
          return {};
        }),
      }),
      subagent: new SubagentExecutor({ providers: [] }),
    });

    const [a, b] = await Promise.all([
      orchestrator.requestRun("job", "manual"),
      orchestrator.requestRun("job", "manual"),
    ]);
    // One runs; the other is turned away rather than executing alongside it.
    const statuses = [a.status, b.status].sort();
    expect(statuses).toEqual(["skipped", "succeeded"]);
    expect(peak).toBe(1);
  });

  it("skips when the global concurrency ceiling is reached", async () => {
    await storage.writeDefinition("job-a", definitionYaml({ cronjobId: "job-a" }));
    await storage.writeDefinition("job-b", definitionYaml({ cronjobId: "job-b" }));
    const orchestrator = new Orchestrator({
      storage,
      python: new PythonExecutor({
        scriptsRoot: storage.scriptsDir,
        runner: fakeRunner(async () => {
          await new Promise((resolve) => setTimeout(resolve, 30));
          return {};
        }),
      }),
      subagent: new SubagentExecutor({ providers: [] }),
      maxGlobalConcurrency: 1,
    });

    // Start the first run and let it reach its node before the second asks,
    // so the ceiling is genuinely occupied at the moment of the second claim.
    const first = orchestrator.requestRun("job-a", "manual");
    await new Promise((resolve) => setTimeout(resolve, 10));
    const second = await orchestrator.requestRun("job-b", "manual");
    const firstOutcome = await first;

    expect(firstOutcome.status).toBe("succeeded");
    expect(second.status).toBe("skipped");
    expect(second.errorCode).toBe("concurrency_limit");
  });

  it("releases the lock on every terminal path", async () => {
    await storage.writeDefinition("job", definitionYaml({ cronjobId: "job", scheduleTime: "bogus" }));
    const orchestrator = build();
    await orchestrator.requestRun("job", "cron");
    // A rejected run must not leave its lock behind, or the job is wedged.
    expect(await storage.readLock("job")).toBeUndefined();

    await storage.writeDefinition("job", definitionYaml({ cronjobId: "job" }));
    await orchestrator.requestRun("job", "cron");
    expect(await storage.readLock("job")).toBeUndefined();
  });

  it("writes a run record before the first node executes", async () => {
    await storage.writeDefinition("job", definitionYaml({ cronjobId: "job" }));
    let stateDuringRun: string | undefined;
    const orchestrator = new Orchestrator({
      storage,
      python: new PythonExecutor({
        scriptsRoot: storage.scriptsDir,
        runner: fakeRunner(async () => {
          const ids = await storage.listRunIds("job");
          const state = ids[0] === undefined ? undefined : await storage.readRunState("job", ids[0]);
          stateDuringRun = state?.status;
          return {};
        }),
      }),
      subagent: new SubagentExecutor({ providers: [] }),
    });
    await orchestrator.requestRun("job", "cron");
    // The record existed while the node was running, so a crash would leave a trace.
    expect(stateDuringRun).toBe("running");
  });

  it("times a node out and records timed_out", async () => {
    await storage.writeDefinition("job", definitionYaml({ cronjobId: "job", timeoutSeconds: 1 }));
    const orchestrator = new Orchestrator({
      storage,
      python: new PythonExecutor({
        scriptsRoot: storage.scriptsDir,
        runner: fakeRunner(() => ({ timedOut: true })),
      }),
      subagent: new SubagentExecutor({ providers: [] }),
    });
    const outcome = await orchestrator.requestRun("job", "cron");
    expect(outcome.status).toBe("timed_out");
  });

  it("cancels a run when the signal is already aborted", async () => {
    await storage.writeDefinition("job", definitionYaml({ cronjobId: "job" }));
    const controller = new AbortController();
    controller.abort();
    const outcome = await build().requestRun("job", "cron", { signal: controller.signal });
    expect(outcome.status).toBe("cancelled");
  });
});

describe("recovery", () => {
  it("marks a non-terminal run from a dead process as failed", async () => {
    await storage.writeDefinition("job", definitionYaml({ cronjobId: "job" }));
    // A lock owned by a pid that cannot be alive, plus a mid-flight run: this
    // is the shape a crashed host leaves behind.
    await storage.acquireRunLock("job", "stale-run");
    await writeFile(
      storage.lockPath("job"),
      JSON.stringify({ runId: "stale-run", pid: 999_999, at: new Date().toISOString() }),
    );
    await storage.createRun({
      runId: "stale-run",
      cronjobId: "job",
      definitionDigest: "d".repeat(64),
      trigger: "cron",
      status: "running",
      startedAt: new Date().toISOString(),
      nodes: { collect: "running" },
      logPath: "",
      notificationStatus: "pending",
    });

    const recovered = await build().recoverInterrupted(["job"]);
    expect(recovered).toContain("job/stale-run");
    const state = await storage.readRunState("job", "stale-run");
    expect(state?.status).toBe("failed");
    expect(state?.errorCode).toBe("host_restarted");
    // The abandoned lock is released so the job can run again.
    expect(await storage.readLock("job")).toBeUndefined();
  });

  it("leaves a terminal run untouched", async () => {
    await storage.writeDefinition("job", definitionYaml({ cronjobId: "job" }));
    await storage.createRun({
      runId: "done",
      cronjobId: "job",
      definitionDigest: "d".repeat(64),
      trigger: "cron",
      status: "succeeded",
      startedAt: new Date().toISOString(),
      finishedAt: new Date().toISOString(),
      nodes: {},
      logPath: "",
      notificationStatus: "delivered",
    });
    await build().recoverInterrupted(["job"]);
    expect((await storage.readRunState("job", "done"))?.status).toBe("succeeded");
  });

  it("does not steal a lock from a live process", async () => {
    await storage.writeDefinition("job", definitionYaml({ cronjobId: "job" }));
    // A lock owned by this very process is provably alive.
    await storage.acquireRunLock("job", "owned");
    const recovered = await build().recoverInterrupted(["job"]);
    expect(recovered).not.toContain("job/owned");
    expect(await storage.readLock("job")).toBeDefined();
  });
});
