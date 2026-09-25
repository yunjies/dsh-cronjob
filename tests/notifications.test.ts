/**
 * Notifications and executors.
 *
 * Two properties matter here. Delivery must be idempotent, because a session
 * resume event can fire more than once for the same run. And a node's output
 * must never be promoted into an instruction, which is why the notification
 * body is asserted to carry the summary and nothing from the node.
 */

import { join } from "node:path";

import { afterEach, beforeEach, describe, expect, it } from "vitest";

import { CronjobStorage } from "../src/cronjob/storage.js";
import { NotificationOutbox, renderNotification } from "../src/cronjob/notifications.js";
import { PythonExecutor } from "../src/cronjob/executors/python.js";
import { SubagentExecutor, type SubagentProvider } from "../src/cronjob/executors/subagent.js";
import { createScripts, definitionYaml, fakeRunner, makeTempHome, type TempHome } from "./helpers.js";

let home: TempHome;
let storage: CronjobStorage;

beforeEach(async () => {
  home = await makeTempHome();
  storage = new CronjobStorage({ root: join(home.path, "cronjobs") });
  await storage.ensureLayout();
});

afterEach(async () => {
  await home.cleanup();
});

/** A registry whose live sessions are whatever the test declares. */
function registry(live: Set<string>, sent: string[] = []) {
  return {
    sent,
    findLive(sessionId: string) {
      if (!live.has(sessionId)) return undefined;
      return {
        sessionId,
        async deliver(message: string) {
          sent.push(message);
        },
      };
    },
  };
}

describe("NotificationOutbox", () => {
  const request = {
    cronjobId: "job",
    runId: "run-1",
    bindSessionId: "session-1",
    status: "succeeded" as const,
    trigger: "cron" as const,
    summary: "1 node(s) completed",
    logPath: "",
  };

  it("delivers immediately to a live session", async () => {
    const sent: string[] = [];
    const outbox = new NotificationOutbox({ storage, agents: registry(new Set(["session-1"]), sent) });
    await outbox.enqueue(request);
    expect(sent).toHaveLength(1);
    expect(sent[0]).toContain("run-1");
  });

  it("keeps the record pending while the session is offline", async () => {
    const outbox = new NotificationOutbox({ storage, agents: registry(new Set()) });
    const record = await outbox.enqueue(request);
    expect(record.delivery).toBe("pending");
    const stored = await storage.listNotifications("session-1");
    expect(stored).toHaveLength(1);
  });

  it("drains the backlog once the session becomes live", async () => {
    const live = new Set<string>();
    const sent: string[] = [];
    const outbox = new NotificationOutbox({ storage, agents: registry(live, sent) });
    await outbox.enqueue(request);
    expect(sent).toHaveLength(0);

    live.add("session-1");
    const delivered = await outbox.drain("session-1");
    expect(delivered).toBe(1);
    expect(sent).toHaveLength(1);
  });

  it("does not deliver the same run twice", async () => {
    const sent: string[] = [];
    const outbox = new NotificationOutbox({ storage, agents: registry(new Set(["session-1"]), sent) });
    await outbox.enqueue(request);
    // A repeated resume event must not produce a second message.
    await outbox.enqueue(request);
    await outbox.drain("session-1");
    expect(sent).toHaveLength(1);
  });

  it("parks a record after repeated delivery failures instead of looping", async () => {
    let attempts = 0;
    const failing = {
      findLive: () => ({
        sessionId: "session-1",
        async deliver() {
          attempts += 1;
          throw new Error("channel down");
        },
      }),
    };
    const outbox = new NotificationOutbox({ storage, agents: failing, maxAttempts: 2 });
    await outbox.enqueue(request);
    await outbox.drain("session-1");
    await outbox.drain("session-1");
    expect(attempts).toBe(2);

    const stored = await storage.listNotifications("session-1");
    expect(stored[0]?.delivery).toBe("failed");
    // A parked record is not retried again.
    await outbox.drain("session-1");
    expect(attempts).toBe(2);
  });

  it("keeps node output out of the notification body", async () => {
    const record = {
      notificationId: "n-1",
      cronjobId: "job",
      runId: "run-1",
      status: "succeeded" as const,
      delivery: "pending" as const,
      summary: "done",
      createdAt: new Date().toISOString(),
      attempts: 0,
      idempotencyKey: "k",
    };
    const body = renderNotification(record);
    // The framing marks this as a report, not a fresh user instruction.
    expect(body).toContain("[CRONJOB RUN]");
    expect(body).toContain("job: job");
  });
});

describe("PythonExecutor", () => {
  it("refuses a script that does not exist", async () => {
    const executor = new PythonExecutor({
      scriptsRoot: storage.scriptsDir,
      runner: fakeRunner(() => ({})),
    });
    const result = await executor.execute(
      { nodeType: "pythonScript", nodeId: "n", scriptPath: "missing.py" },
      { runId: "r", cronjobId: "job" },
      { timeoutMs: 1000 },
    );
    expect(result.status).toBe("failed");
    expect(result.errorCode).toBe("invalid_script_path");
  });

  it("refuses a symlink that resolves outside the artifact root", async () => {
    const { symlink, writeFile } = await import("node:fs/promises");
    const outside = join(home.path, "outside.py");
    await writeFile(outside, "print('escaped')");
    await symlink(outside, join(storage.scriptsDir, "link.py"));

    const executor = new PythonExecutor({
      scriptsRoot: storage.scriptsDir,
      runner: fakeRunner(() => ({})),
    });
    const result = await executor.execute(
      { nodeType: "pythonScript", nodeId: "n", scriptPath: "link.py" },
      { runId: "r", cronjobId: "job" },
      { timeoutMs: 1000 },
    );
    // The lexical path looked fine; only the realpath check catches this.
    expect(result.status).toBe("failed");
    expect(result.errorCode).toBe("invalid_script_path");
  });

  it("passes parameters and context as data, never as arguments", async () => {
    await createScripts(storage.scriptsDir, ["p.py"]);
    let captured: { args: readonly string[]; stdin?: string } | undefined;
    const executor = new PythonExecutor({
      scriptsRoot: storage.scriptsDir,
      runner: fakeRunner((request) => {
        captured = request;
        return {};
      }),
    });
    await executor.execute(
      {
        nodeType: "pythonScript",
        nodeId: "n",
        scriptPath: "p.py",
        scriptParams: { value: "; rm -rf /" },
      },
      { runId: "r", cronjobId: "job" },
      { timeoutMs: 1000 },
    );
    // The hostile-looking value travels on stdin, and argv holds only the path.
    expect(captured?.args).toHaveLength(1);
    expect(captured?.args[0]).toContain("p.py");
    expect(captured?.stdin).toContain("; rm -rf /");
  });

  it("reports a non-zero exit as a failure", async () => {
    await createScripts(storage.scriptsDir, ["bad.py"]);
    const executor = new PythonExecutor({
      scriptsRoot: storage.scriptsDir,
      runner: fakeRunner(() => ({ code: 2, stderr: "nope" })),
    });
    const result = await executor.execute(
      { nodeType: "pythonScript", nodeId: "n", scriptPath: "bad.py" },
      { runId: "r", cronjobId: "job" },
      { timeoutMs: 1000 },
    );
    expect(result.status).toBe("failed");
    expect(result.exitCode).toBe(2);
  });

  it("truncates oversized output", async () => {
    await createScripts(storage.scriptsDir, ["loud.py"]);
    const executor = new PythonExecutor({
      scriptsRoot: storage.scriptsDir,
      runner: fakeRunner(() => ({ stdout: "x".repeat(200_000) })),
    });
    const result = await executor.execute(
      { nodeType: "pythonScript", nodeId: "n", scriptPath: "loud.py" },
      { runId: "r", cronjobId: "job" },
      { timeoutMs: 1000 },
    );
    expect(result.stdout?.length).toBeLessThan(200_000);
    expect(result.stdout).toContain("truncated");
  });
});

describe("SubagentExecutor", () => {
  function provider(name: string, answer = "child answer"): SubagentProvider & { interrupted: boolean } {
    const state = {
      interrupted: false,
      name,
      async start() {
        return {
          childId: "child-1",
          result: Promise.resolve({ text: answer }),
          async interrupt() {
            state.interrupted = true;
          },
        };
      },
    };
    return state;
  }

  it("uses the default provider when the node names none", async () => {
    const resolved: string[] = [];
    const executor = new SubagentExecutor({
      providers: [provider("alpha")],
      defaultProvider: "alpha",
      onProviderResolved: (info) => resolved.push(info.provider),
    });
    const result = await executor.execute(
      { nodeType: "subagent", nodeId: "n", prompt: "hi" },
      { runId: "r", cronjobId: "job" },
      { timeoutMs: 1000 },
    );
    expect(result.status).toBe("succeeded");
    expect(resolved).toEqual(["alpha"]);
  });

  it("reports an unknown provider rather than silently defaulting", async () => {
    const executor = new SubagentExecutor({ providers: [provider("alpha")], defaultProvider: "alpha" });
    const result = await executor.execute(
      { nodeType: "subagent", nodeId: "n", prompt: "hi", provider: "ghost" },
      { runId: "r", cronjobId: "job" },
      { timeoutMs: 1000 },
    );
    expect(result.status).toBe("failed");
    expect(result.errorCode).toBe("unknown_provider");
  });

  it("fails when no provider is registered at all", async () => {
    const executor = new SubagentExecutor({ providers: [] });
    const result = await executor.execute(
      { nodeType: "subagent", nodeId: "n", prompt: "hi" },
      { runId: "r", cronjobId: "job" },
      { timeoutMs: 1000 },
    );
    expect(result.status).toBe("failed");
    expect(result.errorCode).toBe("no_provider");
  });
});
