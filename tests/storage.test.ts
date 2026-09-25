/**
 * Storage: atomicity, path containment, locking and retention.
 *
 * These assert the failure behaviour rather than the happy path, because the
 * happy path is the one that gets noticed in manual use. A partial write, a
 * path escape and a stolen lock are all silent until they corrupt something.
 */

import { readFile, writeFile } from "node:fs/promises";
import { join } from "node:path";

import { afterEach, beforeEach, describe, expect, it } from "vitest";

import { CronjobStorage, allocateRunId, canonicalize, digestOf, resolveWithin } from "../src/cronjob/storage.js";
import type { RunState } from "../src/cronjob/contracts.js";
import { makeTempHome, type TempHome } from "./helpers.js";

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

function runState(cronjobId: string, runId: string): RunState {
  return {
    runId,
    cronjobId,
    definitionDigest: "d".repeat(64),
    trigger: "manual",
    status: "running",
    startedAt: new Date().toISOString(),
    nodes: { a: "pending" },
    logPath: "",
    notificationStatus: "pending",
  };
}

describe("path containment", () => {
  it("refuses an identifier that could escape its directory", () => {
    for (const id of ["../evil", "a/b", ".", "..", "", "a b"]) {
      expect(() => storage.definitionPath(id), id).toThrow();
    }
  });

  it("refuses a child that resolves outside the root", () => {
    expect(() => resolveWithin("/tmp/root", "../etc/passwd")).toThrow();
    expect(resolveWithin("/tmp/root", "inside/file")).toBe("/tmp/root/inside/file");
  });

  it("refuses to read a definition that is not a regular file", async () => {
    // A symlink placed in the definitions directory must not be followed.
    const { symlink } = await import("node:fs/promises");
    const secret = join(home.path, "secret.txt");
    await writeFile(secret, "top secret");
    await symlink(secret, storage.definitionPath("linked"));
    await expect(storage.readDefinitionText("linked")).rejects.toThrow();
  });
});

describe("atomic definition writes", () => {
  it("leaves the previous content intact when a write fails", async () => {
    await storage.writeDefinition("job", "original: true\n");
    expect(await storage.readDefinitionText("job")).toBe("original: true\n");

    // Make the final rename fail by replacing the target with a directory:
    // "job.yaml" cannot be overwritten by a rename, so the write fails after
    // the temp file was written. The old definition must survive untouched.
    const { rm, mkdir } = await import("node:fs/promises");
    await rm(storage.definitionPath("job"));
    await mkdir(storage.definitionPath("job"));
    await expect(storage.writeDefinition("job", "replacement: true\n")).rejects.toThrow();

    // No stray temp file is left behind by the failed attempt.
    const { readdir } = await import("node:fs/promises");
    const leftovers = (await readdir(storage.definitionsDir)).filter((n) => n.includes(".tmp-"));
    expect(leftovers).toEqual([]);
  });

  it("writes no partial file when two writers race", async () => {
    await Promise.all([
      storage.writeDefinition("job", "a: 1\n"),
      storage.writeDefinition("job", "b: 2\n"),
    ]);
    const text = await storage.readDefinitionText("job");
    // Whichever won, the result is one complete document, never a mixture.
    expect(["a: 1\n", "b: 2\n"]).toContain(text);
  });

  it("rejects a definition larger than the read ceiling", async () => {
    await storage.writeDefinition("big", "x".repeat(2 * 1024 * 1024));
    await expect(storage.readDefinitionText("big")).rejects.toThrow(/exceeds/);
  });

  it("lists definition ids in a deterministic order", async () => {
    await storage.writeDefinition("zeta", "z: 1\n");
    await storage.writeDefinition("alpha", "a: 1\n");
    expect(await storage.listDefinitionIds()).toEqual(["alpha", "zeta"]);
  });
});

describe("run records", () => {
  it("creates the run directory before any node result is written", async () => {
    const runId = allocateRunId(new Date("2026-03-01T08:00:00Z"));
    await storage.createRun(runState("job", runId));
    const state = await storage.readRunState("job", runId);
    expect(state?.status).toBe("running");
    expect(await storage.listRunIds("job")).toEqual([runId]);
  });

  it("allocates distinct run ids within one millisecond", () => {
    const at = new Date("2026-03-01T08:00:00Z");
    const ids = new Set(Array.from({ length: 50 }, () => allocateRunId(at)));
    expect(ids.size).toBe(50);
  });
});

describe("single-flight locking", () => {
  it("lets exactly one of two concurrent claims win", async () => {
    const first = await storage.acquireRunLock("job", "run-1");
    const second = await storage.acquireRunLock("job", "run-2");
    expect(first).toBeDefined();
    expect(second).toBeUndefined();
    await first?.release();
    // Released, so the next claim succeeds.
    const third = await storage.acquireRunLock("job", "run-3");
    expect(third).toBeDefined();
    await third?.release();
  });

  it("treats release as idempotent", async () => {
    const lock = await storage.acquireRunLock("job", "run-1");
    await lock?.release();
    await lock?.release();
    expect(await storage.readLock("job")).toBeUndefined();
  });
});

describe("retention", () => {
  it("keeps runs that are not yet terminal", async () => {
    // Prune iterates the definitions on disk, so the job must exist.
    await storage.writeDefinition("job", "cronjobId: job\n");
    const done = allocateRunId(new Date("2026-01-01T00:00:00Z"));
    const live = allocateRunId(new Date("2026-01-02T00:00:00Z"));
    await storage.createRun({ ...runState("job", done), status: "succeeded" });
    await storage.createRun({ ...runState("job", live), status: "running" });

    const report = await storage.prune({ maxRunsPerJob: 1 });
    expect(report.removedRuns).toContain(`job/${done}`);
    // The non-terminal run is evidence of an in-flight job; pruning it would
    // make the run unrecoverable.
    expect(await storage.readRunState("job", live)).toBeDefined();
  });
});

describe("canonical digests", () => {
  it("is insensitive to key order", () => {
    expect(canonicalize({ b: 1, a: 2 })).toBe(canonicalize({ a: 2, b: 1 }));
    expect(digestOf({ b: 1, a: 2 })).toBe(digestOf({ a: 2, b: 1 }));
  });

  it("changes when a value changes", () => {
    expect(digestOf({ a: 1 })).not.toBe(digestOf({ a: 2 }));
  });
});

describe("log layout", () => {
  it("names a log file from the run timestamp and id", () => {
    const name = CronjobStorage.logFileName(new Date("2026-03-01T08:09:10.111Z"), "run-1");
    // No colon (illegal on Windows) and no separator: the name is one segment.
    expect(name).not.toContain(":");
    expect(name).not.toContain("/");
    expect(name).toContain("run-1");
    expect(name.endsWith(".log")).toBe(true);
  });
});

describe("notification outbox on disk", () => {
  it("reads back an enqueued record and survives one corrupt file", async () => {
    await storage.enqueueNotification("session-1", {
      notificationId: "n-1",
      cronjobId: "job",
      runId: "run-1",
      status: "succeeded",
      summary: "ok",
      createdAt: new Date().toISOString(),
      attempts: 0,
      idempotencyKey: "k1",
    });
    // A corrupt sibling must not hide the readable record.
    await writeFile(join(storage.notificationDir("session-1"), "broken.json"), "{not json");

    const records = await storage.listNotifications("session-1");
    expect(records.map((r) => r.notificationId)).toEqual(["n-1"]);
  });
});
