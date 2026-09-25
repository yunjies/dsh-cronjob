/**
 * Shared fixtures.
 *
 * Every suite gets its own temp DSH home, so runs and definitions from one test
 * can never be seen by another. The temp root is removed on teardown.
 */

import { mkdir, mkdtemp, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { dirname, join } from "node:path";

import type { SubprocessRunner } from "../src/cronjob/executors/python.js";

export interface TempHome {
  readonly path: string;
  cleanup(): Promise<void>;
}

/** Create an isolated DSH home for one suite. */
export async function makeTempHome(prefix = "cronjob-test-"): Promise<TempHome> {
  const path = await mkdtemp(join(tmpdir(), prefix));
  return {
    path,
    async cleanup() {
      await rm(path, { recursive: true, force: true });
    },
  };
}

/** A minimal valid definition for one job. */
export function definitionYaml(
  overrides: {
    readonly cronjobId?: string;
    readonly scheduleTime?: string;
    readonly timeZone?: string;
    readonly enabled?: boolean;
    readonly bindSessionId?: string;
    readonly timeoutSeconds?: number;
    readonly nodes?: string;
  } = {},
): string {
  const id = overrides.cronjobId ?? "daily-report";
  const nodes =
    overrides.nodes ??
    [
      "    - nodeId: collect",
      "      nodeType: pythonScript",
      '      scriptPath: "collect.py"',
    ].join("\n");
  return [
    `- cronjobId: ${id}`,
    "  cronjobName: Daily report",
    `  scheduleTime: "${overrides.scheduleTime ?? "0 9 * * *"}"`,
    `  timeZone: ${overrides.timeZone ?? "UTC"}`,
    ...(overrides.bindSessionId === undefined
      ? []
      : [`  bindSessionId: ${overrides.bindSessionId}`]),
    `  enabled: ${overrides.enabled ?? true}`,
    `  timeoutSeconds: ${overrides.timeoutSeconds ?? 300}`,
    "  maxConcurrentRuns: 1",
    "  misfirePolicy: skip",
    "  workflow:",
    nodes,
    "",
  ].join("\n");
}

/** A subprocess runner whose responses are scripted by the test. */
export function fakeRunner(
  respond: (request: { readonly args: readonly string[]; readonly stdin?: string }) =>
    | { code?: number | null; stdout?: string; stderr?: string; timedOut?: boolean }
    | Promise<{ code?: number | null; stdout?: string; stderr?: string; timedOut?: boolean }>,
): SubprocessRunner {
  return {
    async run(request) {
      const result = await respond({ args: request.args, ...(request.stdin === undefined ? {} : { stdin: request.stdin }) });
      return {
        code: result.code === undefined ? 0 : result.code,
        signal: null,
        stdout: result.stdout ?? "",
        stderr: result.stderr ?? "",
        timedOut: result.timedOut ?? false,
        truncated: false,
      };
    },
  };
}

/** A deterministic clock a test can advance. */
export function fakeClock(start: Date): { now(): Date; advance(ms: number): void; set(at: Date): void } {
  let current = start.getTime();
  return {
    now: () => new Date(current),
    advance: (ms: number) => {
      current += ms;
    },
    set: (at: Date) => {
      current = at.getTime();
    },
  };
}

/**
 * A timer seam that records pending callbacks instead of waiting.
 *
 * Tests advance time explicitly by firing the callbacks they expect, which is
 * what makes schedule assertions deterministic rather than wall-clock races.
 */
export function fakeTimers(): {
  readonly timers: { timeout(callback: () => void, delay: number): () => void };
  readonly scheduled: { callback: () => void; delay: number; cancelled: boolean }[];
  fireAll(): void;
  readonly liveCount: number;
} {
  const scheduled: { callback: () => void; delay: number; cancelled: boolean }[] = [];
  return {
    timers: {
      timeout(callback, delay) {
        const entry = { callback, delay, cancelled: false };
        scheduled.push(entry);
        return () => {
          entry.cancelled = true;
        };
      },
    },
    scheduled,
    fireAll() {
      // Snapshot first: a firing callback may schedule its follow-up wake.
      for (const entry of [...scheduled]) {
        if (!entry.cancelled) entry.callback();
      }
    },
    get liveCount() {
      return scheduled.filter((entry) => !entry.cancelled).length;
    },
  };
}

/**
 * Materialize the script files a definition names.
 *
 * The executor resolves each script through `realpath` to prove it stays inside
 * the artifact root, so the file must actually exist for a run to reach the
 * node — a definition alone is not enough.
 */
export async function createScripts(
  scriptsDir: string,
  names: readonly string[],
  body = "print('ok')\n",
): Promise<void> {
  for (const name of names) {
    const path = join(scriptsDir, name);
    await mkdir(dirname(path), { recursive: true });
    await writeFile(path, body, "utf8");
  }
}
