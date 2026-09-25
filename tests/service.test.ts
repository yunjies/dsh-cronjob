/**
 * The assembled service: reload, enable/disable, and timer reconciliation.
 *
 * This suite exercises the whole composition over an isolated DSH home, so it
 * covers the integration the unit suites deliberately stub out.
 */

import { join } from "node:path";

import { afterEach, beforeEach, describe, expect, it } from "vitest";

import { CronjobService, applyEnabled } from "../src/cronjob/service.js";
import { createScripts, definitionYaml, fakeRunner, fakeTimers, makeTempHome, type TempHome } from "./helpers.js";

let home: TempHome;

beforeEach(async () => {
  home = await makeTempHome();
});

afterEach(async () => {
  await home.cleanup();
});

function build(extra: { readonly live?: Set<string> } = {}) {
  const timers = fakeTimers();
  const sent: string[] = [];
  const live = extra.live ?? new Set<string>(["session-1"]);
  const service = new CronjobService({
    dshHome: home.path,
    timers: timers.timers,
    agents: {
      findLive(sessionId) {
        if (!live.has(sessionId)) return undefined;
        return {
          sessionId,
          async deliver(message) {
            sent.push(message);
          },
        };
      },
    },
    runner: fakeRunner(() => ({ stdout: "ok" })),
    now: () => new Date("2026-03-01T08:00:00Z"),
  });
  return { service, timers, sent };
}

describe("CronjobService", () => {
  it("loads a valid table and arms a timer for each enabled job", async () => {
    const { service, timers } = build();
    await service.start();
    await service.upsert("job", definitionYaml({ cronjobId: "job" }));
    expect(service.list().map((j) => j.cronjobId)).toEqual(["job"]);
    expect(timers.liveCount).toBeGreaterThan(0);
    await service.dispose();
  });

  it("refuses to persist an invalid definition", async () => {
    const { service } = build();
    await service.start();
    const result = await service.upsert("job", definitionYaml({ cronjobId: "job", scheduleTime: "bogus" }));
    expect(result.ok).toBe(false);
    // Nothing was written, so no broken definition can become the last good one.
    expect(service.list()).toEqual([]);
    await service.dispose();
  });

  it("keeps loading the rest of the table when one file is broken", async () => {
    const { service } = build();
    await service.start();
    await service.upsert("good", definitionYaml({ cronjobId: "good" }));
    const { writeFile } = await import("node:fs/promises");
    await writeFile(join(service.storage.definitionsDir, "bad.yaml"), "not: [valid", "utf8");

    const results = await service.reload();
    // The good job survives; only the broken file is reported.
    expect(service.list().map((j) => j.cronjobId)).toEqual(["good"]);
    expect(results.some((r) => !r.ok)).toBe(true);
    await service.dispose();
  });

  it("disables a job without deleting its history", async () => {
    const { service, timers } = build();
    await service.start();
    await service.upsert("job", definitionYaml({ cronjobId: "job" }));
    expect(timers.liveCount).toBeGreaterThan(0);

    await service.setEnabled("job", false);
    // Disabled means unscheduled, and the row is still listed.
    expect(service.get("job")?.enabled).toBe(false);
    expect(service.list()).toHaveLength(1);
    await service.dispose();
  });

  it("runs a job on demand and notifies the bound session", async () => {
    const { service, sent } = build();
    await service.start();
    await createScripts(service.storage.scriptsDir, ["collect.py"]);
    await service.upsert(
      "job",
      definitionYaml({ cronjobId: "job", bindSessionId: "session-1" }),
    );

    const outcome = await service.runNow("job");
    expect(outcome.status).toBe("succeeded");
    expect(sent).toHaveLength(1);
    expect(sent[0]).toContain("succeeded");
    await service.dispose();
  });

  it("deletes a job and stops scheduling it", async () => {
    const { service, timers } = build();
    await service.start();
    await service.upsert("job", definitionYaml({ cronjobId: "job" }));
    expect(await service.delete("job")).toBe(true);
    expect(service.list()).toEqual([]);
    expect(timers.liveCount).toBe(0);
    await service.dispose();
  });

  it("leaves no timer behind after dispose", async () => {
    const { service, timers } = build();
    await service.start();
    await service.upsert("job", definitionYaml({ cronjobId: "job" }));
    await service.dispose();
    expect(timers.liveCount).toBe(0);
  });
});

describe("applyEnabled", () => {
  it("flips the flag and preserves the surrounding text", () => {
    const text = definitionYaml({ cronjobId: "job", enabled: true });
    const flipped = applyEnabled(text, false);
    expect(flipped).toContain("enabled: false");
    expect(flipped).not.toContain("enabled: true");
    expect(flipped).toContain("cronjobName: Daily report");
  });
});
