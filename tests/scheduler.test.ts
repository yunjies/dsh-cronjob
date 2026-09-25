/**
 * Scheduler: cron arithmetic, time zones, and timer lifecycle.
 *
 * The lifecycle assertions are the point. A scheduler that fires correctly but
 * leaks a timer per reload looks fine in a short session and degrades a
 * long-running host, so "zero live timers after dispose" and "unchanged digest
 * does not re-arm" are both asserted explicitly.
 */

import { describe, expect, it} from "vitest";

import {
  Scheduler,
  computeNextFire,
  estimatePeriodMs,
  graceFor,
  isValidTimeZone,
  parseCronExpression,
} from "../src/cronjob/scheduler.js";
import { digestOf } from "../src/cronjob/storage.js";
import type { CanonicalDefinition, CronjobDefinition } from "../src/cronjob/contracts.js";
import { fakeClock, fakeTimers } from "./helpers.js";

function canonical(overrides: Partial<CronjobDefinition> = {}): CanonicalDefinition {
  const definition: CronjobDefinition = {
    cronjobId: "job",
    cronjobName: "Job",
    scheduleTime: "0 9 * * *",
    timeZone: "UTC",
    enabled: true,
    timeoutSeconds: 300,
    maxConcurrentRuns: 1,
    misfirePolicy: "skip",
    workflow: [{ nodeType: "pythonScript", nodeId: "n", scriptPath: "a.py" }],
    ...overrides,
  };
  return { definition, digest: digestOf(definition) };
}

describe("cron arithmetic", () => {
  it("parses a valid five-field expression", () => {
    expect(parseCronExpression("0 9 * * *")).toBe(true);
    expect(parseCronExpression("*/5 * * * *")).toBe(true);
  });

  it("rejects a malformed expression", () => {
    expect(parseCronExpression("not a cron")).toBe(false);
    // The field count is enforced here, not by the underlying library, which
    // accepts these: a dropped or added column must not silently become a
    // schedule the author never wrote.
    expect(parseCronExpression("0 9 * *")).toBe(false);
    expect(parseCronExpression("0 0 9 * * *")).toBe(false);
    expect(parseCronExpression("")).toBe(false);
    // Out-of-range values are still caught by the parser.
    expect(parseCronExpression("60 9 * * *")).toBe(false);
    expect(parseCronExpression("0 25 * * *")).toBe(false);
  });

  it("computes the next occurrence strictly after the given instant", () => {
    const from = new Date("2026-03-01T08:00:00Z");
    const next = computeNextFire("0 9 * * *", "UTC", from);
    expect(next?.toISOString()).toBe("2026-03-01T09:00:00.000Z");
  });

  it("returns undefined rather than throwing on an invalid expression", () => {
    expect(computeNextFire("bogus", "UTC", new Date())).toBeUndefined();
  });

  it("honours the named zone rather than the host zone", () => {
    // 09:00 in Shanghai is 01:00 UTC; the same expression in UTC is not.
    const from = new Date("2026-03-01T00:30:00Z");
    const shanghai = computeNextFire("0 9 * * *", "Asia/Shanghai", from);
    const utc = computeNextFire("0 9 * * *", "UTC", from);
    expect(shanghai?.toISOString()).toBe("2026-03-01T01:00:00.000Z");
    expect(utc?.toISOString()).toBe("2026-03-01T09:00:00.000Z");
  });

  it("validates IANA zone names", () => {
    expect(isValidTimeZone("Europe/Berlin")).toBe(true);
    expect(isValidTimeZone("UTC")).toBe(true);
    expect(isValidTimeZone("Mars/Olympus")).toBe(false);
    expect(isValidTimeZone("")).toBe(false);
  });

  it("sizes the grace window from the schedule period, clamped", () => {
    const from = new Date("2026-03-01T08:00:00Z");
    // Hourly: half the period is 30 min, inside the clamp, so it applies.
    expect(graceFor("0 * * * *", from)).toBe(30 * 60 * 1000);
    // Every minute: half is below the 2-minute floor, so the floor applies.
    expect(graceFor("* * * * *", from)).toBe(2 * 60 * 1000);
    // Daily: half is far above the 2-hour ceiling, so the ceiling applies.
    expect(graceFor("0 9 * * *", from)).toBe(2 * 60 * 60 * 1000);
  });

  it("measures the period between consecutive occurrences", () => {
    expect(estimatePeriodMs("0 * * * *", new Date("2026-03-01T08:00:00Z"))).toBe(60 * 60 * 1000);
  });
});

describe("timer lifecycle", () => {
  it("arms one timer per enabled job", () => {
    const timers = fakeTimers();
    const scheduler = new Scheduler({
      timers: timers.timers,
      onFire: () => undefined,
      now: () => new Date("2026-03-01T08:00:00Z"),
    });
    scheduler.apply([canonical({ cronjobId: "a" }), canonical({ cronjobId: "b" })]);
    expect(scheduler.liveTimerCount).toBe(2);
    expect(scheduler.snapshot().map((j) => j.cronjobId)).toEqual(["a", "b"]);
  });

  it("does not arm a disabled job", () => {
    const timers = fakeTimers();
    const scheduler = new Scheduler({ timers: timers.timers, onFire: () => undefined });
    scheduler.apply([canonical({ enabled: false })]);
    expect(scheduler.liveTimerCount).toBe(0);
  });

  it("leaves an unchanged definition's timer alone", () => {
    const timers = fakeTimers();
    const scheduler = new Scheduler({ timers: timers.timers, onFire: () => undefined });
    const job = canonical();
    scheduler.apply([job]);
    const before = timers.scheduled.length;
    scheduler.apply([job]);
    // Same digest: re-applying must not schedule a second wakeup.
    expect(timers.scheduled.length).toBe(before);
  });

  it("replaces the timer when the definition changes", () => {
    const timers = fakeTimers();
    const scheduler = new Scheduler({ timers: timers.timers, onFire: () => undefined });
    scheduler.apply([canonical()]);
    scheduler.apply([canonical({ scheduleTime: "30 9 * * *" })]);
    expect(scheduler.liveTimerCount).toBe(1);
    // The superseded handle was cancelled, so exactly one is live.
    expect(timers.liveCount).toBe(1);
  });

  it("disarms a job that is no longer present", () => {
    const timers = fakeTimers();
    const scheduler = new Scheduler({ timers: timers.timers, onFire: () => undefined });
    scheduler.apply([canonical({ cronjobId: "a" }), canonical({ cronjobId: "b" })]);
    scheduler.apply([canonical({ cronjobId: "a" })]);
    expect(scheduler.snapshot().map((j) => j.cronjobId)).toEqual(["a"]);
  });

  it("disposes every timer and leaves none live", () => {
    const timers = fakeTimers();
    const scheduler = new Scheduler({ timers: timers.timers, onFire: () => undefined });
    scheduler.apply([canonical({ cronjobId: "a" }), canonical({ cronjobId: "b" })]);
    scheduler.dispose();
    expect(scheduler.liveTimerCount).toBe(0);
    expect(timers.liveCount).toBe(0);
  });

  it("fires when the clock reaches the target", () => {
    const timers = fakeTimers();
    const clock = fakeClock(new Date("2026-03-01T08:59:59Z"));
    const fired: string[] = [];
    const scheduler = new Scheduler({
      timers: timers.timers,
      now: clock.now,
      onFire: (context) => fired.push(context.cronjobId),
    });
    scheduler.apply([canonical()]);

    clock.set(new Date("2026-03-01T09:00:00Z"));
    timers.fireAll();
    expect(fired).toEqual(["job"]);
  });

  it("re-wakes instead of firing when the clock has not reached the target", () => {
    const timers = fakeTimers();
    const clock = fakeClock(new Date("2026-03-01T08:00:00Z"));
    const fired: string[] = [];
    const scheduler = new Scheduler({
      timers: timers.timers,
      now: clock.now,
      onFire: (context) => fired.push(context.cronjobId),
    });
    scheduler.apply([canonical()]);

    // A segment boundary is a chance to re-sample the clock, not a fire.
    timers.fireAll();
    expect(fired).toEqual([]);
  });

  it("does not fire a job that was removed before its wake", () => {
    const timers = fakeTimers();
    const clock = fakeClock(new Date("2026-03-01T08:59:59Z"));
    const fired: string[] = [];
    const scheduler = new Scheduler({
      timers: timers.timers,
      now: clock.now,
      onFire: (context) => fired.push(context.cronjobId),
    });
    scheduler.apply([canonical()]);
    scheduler.remove("job");
    clock.set(new Date("2026-03-01T09:00:00Z"));
    timers.fireAll();
    expect(fired).toEqual([]);
  });

  it("does not fire after disposal", () => {
    const timers = fakeTimers();
    const clock = fakeClock(new Date("2026-03-01T08:59:59Z"));
    const fired: string[] = [];
    const scheduler = new Scheduler({
      timers: timers.timers,
      now: clock.now,
      onFire: (context) => fired.push(context.cronjobId),
    });
    scheduler.apply([canonical()]);
    scheduler.dispose();
    clock.set(new Date("2026-03-01T09:00:00Z"));
    timers.fireAll();
    expect(fired).toEqual([]);
  });

  it("caps a long wait into bounded segments", () => {
    const timers = fakeTimers();
    const scheduler = new Scheduler({
      timers: timers.timers,
      onFire: () => undefined,
      now: () => new Date("2026-01-01T00:00:00Z"),
    });
    // A yearly schedule must not ask for a year-long timer in one shot.
    scheduler.apply([canonical({ scheduleTime: "0 0 1 1 *" })]);
    expect(timers.scheduled[0]?.delay).toBeLessThanOrEqual(60 * 60 * 1000);
  });

  it("reports a schedule that has no future occurrence instead of arming", () => {
    const timers = fakeTimers();
    const events: string[] = [];
    const scheduler = new Scheduler({
      timers: timers.timers,
      onFire: () => undefined,
      onEvent: (event) => events.push(event.type),
    });
    // February 30 never occurs.
    scheduler.apply([canonical({ scheduleTime: "0 0 30 2 *" })]);
    expect(scheduler.liveTimerCount).toBe(0);
    expect(events).toContain("skipped");
  });
});
