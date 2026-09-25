/**
 * Cron scheduling: expression parsing, time zones, next-fire computation and
 * managed timer lifecycle.
 *
 * The scheduler owns exactly one timer per enabled job and never touches an OS
 * crontab. Every re-arm compares the definition digest first: a changed
 * definition replaces the timer, an unchanged one is left alone, so an
 * idempotent reload cannot leak timers.
 *
 * @module @deepseek-ai/dsh-cronjob/scheduler
 */

import { CronExpressionParser } from "cron-parser";

import type { CanonicalDefinition } from "./contracts.js";

/**
 * Longest single timer segment.
 *
 * A timer spanning months is unreliable: the process may be suspended and the
 * wall clock may move. Long waits are therefore split into bounded segments,
 * and each wake re-samples the clock before deciding whether to fire.
 */
export const MAX_TIMER_SEGMENT_MS = 60 * 60 * 1000;

/** The managed-timer seam, mirroring the Cordis timer service. */
export interface SchedulerTimers {
  /** Schedule one callback; the returned function cancels it. */
  timeout(callback: () => void, delay: number): () => void;
}

/** What a fire handler is told about the occurrence it is servicing. */
export interface FireContext {
  readonly cronjobId: string;
  readonly scheduledAt: Date;
  /** True when the wake happened past the grace window. */
  readonly late: boolean;
}

export interface ScheduledJob {
  readonly cronjobId: string;
  readonly digest: string;
  readonly nextFireAt: Date;
}

export type SchedulerEvent =
  | { readonly type: "armed"; readonly cronjobId: string; readonly nextFireAt: Date }
  | { readonly type: "disarmed"; readonly cronjobId: string }
  | { readonly type: "skipped"; readonly cronjobId: string; readonly reason: string };

export interface SchedulerOptions {
  readonly timers: SchedulerTimers;
  /** Invoked when a job comes due. Errors are the orchestrator's to record. */
  readonly onFire: (context: FireContext) => void;
  /** Optional observer for timer transitions; used by logs and tests. */
  readonly onEvent?: (event: SchedulerEvent) => void;
  /** Clock seam so tests can advance time deterministically. */
  readonly now?: () => Date;
}

interface Entry {
  readonly digest: string;
  readonly definition: CanonicalDefinition;
  cancel: (() => void) | undefined;
  nextFireAt: Date;
}

/**
 * Hold one managed timer per enabled job.
 *
 * The map is the scheduler's whole state; everything else is derived from the
 * definitions it is handed, so a caller can diff and re-arm by passing the new
 * table rather than by reaching in.
 */
export class Scheduler {
  readonly #timers: SchedulerTimers;
  readonly #onFire: (context: FireContext) => void;
  readonly #onEvent: ((event: SchedulerEvent) => void) | undefined;
  readonly #now: () => Date;
  readonly #entries = new Map<string, Entry>();
  #disposed = false;

  constructor(options: SchedulerOptions) {
    this.#timers = options.timers;
    this.#onFire = options.onFire;
    this.#onEvent = options.onEvent;
    this.#now = options.now ?? (() => new Date());
  }

  /**
   * Reconcile the scheduler with a table of definitions.
   *
   * Disabled and absent jobs are disarmed; a job whose digest is unchanged
   * keeps its existing timer, so repeated reloads are cheap and leak nothing.
   */
  apply(definitions: readonly CanonicalDefinition[]): void {
    if (this.#disposed) return;
    const wanted = new Map<string, CanonicalDefinition>();
    for (const canonical of definitions) {
      if (canonical.definition.enabled) wanted.set(canonical.definition.cronjobId, canonical);
    }

    for (const [cronjobId, entry] of this.#entries) {
      if (!wanted.has(cronjobId)) {
        this.#disarm(cronjobId, entry);
      }
    }

    for (const [cronjobId, canonical] of wanted) {
      const existing = this.#entries.get(cronjobId);
      if (existing !== undefined && existing.digest === canonical.digest) continue;
      if (existing !== undefined) this.#disarm(cronjobId, existing);
      this.#arm(canonical);
    }
  }

  /** Stop scheduling one job, leaving its history untouched. */
  remove(cronjobId: string): void {
    const entry = this.#entries.get(cronjobId);
    if (entry !== undefined) this.#disarm(cronjobId, entry);
  }

  /** Release every timer. Idempotent, so a `finally` block can always call it. */
  dispose(): void {
    if (this.#disposed) return;
    this.#disposed = true;
    for (const [cronjobId, entry] of this.#entries) {
      entry.cancel?.();
      entry.cancel = undefined;
      this.#onEvent?.({ type: "disarmed", cronjobId });
    }
    this.#entries.clear();
  }

  /** Live timers, for `status` output and leak assertions. */
  snapshot(): ScheduledJob[] {
    return [...this.#entries.entries()]
      .map(([cronjobId, entry]) => ({
        cronjobId,
        digest: entry.digest,
        nextFireAt: entry.nextFireAt,
      }))
      .sort((a, b) => a.cronjobId.localeCompare(b.cronjobId));
  }

  get liveTimerCount(): number {
    return this.#entries.size;
  }

  #arm(canonical: CanonicalDefinition): void {
    const { cronjobId, scheduleTime, timeZone } = canonical.definition;
    const at = this.#now();
    const next = computeNextFire(scheduleTime, timeZone, at);
    if (next === undefined) {
      // Validation rejects a schedule with no future occurrence, so reaching
      // here means the expression became unschedulable after it was accepted.
      this.#onEvent?.({
        type: "skipped",
        cronjobId,
        reason: "expression has no future occurrence",
      });
      return;
    }

    const entry: Entry = {
      digest: canonical.digest,
      definition: canonical,
      cancel: undefined,
      nextFireAt: next,
    };
    this.#entries.set(cronjobId, entry);
    this.#scheduleWake(cronjobId, entry);
    this.#onEvent?.({ type: "armed", cronjobId, nextFireAt: next });
  }

  /**
   * Wake in bounded segments.
   *
   * Each wake re-reads the clock: a segment boundary is not a fire, it is a
   * chance to notice that the target moved, the process slept, or the job was
   * removed. Only when the sampled clock reaches the target does a fire issue.
   */
  #scheduleWake(cronjobId: string, entry: Entry): void {
    const delay = Math.max(0, entry.nextFireAt.getTime() - this.#now().getTime());
    entry.cancel = this.#timers.timeout(() => {
      if (this.#disposed) return;
      const current = this.#entries.get(cronjobId);
      // A replaced or removed entry must not fire from a stale callback.
      if (current !== entry) return;

      const now = this.#now();
      const remaining = entry.nextFireAt.getTime() - now.getTime();
      if (remaining > 0) {
        this.#scheduleWake(cronjobId, entry);
        return;
      }

      const graceMs = graceFor(entry.definition.definition.scheduleTime, now);
      const late = now.getTime() - entry.nextFireAt.getTime() > graceMs;
      const scheduledAt = entry.nextFireAt;

      // Disarm before firing: the fire path re-arms from the current
      // definition, so leaving the old handle live would double-schedule.
      entry.cancel = undefined;
      this.#onFire({ cronjobId, scheduledAt, late });
    }, Math.min(delay, MAX_TIMER_SEGMENT_MS));
  }

  #disarm(cronjobId: string, entry: Entry): void {
    entry.cancel?.();
    entry.cancel = undefined;
    this.#entries.delete(cronjobId);
    this.#onEvent?.({ type: "disarmed", cronjobId });
  }
}

/**
 * The grace window: half the schedule period, clamped.
 *
 * Within grace a late fire is serviced as the occurrence it belongs to; past
 * it, the orchestrator decides to collapse the backlog into one run rather
 * than replay every missed slot.
 */
export function graceFor(expression: string, from: Date): number {
  const period = estimatePeriodMs(expression, from);
  return Math.min(2 * 60 * 60 * 1000, Math.max(2 * 60 * 1000, Math.floor(period / 2)));
}

/** Interval between the next two occurrences, used to size the grace window. */
export function estimatePeriodMs(expression: string, from: Date): number {
  const first = computeNextFire(expression, "UTC", from);
  if (first === undefined) return 24 * 60 * 60 * 1000;
  const second = computeNextFire(expression, "UTC", first);
  if (second === undefined) return 24 * 60 * 60 * 1000;
  return Math.max(60 * 1000, second.getTime() - first.getTime());
}

/**
 * The first occurrence strictly after `from`.
 *
 * Returns `undefined` for an unparsable expression or one with no future
 * occurrence rather than throwing, so callers treat "unschedulable" as data.
 */
export function computeNextFire(
  expression: string,
  timeZone: string,
  from: Date,
): Date | undefined {
  // Same field-count gate as `parseCronExpression`: without it a four-field
  // expression would compute a fire time from a schedule nobody wrote.
  if (!parseCronExpression(expression)) return undefined;
  try {
    const parsed = CronExpressionParser.parse(expression, {
      currentDate: from,
      tz: timeZone,
    });
    return parsed.next().toDate();
  } catch {
    return undefined;
  }
}

/**
 * Report whether the expression is a well-formed **five-field** cron.
 *
 * The field count is checked here rather than trusted to the parser: the
 * underlying library silently accepts four fields (filling a default) and an
 * empty string, so a typo like a dropped day-of-month column would be
 * "validated" into a schedule the author never wrote.
 */
export function parseCronExpression(expression: string): boolean {
  const fields = expression.trim().split(/\s+/);
  if (fields.length !== 5) return false;
  if (fields.some((field) => field.length === 0)) return false;
  try {
    CronExpressionParser.parse(expression);
    return true;
  } catch {
    return false;
  }
}

/**
 * Report whether the zone is a known IANA zone.
 *
 * An unknown zone silently falling back to the host zone would make a schedule
 * mean different things on different machines, so it is rejected instead.
 */
export function isValidTimeZone(timeZone: string): boolean {
  if (timeZone.length === 0) return false;
  try {
    new Intl.DateTimeFormat("en-US", { timeZone });
    return true;
  } catch {
    return false;
  }
}
