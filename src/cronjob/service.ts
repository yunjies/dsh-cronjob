/**
 * The `cronjobs` Host service.
 *
 * This module is the composition boundary. It assembles storage, validation,
 * the scheduler, the orchestrator, logging and the notification outbox, and it
 * is the only thing that knows the DSH home layout. Everything below it works
 * on the frozen contracts.
 *
 * It publishes a service, so it belongs on the Host plane: scheduling, storage
 * and notification are cross-session, and a second copy inside an agent preset
 * would collide on the service name.
 *
 * @module @deepseek-ai/dsh-cronjob/service
 */

import { join } from "node:path";

import type { CanonicalDefinition, RunStatus, RunTrigger } from "./contracts.js";
import { CronjobStorage, isTerminal } from "./storage.js";
import { Scheduler, type FireContext, type SchedulerTimers } from "./scheduler.js";
import { validateDefinitionText, type ValidationResult } from "./validation.js";
import { NotificationOutbox, type NotificationAgentRegistry, type NotificationSessionLifecycle } from "./notifications.js";
import { Orchestrator, type RunOutcome } from "./orchestrator.js";
import { PythonExecutor, createSubprocessRunner, type SubprocessRunner } from "./executors/python.js";
import { SubagentExecutor, type SubagentProvider } from "./executors/subagent.js";

/** A job row as the tool layer and a status command see it. */
export interface CronjobSummary {
  readonly cronjobId: string;
  readonly cronjobName: string;
  readonly scheduleTime: string;
  readonly timeZone: string;
  readonly enabled: boolean;
  readonly nextFireAt?: string;
  readonly bindSessionId?: string;
  readonly nodeCount: number;
  readonly digest: string;
}

export interface RunSummary {
  readonly runId: string;
  readonly cronjobId: string;
  readonly status: RunStatus;
  readonly trigger: RunTrigger;
  readonly startedAt: string;
  readonly finishedAt?: string;
  readonly summary?: string;
  readonly errorCode?: string;
}

export interface CronjobServiceOptions {
  /** DSH home, or an explicit cronjobs root for tests. */
  readonly dshHome: string;
  readonly timers: SchedulerTimers;
  readonly agents: NotificationAgentRegistry;
  readonly sessions?: NotificationSessionLifecycle;
  readonly subagentProviders?: readonly SubagentProvider[];
  readonly subagentDefaultProvider?: string;
  readonly runner?: SubprocessRunner;
  readonly interpreter?: string;
  readonly maxGlobalConcurrency?: number;
  readonly logger?: {
    info(event: string, data?: Record<string, unknown>): void;
    error(event: string, data?: Record<string, unknown>): void;
  };
  readonly now?: () => Date;
}

/**
 * The assembled cronjob service.
 *
 * `start` loads the table and arms timers; `dispose` releases every timer, lock
 * and subscription it owns.
 */
export class CronjobService {
  readonly storage: CronjobStorage;
  readonly scheduler: Scheduler;
  readonly orchestrator: Orchestrator;
  readonly notifications: NotificationOutbox;
  readonly #logger: CronjobServiceOptions["logger"];
  readonly #now: () => Date;
  readonly #definitions = new Map<string, CanonicalDefinition>();
  #started = false;
  #disposed = false;

  constructor(options: CronjobServiceOptions) {
    const root = join(options.dshHome, "cronjobs");
    this.storage = new CronjobStorage({ root });
    this.#logger = options.logger;
    this.#now = options.now ?? (() => new Date());

    this.scheduler = new Scheduler({
      timers: options.timers,
      onEvent: (event) => {
        if (event.type === "armed") {
          this.#logger?.info("scheduler.armed", {
            cronjobId: event.cronjobId,
            nextFireAt: event.nextFireAt.toISOString(),
          });
        } else if (event.type === "skipped") {
          this.#logger?.error("scheduler.skipped", {
            cronjobId: event.cronjobId,
            reason: event.reason,
          });
        }
      },
      onFire: (context) => {
        void this.handleFire(context);
      },
      now: this.#now,
    });

    const python = new PythonExecutor({
      scriptsRoot: this.storage.scriptsDir,
      runner: options.runner ?? createSubprocessRunner(),
      ...(options.interpreter === undefined ? {} : { interpreter: options.interpreter }),
    });
    const subagent = new SubagentExecutor({
      providers: options.subagentProviders ?? [],
      ...(options.subagentDefaultProvider === undefined
        ? {}
        : { defaultProvider: options.subagentDefaultProvider }),
      onProviderResolved: (info) => {
        // The resolved provider is logged because "which provider actually ran"
        // is the question a failing scheduled job usually raises.
        this.#logger?.info("node.provider_resolved", { ...info });
      },
    });

    this.orchestrator = new Orchestrator({
      storage: this.storage,
      python,
      subagent,
      ...(options.maxGlobalConcurrency === undefined
        ? {}
        : { maxGlobalConcurrency: options.maxGlobalConcurrency }),
      ...(options.logger === undefined ? {} : { logger: options.logger }),
      now: this.#now,
    });

    this.notifications = new NotificationOutbox({
      storage: this.storage,
      agents: options.agents,
      ...(options.sessions === undefined ? {} : { sessions: options.sessions }),
      ...(options.logger === undefined ? {} : { logger: options.logger }),
    });
  }

  /** Create the layout, load the table and arm timers. Idempotent. */
  async start(): Promise<void> {
    if (this.#started || this.#disposed) return;
    this.#started = true;
    await this.storage.ensureLayout();
    await this.reload();
  }

  /**
   * Re-read every definition and reconcile timers.
   *
   * A file that fails validation is reported and skipped; the rest of the table
   * still loads. One broken file must not stop every other job, which is the
   * distinction between this and the per-run rejection in the orchestrator.
   */
  async reload(): Promise<ValidationResult[]> {
    const results: ValidationResult[] = [];
    const canonical: CanonicalDefinition[] = [];
    this.#definitions.clear();

    for (const cronjobId of await this.storage.listDefinitionIds()) {
      const text = await this.storage.readDefinitionText(cronjobId);
      if (text === undefined) continue;
      const result = validateDefinitionText(text, cronjobId, { now: this.#now() });
      results.push(result);
      if (result.ok) {
        canonical.push(result.canonical);
        this.#definitions.set(cronjobId, result.canonical);
      } else {
        this.#logger?.error("definition.invalid", {
          cronjobId,
          diagnostics: result.diagnostics.map((d) => `${d.code}@${d.path}`),
        });
      }
    }

    this.scheduler.apply(canonical);
    return results;
  }

  /** The validated table, sorted by id. */
  list(): CronjobSummary[] {
    const armed = new Map(this.scheduler.snapshot().map((job) => [job.cronjobId, job]));
    return [...this.#definitions.values()]
      .map(({ definition, digest }) => {
        const scheduled = armed.get(definition.cronjobId);
        return {
          cronjobId: definition.cronjobId,
          cronjobName: definition.cronjobName,
          scheduleTime: definition.scheduleTime,
          timeZone: definition.timeZone,
          enabled: definition.enabled,
          ...(scheduled === undefined ? {} : { nextFireAt: scheduled.nextFireAt.toISOString() }),
          ...(definition.bindSessionId === undefined
            ? {}
            : { bindSessionId: definition.bindSessionId }),
          nodeCount: definition.workflow.length,
          digest,
        };
      })
      .sort((a, b) => a.cronjobId.localeCompare(b.cronjobId));
  }

  get(cronjobId: string): CronjobSummary | undefined {
    return this.list().find((job) => job.cronjobId === cronjobId);
  }

  /** Validate a candidate definition without writing it. */
  validate(text: string, cronjobId: string): ValidationResult {
    return validateDefinitionText(text, cronjobId, { now: this.#now() });
  }

  /**
   * Write a definition and re-reconcile.
   *
   * Validation happens before the write, so an obviously broken definition is
   * never persisted as the last good one.
   */
  async upsert(cronjobId: string, text: string): Promise<ValidationResult> {
    const result = validateDefinitionText(text, cronjobId, { now: this.#now() });
    if (!result.ok) return result;
    await this.storage.writeDefinition(cronjobId, text);
    await this.reload();
    return result;
  }

  /** Enable or disable a job by rewriting its definition. */
  async setEnabled(cronjobId: string, enabled: boolean): Promise<boolean> {
    const text = await this.storage.readDefinitionText(cronjobId);
    if (text === undefined) return false;
    const result = validateDefinitionText(text, cronjobId, { now: this.#now() });
    if (!result.ok) return false;
    const updated = applyEnabled(text, enabled);
    await this.storage.writeDefinition(cronjobId, updated);
    await this.reload();
    return true;
  }

  /**
   * Delete a definition.
   *
   * Scheduling stops immediately; run history is left to retention rather than
   * deleted here, so evidence of what the job did survives its removal.
   */
  async delete(cronjobId: string): Promise<boolean> {
    this.scheduler.remove(cronjobId);
    this.#definitions.delete(cronjobId);
    return this.storage.deleteDefinition(cronjobId);
  }

  /** Fire one job now, outside the schedule. */
  async runNow(cronjobId: string, signal?: AbortSignal): Promise<RunOutcome> {
    const outcome = await this.orchestrator.requestRun(cronjobId, "manual", {
      ...(signal === undefined ? {} : { signal }),
    });
    await this.#notify(outcome);
    return outcome;
  }

  /** Run summaries for a job, newest first. */
  async listRuns(cronjobId: string, limit = 20): Promise<RunSummary[]> {
    const out: RunSummary[] = [];
    for (const runId of await this.storage.listRunIds(cronjobId)) {
      if (out.length >= limit) break;
      const state = await this.storage.readRunState(cronjobId, runId);
      if (state === undefined) continue;
      out.push({
        runId: state.runId,
        cronjobId: state.cronjobId,
        status: state.status,
        trigger: state.trigger,
        startedAt: state.startedAt,
        ...(state.finishedAt === undefined ? {} : { finishedAt: state.finishedAt }),
        ...(state.resultSummary === undefined ? {} : { summary: state.resultSummary }),
        ...(state.errorCode === undefined ? {} : { errorCode: state.errorCode }),
      });
    }
    return out;
  }

  /** Read one run's log, bounded by `maxBytes`. */
  async readLog(cronjobId: string, runId: string, maxBytes = 64 * 1024): Promise<string | undefined> {
    const state = await this.storage.readRunState(cronjobId, runId);
    if (state === undefined || state.logPath.length === 0) return undefined;
    const { readFile } = await import("node:fs/promises");
    try {
      const text = await readFile(state.logPath, "utf8");
      return text.length > maxBytes ? `${text.slice(0, maxBytes)}\n… (truncated)` : text;
    } catch {
      return undefined;
    }
  }

  /** Attempt delivery of everything pending for one session. */
  async drainNotifications(sessionId: string): Promise<number> {
    return this.notifications.drain(sessionId);
  }

  /** Recover runs and locks abandoned by a previous process. */
  async recover(): Promise<string[]> {
    return this.orchestrator.recoverInterrupted([...this.#definitions.keys()]);
  }

  /**
   * Release every resource. Idempotent.
   *
   * Timers and subscriptions go first, then in-flight runs are awaited, so a
   * reload never leaves a subprocess running with no owner.
   */
  async dispose(): Promise<void> {
    if (this.#disposed) return;
    this.#disposed = true;
    this.scheduler.dispose();
    this.notifications.dispose();
    await this.orchestrator.drain();
    this.orchestrator.dispose();
    for (const cronjobId of this.#definitions.keys()) {
      await this.storage.releaseRunLock(cronjobId).catch(() => undefined);
    }
    this.#definitions.clear();
  }

  /** The scheduled-fire path: run, then notify. */
  async handleFire(context: FireContext): Promise<void> {
    const outcome = await this.orchestrator.requestRun(context.cronjobId, "cron");
    await this.#notify(outcome);
    if (context.late) {
      this.#logger?.info("run.late_dispatch", {
        cronjobId: context.cronjobId,
        scheduledAt: context.scheduledAt.toISOString(),
      });
    }
    // Re-arm from the current definition regardless of how the run ended:
    // letting one bad run stop the schedule would turn a single failure into a
    // permanently dead job.
    await this.reload().catch(() => undefined);
  }

  async #notify(outcome: RunOutcome): Promise<void> {
    const definition = this.#definitions.get(outcome.cronjobId)?.definition;
    const bindSessionId = definition?.bindSessionId;
    // A job with no bound session records its artifacts and produces no
    // notification; there is no one to tell.
    if (bindSessionId === undefined) return;
    // A skipped or rejected run is still notified: silence would make a job
    // that never actually runs look healthy.
    await this.notifications
      .enqueue({
        cronjobId: outcome.cronjobId,
        runId: outcome.runId,
        bindSessionId,
        status: outcome.status,
        trigger: outcome.trigger,
        summary: outcome.summary,
        logPath: outcome.logPath,
      })
      .catch((error) => {
        this.#logger?.error("notification.enqueue_failed", {
          cronjobId: outcome.cronjobId,
          message: error instanceof Error ? error.message : String(error),
        });
      });
  }
}

/**
 * Flip `enabled` in a definition's text.
 *
 * Done as a targeted line edit rather than a re-serialize so comments and
 * formatting a human wrote are preserved.
 */
export function applyEnabled(text: string, enabled: boolean): string {
  const lines = text.split("\n");
  const index = lines.findIndex((line) => /^\s*enabled\s*:/.test(line));
  const rendered = `${lines[index]?.match(/^\s*/)?.[0] ?? ""}enabled: ${enabled ? "true" : "false"}`;
  if (index >= 0) {
    lines[index] = rendered;
  } else {
    lines.push(rendered);
  }
  return lines.join("\n");
}

/** Report whether any run of this job is still in flight. */
export async function hasLiveRun(
  storage: CronjobStorage,
  cronjobId: string,
): Promise<boolean> {
  for (const runId of await storage.listRunIds(cronjobId)) {
    const state = await storage.readRunState(cronjobId, runId);
    if (state !== undefined && !isTerminal(state)) return true;
  }
  return false;
}
