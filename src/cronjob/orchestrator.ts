/**
 * Run orchestration: the single allowed entry into execution.
 *
 * Three properties this module exists to hold:
 *
 * - **Claim before execute.** A run is claimed atomically before any node
 *   starts, so two concurrent triggers cannot both execute the same job.
 * - **Re-validate every time.** The orchestrator re-reads and re-validates the
 *   definition itself; a caller cannot hand it a pre-validated shortcut.
 * - **Every exit is terminal and recorded.** Success, failure, timeout,
 *   cancellation, skip and rejection each write a distinct state, and the
 *   `finally` path always releases the lock and the token.
 *
 * @module @deepseek-ai/dsh-cronjob/orchestrator
 */

import type {
  CanonicalDefinition,
  JsonValue,
  NodeStatus,
  RunId,
  RunState,
  RunStatus,
  RunTrigger,
  WorkflowNode,
} from "./contracts.js";
import { allocateRunId, CronjobStorage, isTerminal, type RunLock } from "./storage.js";
import { openRunLog, type RunLogger } from "./logging.js";
import { validateDefinitionText } from "./validation.js";
import type { NodeResult } from "./executors/python.js";
import type { PythonExecutor } from "./executors/python.js";
import type { SubagentExecutor } from "./executors/subagent.js";

/** A run's outcome, as the caller sees it. */
export interface RunOutcome {
  readonly runId: RunId;
  readonly cronjobId: string;
  readonly status: RunStatus;
  readonly errorCode?: string;
  readonly summary: string;
  readonly startedAt: string;
  readonly finishedAt: string;
}

export interface OrchestratorOptions {
  readonly storage: CronjobStorage;
  readonly python: PythonExecutor;
  readonly subagent: SubagentExecutor;
  /** Bounded global concurrency across all jobs. */
  readonly maxGlobalConcurrency?: number;
  readonly logger?: {
    info(event: string, data?: Record<string, unknown>): void;
    error(event: string, data?: Record<string, unknown>): void;
  };
  readonly now?: () => Date;
}

interface PendingRun {
  readonly resolve: (outcome: RunOutcome) => void;
}

const DEFAULT_GLOBAL_CONCURRENCY = 4;

/**
 * The single entry point for executing a job.
 *
 * `requestRun` is the only public way in: it re-reads the definition, validates
 * it, claims a slot and only then delegates to the node loop.
 */
export class Orchestrator {
  readonly #storage: CronjobStorage;
  readonly #python: PythonExecutor;
  readonly #subagent: SubagentExecutor;
  readonly #maxGlobalConcurrency: number;
  readonly #logger: OrchestratorOptions["logger"];
  readonly #now: () => Date;
  readonly #running = new Map<string, RunId>();
  readonly #inFlight = new Set<Promise<unknown>>();
  #activeGlobally = 0;
  #disposed = false;

  constructor(options: OrchestratorOptions) {
    this.#storage = options.storage;
    this.#python = options.python;
    this.#subagent = options.subagent;
    this.#maxGlobalConcurrency = options.maxGlobalConcurrency ?? DEFAULT_GLOBAL_CONCURRENCY;
    this.#logger = options.logger;
    this.#now = options.now ?? (() => new Date());
  }

  get runningCount(): number {
    return this.#running.size;
  }

  isRunning(cronjobId: string): boolean {
    return this.#running.has(cronjobId);
  }

  /**
   * Validate, claim and run one job.
   *
   * Never rejects: every failure becomes a terminal outcome, because a caller
   * (a timer callback) has no way to handle an exception and must still be able
   * to re-arm.
   */
  async requestRun(
    cronjobId: string,
    trigger: RunTrigger,
    options: { readonly signal?: AbortSignal } = {},
  ): Promise<RunOutcome> {
    const startedAt = this.#now();
    const runId = allocateRunId(startedAt);

    // ── re-read and re-validate, unconditionally ──────────────────────────
    const text = await this.#storage.readDefinitionText(cronjobId);
    if (text === undefined) {
      return this.#rejected(cronjobId, runId, startedAt, "definition_missing", "definition file not found");
    }
    const validation = validateDefinitionText(text, cronjobId, { now: startedAt });
    if (!validation.ok) {
      const first = validation.diagnostics[0];
      return this.#rejected(
        cronjobId,
        runId,
        startedAt,
        first?.code ?? "schema_invalid",
        first?.message ?? "definition is invalid",
      );
    }
    const canonical = validation.canonical;

    if (!canonical.definition.enabled) {
      return {
        runId,
        cronjobId,
        status: "skipped",
        errorCode: "job_disabled",
        summary: "job is disabled",
        startedAt: startedAt.toISOString(),
        finishedAt: this.#now().toISOString(),
      };
    }

    // ── claim ─────────────────────────────────────────────────────────────
    if (this.#running.has(cronjobId)) {
      return {
        runId,
        cronjobId,
        status: "skipped",
        errorCode: "already_running",
        summary: "another run of this job is still in flight",
        startedAt: startedAt.toISOString(),
        finishedAt: this.#now().toISOString(),
      };
    }
    if (this.#activeGlobally >= this.#maxGlobalConcurrency) {
      return {
        runId,
        cronjobId,
        status: "skipped",
        errorCode: "concurrency_limit",
        summary: "global concurrency limit reached",
        startedAt: startedAt.toISOString(),
        finishedAt: this.#now().toISOString(),
      };
    }
    const lock = await this.#storage.acquireRunLock(cronjobId, runId);
    if (lock === undefined) {
      return {
        runId,
        cronjobId,
        status: "skipped",
        errorCode: "lock_held",
        summary: "another process holds this job's lock",
        startedAt: startedAt.toISOString(),
        finishedAt: this.#now().toISOString(),
      };
    }

    this.#running.set(cronjobId, runId);
    this.#activeGlobally += 1;
    try {
      return await this.#execute(canonical, runId, trigger, startedAt, lock, options.signal);
    } finally {
      this.#running.delete(cronjobId);
      this.#activeGlobally -= 1;
      await lock.release().catch(() => undefined);
    }
  }

  /**
   * Wait for in-flight runs to settle.
   *
   * Called on dispose so a reload does not abandon a subprocess mid-run.
   */
  async drain(): Promise<void> {
    await Promise.allSettled([...this.#inFlight]);
  }

  dispose(): void {
    this.#disposed = true;
  }

  async #execute(
    canonical: CanonicalDefinition,
    runId: RunId,
    trigger: RunTrigger,
    startedAt: Date,
    _lock: RunLock,
    signal: AbortSignal | undefined,
  ): Promise<RunOutcome> {
    const { cronjobId } = canonical.definition;
    const log = await openRunLog({
      storage: this.#storage,
      cronjobId,
      runId,
      startedAt,
    });

    const nodes: Record<string, NodeStatus> = {};
    for (const node of canonical.definition.workflow) nodes[node.nodeId] = "pending";

    let state: RunState = {
      runId,
      cronjobId,
      definitionDigest: canonical.digest,
      trigger,
      status: "accepted",
      startedAt: startedAt.toISOString(),
      nodes,
      logPath: this.#relativeLogPath(log),
      notificationStatus: "pending",
    };

    // The record exists before any node runs, so a crash in the first node
    // still leaves evidence that the run started.
    await this.#storage.createRun(state);
    await log.info("run.started", { trigger });

    const deadline = startedAt.getTime() + canonical.definition.timeoutSeconds * 1000;
    const accumulated: Record<string, JsonValue> = {};

    return this.#guard(log, state, async () => {
      state = { ...state, status: "running" };
      await this.#storage.writeRunState(state);

      for (const node of canonical.definition.workflow) {
        if (signal?.aborted === true) {
          return this.#finish(log, state, "cancelled", "run_cancelled", "run was cancelled");
        }
        if (this.#now().getTime() > deadline) {
          return this.#finish(log, state, "timed_out", "run_timeout", "run exceeded its task deadline");
        }

        nodes[node.nodeId] = "running";
        await this.#storage.writeRunState({ ...state, nodes: { ...nodes } });
        await log.info("node.started", { nodeType: node.nodeType }, node.nodeId);

        const result = await this.#runNode(node, canonical, runId, accumulated, deadline, signal);
        nodes[node.nodeId] = result.status === "succeeded" ? "succeeded" : result.status;
        await this.#storage.writeNodeResult(cronjobId, runId, node.nodeId, toJson(result));
        await this.#storage.writeRunState({ ...state, nodes: { ...nodes } });

        if (result.status !== "succeeded") {
          await log.error(
            "node.failed",
            { errorCode: result.errorCode ?? "node_failed", message: result.message ?? "" },
            node.nodeId,
          );
          // The first failure stops the workflow: later nodes usually consume
          // the earlier node's product, so continuing manufactures more errors.
          // `result.status` is already known not to be "succeeded" here.
          return this.#finish(
            log,
            { ...state, nodes: { ...nodes } },
            result.status,
            result.errorCode ?? "node_failed",
            result.message ?? `node ${node.nodeId} failed`,
          );
        }

        accumulated[node.nodeId] = (result.stdout ?? "") as JsonValue;
        await log.info("node.succeeded", { durationMs: result.durationMs }, node.nodeId);
      }

      return this.#finish(
        log,
        { ...state, nodes: { ...nodes } },
        "succeeded",
        undefined,
        `${canonical.definition.workflow.length} node(s) completed`,
      );
    });
  }

  async #runNode(
    node: WorkflowNode,
    canonical: CanonicalDefinition,
    runId: RunId,
    accumulated: Record<string, JsonValue>,
    deadline: number,
    signal: AbortSignal | undefined,
  ): Promise<NodeResult> {
    const remaining = Math.max(1, deadline - this.#now().getTime());
    const context = {
      context: canonical.definition.context ?? {},
      previous: accumulated,
      runId,
      cronjobId: canonical.definition.cronjobId,
    };
    if (node.nodeType === "pythonScript") {
      return this.#python.execute(
        node,
        context,
        { timeoutMs: remaining, ...(signal === undefined ? {} : { signal }) },
      );
    }
    return this.#subagent.execute(
      node,
      context,
      { timeoutMs: remaining, ...(signal === undefined ? {} : { signal }) },
    );
  }

  /** Write the terminal state, close the log, and never let either throw. */
  async #finish(
    log: RunLogger,
    state: RunState,
    status: RunStatus,
    errorCode: string | undefined,
    summary: string,
  ): Promise<RunOutcome> {
    const finishedAt = this.#now();
    const terminal: RunState = {
      ...state,
      status,
      finishedAt: finishedAt.toISOString(),
      resultSummary: summary,
      ...(errorCode === undefined ? {} : { errorCode }),
    };
    try {
      await this.#storage.writeRunState(terminal);
    } catch (error) {
      this.#logger?.error("run.state_write_failed", {
        runId: state.runId,
        message: describe(error),
      });
    }
    const logged = status === "succeeded" ? "run.succeeded" : "run.failed";
    await (status === "succeeded" ? log.info(logged, { summary }) : log.error(logged, { summary, errorCode: errorCode ?? null }));
    // A logging failure must not prevent cleanup; it is recorded on the state.
    const logOk = await log.close();
    if (!logOk) {
      await this.#storage
        .writeRunState({ ...terminal, errorCode: terminal.errorCode ?? "logging_failed" })
        .catch(() => undefined);
    }
    return {
      runId: terminal.runId,
      cronjobId: terminal.cronjobId,
      status,
      ...(errorCode === undefined ? {} : { errorCode }),
      summary,
      startedAt: terminal.startedAt,
      finishedAt: terminal.finishedAt ?? finishedAt.toISOString(),
    };
  }

  /** A rejection is recorded, not discarded: "it never ran" must be visible. */
  async #rejected(
    cronjobId: string,
    runId: RunId,
    startedAt: Date,
    errorCode: string,
    message: string,
  ): Promise<RunOutcome> {
    const finishedAt = this.#now();
    const state: RunState = {
      runId,
      cronjobId,
      definitionDigest: "",
      trigger: "cron",
      status: "rejected",
      startedAt: startedAt.toISOString(),
      finishedAt: finishedAt.toISOString(),
      nodes: {},
      resultSummary: message,
      errorCode,
      logPath: "",
      notificationStatus: "pending",
    };
    await this.#storage.createRun(state).catch(() => undefined);
    this.#logger?.error("run.rejected", { cronjobId, runId, errorCode, message });
    return {
      runId,
      cronjobId,
      status: "rejected",
      errorCode,
      summary: message,
      startedAt: state.startedAt,
      finishedAt: state.finishedAt ?? finishedAt.toISOString(),
    };
  }

  /**
   * Recover runs left non-terminal by a crash.
   *
   * They are marked failed rather than re-run: a job that died midway may have
   * already produced side effects, and replaying it is the caller's decision to
   * make explicitly, not a recovery heuristic's.
   */
  async recoverInterrupted(cronjobIds: readonly string[]): Promise<string[]> {
    const recovered: string[] = [];
    for (const cronjobId of cronjobIds) {
      const lock = await this.#storage.readLock(cronjobId);
      if (lock !== undefined && isProcessAlive(lock.pid)) continue;
      await this.#storage.releaseRunLock(cronjobId);
      for (const runId of await this.#storage.listRunIds(cronjobId)) {
        const state = await this.#storage.readRunState(cronjobId, runId);
        if (state === undefined || isTerminal(state)) continue;
        await this.#storage.writeRunState({
          ...state,
          status: "failed",
          finishedAt: this.#now().toISOString(),
          errorCode: "host_restarted",
          resultSummary: "interrupted by a host restart",
        });
        recovered.push(`${cronjobId}/${runId}`);
      }
    }
    return recovered;
  }

  #relativeLogPath(log: RunLogger): string {
    return log.path;
  }

  /** Run `body`, converting any escape into a failed terminal state. */
  async #guard(
    log: RunLogger,
    state: RunState,
    body: () => Promise<RunOutcome>,
  ): Promise<RunOutcome> {
    const promise = body().catch(async (error) => {
      return this.#finish(
        log,
        state,
        "failed",
        "orchestrator_error",
        describe(error),
      );
    });
    this.#inFlight.add(promise);
    try {
      return await promise;
    } finally {
      this.#inFlight.delete(promise);
    }
  }
}

function toJson(result: NodeResult): JsonValue {
  return {
    status: result.status,
    ...(result.errorCode === undefined ? {} : { errorCode: result.errorCode }),
    ...(result.message === undefined ? {} : { message: result.message }),
    ...(result.exitCode === undefined ? {} : { exitCode: result.exitCode }),
    ...(result.stdout === undefined ? {} : { stdout: result.stdout }),
    ...(result.stderr === undefined ? {} : { stderr: result.stderr }),
    durationMs: result.durationMs,
  };
}

/**
 * Whether a pid belongs to a live process.
 *
 * `EPERM` means the process exists but belongs to another user, which still
 * counts as alive — treating it as dead would let recovery steal a lock from a
 * running job.
 */
export function isProcessAlive(pid: number): boolean {
  if (!Number.isSafeInteger(pid) || pid <= 0) return false;
  try {
    process.kill(pid, 0);
    return true;
  } catch (error) {
    return (error as { code?: string }).code === "EPERM";
  }
}

function describe(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}
