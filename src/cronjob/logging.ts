/**
 * Per-run logging.
 *
 * One file per run, JSON Lines, two levels. The invariant that matters: a
 * logging failure must never block a timer, a lock release or a subprocess
 * cleanup. If writing the log were allowed to throw, a full disk would escalate
 * into a permanently stalled job, so every write failure is downgraded to a
 * flag on the run plus a fallback line on the host's stderr.
 *
 * @module @deepseek-ai/dsh-cronjob/logging
 */

import { mkdir, open, type FileHandle } from "node:fs/promises";
import { dirname } from "node:path";

import type { JsonValue, LogLevel, LogRecord, RunId } from "./contracts.js";
import { CronjobStorage } from "./storage.js";

/**
 * Keys whose values are never written to a log.
 *
 * Redaction is by key name, so a definition that carries a token under a
 * recognized key cannot leak it into a file that outlives the run.
 */
const REDACTED_KEYS = new Set([
  "password",
  "passwd",
  "secret",
  "token",
  "apikey",
  "api_key",
  "authorization",
  "cookie",
  "credential",
  "privatekey",
  "private_key",
]);

const REDACTED = "[redacted]";

export interface RunLoggerOptions {
  readonly storage: CronjobStorage;
  readonly cronjobId: string;
  readonly runId: RunId;
  readonly startedAt: Date;
  /** Fallback sink for write failures; defaults to `process.stderr`. */
  readonly onFallback?: (line: string) => void;
}

/**
 * A run's log file.
 *
 * Writes are serialized through `#chain` so two concurrent node events cannot
 * interleave a partial line, and `close` is idempotent for the terminal paths
 * that all funnel into one `finally`.
 */
export class RunLogger {
  readonly #path: string;
  readonly #cronjobId: string;
  readonly #runId: RunId;
  readonly #onFallback: (line: string) => void;
  #handle: FileHandle | undefined;
  #chain: Promise<void> = Promise.resolve();
  #closed = false;
  #failed = false;

  constructor(options: RunLoggerOptions, path: string) {
    this.#path = path;
    this.#cronjobId = options.cronjobId;
    this.#runId = options.runId;
    this.#onFallback =
      options.onFallback ?? ((line) => process.stderr.write(`${line}\n`));
  }

  /** Absolute path of this run's log file. */
  get path(): string {
    return this.#path;
  }

  /** True once any write has failed; surfaced on the run state. */
  get failed(): boolean {
    return this.#failed;
  }

  async info(event: string, data?: Record<string, JsonValue>, nodeId?: string): Promise<void> {
    await this.#write("INFO", event, data, nodeId);
  }

  async error(event: string, data?: Record<string, JsonValue>, nodeId?: string): Promise<void> {
    await this.#write("ERROR", event, data, nodeId);
  }

  /**
   * Flush and close.
   *
   * Never throws: the terminal path that calls this is usually already
   * unwinding from a failure, and a second error there would mask the first.
   */
  async close(): Promise<boolean> {
    if (this.#closed) return !this.#failed;
    this.#closed = true;
    await this.#chain.catch(() => undefined);
    if (this.#handle !== undefined) {
      try {
        await this.#handle.sync();
        await this.#handle.close();
      } catch (error) {
        this.#failed = true;
        this.#fallback(`cronjob log close failed: ${describe(error)}`);
      }
      this.#handle = undefined;
    }
    return !this.#failed;
  }

  async #write(
    level: LogLevel,
    event: string,
    data: Record<string, JsonValue> | undefined,
    nodeId: string | undefined,
  ): Promise<void> {
    if (this.#closed) return;
    const record: LogRecord = {
      at: new Date().toISOString(),
      level,
      cronjobId: this.#cronjobId,
      runId: this.#runId,
      nodeId: nodeId ?? null,
      event,
      ...(data === undefined ? {} : { data: redact(data) as Record<string, JsonValue> }),
    };
    const line = JSON.stringify(record);
    // Chain rather than await directly: callers must not be able to observe
    // ordering differences between two concurrent appends.
    this.#chain = this.#chain.then(() => this.#append(line));
    await this.#chain;
  }

  async #append(line: string): Promise<void> {
    if (this.#failed) return;
    try {
      if (this.#handle === undefined) {
        await mkdir(dirname(this.#path), { recursive: true, mode: 0o700 });
        this.#handle = await open(this.#path, "a", 0o600);
      }
      await this.#handle.write(`${line}\n`);
    } catch (error) {
      this.#failed = true;
      this.#fallback(`cronjob log write failed (${this.#path}): ${describe(error)}`);
    }
  }

  #fallback(line: string): void {
    try {
      this.#onFallback(line);
    } catch {
      // A failing fallback sink is not this module's problem to escalate.
    }
  }
}

/** Open the logger for one run at its conventional path. */
export async function openRunLog(options: RunLoggerOptions): Promise<RunLogger> {
  const path = options.storage.logPath(options.cronjobId, options.startedAt, options.runId);
  const logger = new RunLogger(options, path);
  // The first record proves the file is writable; a failure here is recorded on
  // the logger rather than thrown, so the run can still proceed.
  await logger.info("run.accepted", { scheduleVersion: 1 });
  return logger;
}

/**
 * Replace sensitive values, recursively.
 *
 * Depth is bounded so a self-referential object cannot spin here.
 */
export function redact(value: unknown, depth = 0): unknown {
  if (depth > 16) return REDACTED;
  if (Array.isArray(value)) return value.map((item) => redact(item, depth + 1));
  if (value !== null && typeof value === "object") {
    const out: Record<string, unknown> = {};
    for (const [key, item] of Object.entries(value as Record<string, unknown>)) {
      out[key] = REDACTED_KEYS.has(key.toLowerCase()) ? REDACTED : redact(item, depth + 1);
    }
    return out;
  }
  return value;
}

function describe(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}
