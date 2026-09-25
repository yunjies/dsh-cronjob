/**
 * DSH home storage: layout, atomic writes, locking and retention.
 *
 * Every path is derived from a validated job, run or node identifier, so a
 * definition can never steer a read or write outside the cronjobs root. Writes
 * go through a temporary file plus rename, so a crash mid-write leaves the
 * previous good definition intact rather than a truncated one.
 *
 * @module @deepseek-ai/dsh-cronjob/storage
 */

import { createHash, randomBytes } from "node:crypto";
import { constants as fsConstants } from "node:fs";
import { access, mkdir, open, readFile, readdir, rename, rm, stat, unlink } from "node:fs/promises";
import { dirname, join, resolve, sep } from "node:path";

import type {
  JsonValue,
  NotificationRecord,
  RunId,
  RunState,
  ValidationDiagnostic,
} from "./contracts.js";
import { MAX_DEFINITION_BYTES } from "./contracts.js";

/** Identifier characters allowed in a path segment. */
const SAFE_ID = /^[A-Za-z0-9][A-Za-z0-9._-]*$/;

export class StorageError extends Error {
  override readonly name = "StorageError";

  constructor(
    message: string,
    readonly code: ValidationDiagnostic["code"] = "io_error",
    readonly path?: string,
  ) {
    super(message);
  }
}

/** Reject any identifier that could escape or alias its directory. */
export function assertSafeId(id: string, what: string): void {
  if (!SAFE_ID.test(id) || id === "." || id === "..") {
    throw new StorageError(`unsafe ${what}: ${JSON.stringify(id)}`, "io_error");
  }
}

/** Resolve `child` under `root` and refuse anything that escapes it. */
export function resolveWithin(root: string, child: string): string {
  const full = resolve(root, child);
  const base = resolve(root);
  if (full !== base && !full.startsWith(base + sep)) {
    throw new StorageError(`path escapes root: ${child}`, "invalid_script_path");
  }
  return full;
}

export interface StorageOptions {
  /** The `cronjobs` directory under DSH home. */
  readonly root: string;
}

/** The DSH home layout this package owns. */
export class CronjobStorage {
  readonly root: string;
  readonly definitionsDir: string;
  readonly scriptsDir: string;
  readonly runsDir: string;
  readonly logsDir: string;
  readonly notificationsDir: string;
  readonly locksDir: string;

  constructor(options: StorageOptions) {
    this.root = resolve(options.root);
    this.definitionsDir = join(this.root, "definitions");
    this.scriptsDir = join(this.root, "artifacts", "scripts");
    this.runsDir = join(this.root, "runs");
    this.logsDir = join(this.root, "logs");
    this.notificationsDir = join(this.root, "notifications");
    this.locksDir = join(this.root, "locks");
  }

  /**
   * Create the layout. Directories are created with owner-only permissions:
   * the tree holds run output and session-bound summaries.
   */
  async ensureLayout(): Promise<void> {
    for (const dir of [
      this.root,
      this.definitionsDir,
      this.scriptsDir,
      this.runsDir,
      this.logsDir,
      this.notificationsDir,
      this.locksDir,
    ]) {
      await mkdir(dir, { recursive: true, mode: 0o700 });
    }
  }

  definitionPath(cronjobId: string): string {
    assertSafeId(cronjobId, "cronjobId");
    return join(this.definitionsDir, `${cronjobId}.yaml`);
  }

  runDir(cronjobId: string, runId: RunId): string {
    assertSafeId(cronjobId, "cronjobId");
    assertSafeId(runId, "runId");
    return join(this.runsDir, cronjobId, runId);
  }

  runStatePath(cronjobId: string, runId: RunId): string {
    return join(this.runDir(cronjobId, runId), "state.json");
  }

  nodeResultPath(cronjobId: string, runId: RunId, nodeId: string): string {
    assertSafeId(nodeId, "nodeId");
    return join(this.runDir(cronjobId, runId), "nodes", `${nodeId}.json`);
  }

  static logFileName(at: Date, runId: RunId): string {
    const stamp = at.toISOString().replace(/[:.]/g, "-");
    return `${stamp}-${runId}.log`;
  }

  logPath(cronjobId: string, at: Date, runId: RunId): string {
    assertSafeId(cronjobId, "cronjobId");
    return join(this.logsDir, cronjobId, CronjobStorage.logFileName(at, runId));
  }

  notificationDir(bindSessionId: string): string {
    assertSafeId(bindSessionId, "bindSessionId");
    return join(this.notificationsDir, bindSessionId);
  }

  notificationPath(bindSessionId: string, notificationId: string): string {
    assertSafeId(notificationId, "notificationId");
    return join(this.notificationDir(bindSessionId), `${notificationId}.json`);
  }

  lockPath(cronjobId: string): string {
    assertSafeId(cronjobId, "cronjobId");
    return join(this.locksDir, `${cronjobId}.lock`);
  }

  // -- definitions ---------------------------------------------------------

  /**
   * Read one definition's raw text.
   *
   * The size ceiling is applied here, before any parser sees the text: a YAML
   * alias expansion can blow up a tiny input, and checking after parsing would
   * already have paid the cost.
   */
  async readDefinitionText(cronjobId: string): Promise<string | undefined> {
    const path = this.definitionPath(cronjobId);
    let handle;
    try {
      // `O_NOFOLLOW` keeps a symlink from redirecting the read elsewhere.
      handle = await open(path, fsConstants.O_RDONLY | fsConstants.O_NOFOLLOW);
    } catch (error) {
      if (isNotFound(error)) return undefined;
      throw new StorageError(`cannot read definition: ${describe(error)}`, "io_error", path);
    }
    try {
      const info = await handle.stat();
      if (!info.isFile()) {
        throw new StorageError("definition is not a regular file", "io_error", path);
      }
      if (info.size > MAX_DEFINITION_BYTES) {
        throw new StorageError(
          `definition exceeds ${MAX_DEFINITION_BYTES} bytes`,
          "io_error",
          path,
        );
      }
      return await handle.readFile({ encoding: "utf8" });
    } finally {
      await handle.close();
    }
  }

  /**
   * Write a definition so a reader sees either the old file or the new one.
   *
   * A partial write would leave a syntactically broken definition that the next
   * scheduled fire rejects, so the temp-file-then-rename ordering is the whole
   * point: `fsync` before rename, and the directory entry after.
   */
  async writeDefinition(cronjobId: string, text: string): Promise<void> {
    const path = this.definitionPath(cronjobId);
    await mkdir(dirname(path), { recursive: true, mode: 0o700 });
    await this.writeFileAtomically(path, text);
  }

  async deleteDefinition(cronjobId: string): Promise<boolean> {
    const path = this.definitionPath(cronjobId);
    try {
      await unlink(path);
      return true;
    } catch (error) {
      if (isNotFound(error)) return false;
      throw new StorageError(`cannot delete definition: ${describe(error)}`, "io_error", path);
    }
  }

  /** Definition ids present on disk, sorted for a deterministic scan order. */
  async listDefinitionIds(): Promise<string[]> {
    try {
      const entries = await readdir(this.definitionsDir, { withFileTypes: true });
      return entries
        .filter((entry) => entry.isFile() && entry.name.endsWith(".yaml"))
        .map((entry) => entry.name.slice(0, -".yaml".length))
        .filter((id) => SAFE_ID.test(id))
        .sort();
    } catch (error) {
      if (isNotFound(error)) return [];
      throw new StorageError(`cannot list definitions: ${describe(error)}`, "io_error");
    }
  }

  // -- runs ----------------------------------------------------------------

  /**
   * Allocate the run's directories and state file.
   *
   * This happens before any node executes so a run that dies in its first node
   * still leaves evidence. The alternative — recording after the fact — makes
   * exactly the most interesting failures invisible.
   */
  async createRun(state: RunState): Promise<void> {
    const dir = this.runDir(state.cronjobId, state.runId);
    await mkdir(join(dir, "nodes"), { recursive: true, mode: 0o700 });
    await this.writeRunState(state);
  }

  async writeRunState(state: RunState): Promise<void> {
    const path = this.runStatePath(state.cronjobId, state.runId);
    await this.writeFileAtomically(path, JSON.stringify(state, null, 2));
  }

  /** Read a run state, rejecting anything that is not a regular file. */
  async readRunState(cronjobId: string, runId: RunId): Promise<RunState | undefined> {
    const path = this.runStatePath(cronjobId, runId);
    try {
      const info = await stat(path);
      if (!info.isFile()) {
        throw new StorageError("run state is not a regular file", "io_error", path);
      }
      return JSON.parse(await readFile(path, "utf8")) as RunState;
    } catch (error) {
      if (isNotFound(error)) return undefined;
      throw new StorageError(`cannot read run state: ${describe(error)}`, "io_error", path);
    }
  }

  async writeNodeResult(
    cronjobId: string,
    runId: RunId,
    nodeId: string,
    result: JsonValue,
  ): Promise<void> {
    const path = this.nodeResultPath(cronjobId, runId, nodeId);
    await mkdir(dirname(path), { recursive: true, mode: 0o700 });
    await this.writeFileAtomically(path, JSON.stringify(result, null, 2));
  }

  /** Run ids for one job, newest first. */
  async listRunIds(cronjobId: string): Promise<RunId[]> {
    assertSafeId(cronjobId, "cronjobId");
    const dir = join(this.runsDir, cronjobId);
    try {
      const entries = await readdir(dir, { withFileTypes: true });
      return entries
        .filter((entry) => entry.isDirectory() && SAFE_ID.test(entry.name))
        .map((entry) => entry.name)
        .sort()
        .reverse();
    } catch (error) {
      if (isNotFound(error)) return [];
      throw new StorageError(`cannot list runs: ${describe(error)}`, "io_error");
    }
  }

  // -- locks ---------------------------------------------------------------

  /**
   * Take the job's single-flight lock.
   *
   * Creation is exclusive (`wx`), so two concurrent claims cannot both win. The
   * recorded pid and timestamp let a later process decide whether an abandoned
   * lock is recoverable instead of guessing.
   */
  async acquireRunLock(
    cronjobId: string,
    runId: RunId,
  ): Promise<RunLock | undefined> {
    const path = this.lockPath(cronjobId);
    await mkdir(dirname(path), { recursive: true, mode: 0o700 });
    try {
      const handle = await open(path, "wx", 0o600);
      try {
        await handle.writeFile(
          JSON.stringify({ runId, pid: process.pid, at: new Date().toISOString() }),
        );
        await handle.sync();
      } finally {
        await handle.close();
      }
      return new RunLock(path);
    } catch (error) {
      if (isExists(error)) return undefined;
      throw new StorageError(`cannot acquire lock: ${describe(error)}`, "io_error", path);
    }
  }

  /** Read a lock without taking it, to decide whether it is stale. */
  async readLock(cronjobId: string): Promise<LockRecord | undefined> {
    const path = this.lockPath(cronjobId);
    try {
      return JSON.parse(await readFile(path, "utf8")) as LockRecord;
    } catch (error) {
      if (isNotFound(error)) return undefined;
      return undefined;
    }
  }

  /** Release a lock regardless of owner; used by recovery and dispose. */
  async releaseRunLock(cronjobId: string): Promise<void> {
    await rm(this.lockPath(cronjobId), { force: true });
  }

  // -- notifications -------------------------------------------------------

  async enqueueNotification(
    bindSessionId: string,
    record: NotificationRecord,
  ): Promise<void> {
    const dir = this.notificationDir(bindSessionId);
    await mkdir(dir, { recursive: true, mode: 0o700 });
    await this.writeFileAtomically(
      this.notificationPath(bindSessionId, record.notificationId),
      JSON.stringify(record, null, 2),
    );
  }

  async listNotifications(bindSessionId: string): Promise<NotificationRecord[]> {
    const dir = this.notificationDir(bindSessionId);
    try {
      const entries = await readdir(dir, { withFileTypes: true });
      const out: NotificationRecord[] = [];
      for (const entry of entries) {
        if (!entry.isFile() || !entry.name.endsWith(".json")) continue;
        try {
          out.push(
            JSON.parse(await readFile(join(dir, entry.name), "utf8")) as NotificationRecord,
          );
        } catch {
          // One unreadable record must not block the rest of the outbox.
          continue;
        }
      }
      return out.sort((a, b) => a.createdAt.localeCompare(b.createdAt));
    } catch (error) {
      if (isNotFound(error)) return [];
      throw new StorageError(`cannot list notifications: ${describe(error)}`, "io_error");
    }
  }

  async removeNotification(bindSessionId: string, notificationId: string): Promise<void> {
    await rm(this.notificationPath(bindSessionId, notificationId), { force: true });
  }

  // -- retention -----------------------------------------------------------

  /**
   * Prune old run directories and logs.
   *
   * Runs still referenced by a non-terminal state are never removed: deleting
   * the evidence of an in-flight run would make it unrecoverable after a crash.
   */
  async prune(options: RetentionOptions): Promise<PruneReport> {
    const removedRuns: string[] = [];
    const removedLogs: string[] = [];
    const cutoff =
      options.maxAgeMs === undefined ? undefined : Date.now() - options.maxAgeMs;

    for (const cronjobId of await this.listDefinitionIds().catch(() => [])) {
      const runIds = await this.listRunIds(cronjobId);
      const keep = options.maxRunsPerJob ?? runIds.length;
      for (const runId of runIds.slice(keep)) {
        const state = await this.readRunState(cronjobId, runId).catch(() => undefined);
        if (state !== undefined && !isTerminal(state)) continue;
        if (cutoff !== undefined && state !== undefined) {
          const started = Date.parse(state.startedAt);
          if (Number.isFinite(started) && started > cutoff) continue;
        }
        await rm(this.runDir(cronjobId, runId), { recursive: true, force: true });
        removedRuns.push(`${cronjobId}/${runId}`);
      }
      for (const log of await this.listLogFiles(cronjobId)) {
        if (cutoff === undefined) continue;
        const info = await stat(log).catch(() => undefined);
        if (info !== undefined && info.mtimeMs < cutoff) {
          await rm(log, { force: true });
          removedLogs.push(log);
        }
      }
    }
    return { removedRuns, removedLogs };
  }

  async listLogFiles(cronjobId: string): Promise<string[]> {
    assertSafeId(cronjobId, "cronjobId");
    const dir = join(this.logsDir, cronjobId);
    try {
      const entries = await readdir(dir, { withFileTypes: true });
      return entries
        .filter((entry) => entry.isFile() && entry.name.endsWith(".log"))
        .map((entry) => join(dir, entry.name))
        .sort();
    } catch (error) {
      if (isNotFound(error)) return [];
      throw new StorageError(`cannot list logs: ${describe(error)}`, "io_error");
    }
  }

  async directoryExists(path: string): Promise<boolean> {
    try {
      await access(path, fsConstants.F_OK);
      return true;
    } catch {
      return false;
    }
  }

  // -- internals -----------------------------------------------------------

  /**
   * Temp file, fsync, rename, then fsync the directory entry.
   *
   * The second sync is what makes the rename itself durable; without it a power
   * loss can leave the old name pointing at nothing on some filesystems.
   */
  private async writeFileAtomically(path: string, contents: string): Promise<void> {
    const tmp = `${path}.tmp-${randomBytes(6).toString("hex")}`;
    let handle;
    try {
      handle = await open(tmp, "wx", 0o600);
      await handle.writeFile(contents);
      await handle.sync();
      await handle.close();
      handle = undefined;
      await rename(tmp, path);
      const dir = await open(dirname(path), fsConstants.O_RDONLY);
      try {
        await dir.sync();
      } finally {
        await dir.close();
      }
    } catch (error) {
      if (handle !== undefined) await handle.close().catch(() => undefined);
      await rm(tmp, { force: true }).catch(() => undefined);
      throw new StorageError(`atomic write failed: ${describe(error)}`, "io_error", path);
    }
  }
}

/** A held lock; `release` is idempotent so a `finally` block can always call it. */
export class RunLock {
  #path: string;
  #released = false;

  constructor(path: string) {
    this.#path = path;
  }

  async release(): Promise<void> {
    if (this.#released) return;
    this.#released = true;
    await rm(this.#path, { force: true });
  }

  get path(): string {
    return this.#path;
  }
}

export interface LockRecord {
  readonly runId: RunId;
  readonly pid: number;
  readonly at: string;
}

export interface RetentionOptions {
  readonly maxRunsPerJob?: number;
  readonly maxAgeMs?: number;
}

export interface PruneReport {
  readonly removedRuns: readonly string[];
  readonly removedLogs: readonly string[];
}

export function isTerminal(state: Pick<RunState, "status">): boolean {
  return (
    state.status === "succeeded" ||
    state.status === "failed" ||
    state.status === "timed_out" ||
    state.status === "cancelled" ||
    state.status === "skipped" ||
    state.status === "rejected"
  );
}

/**
 * Allocate a run id: a file-safe UTC timestamp plus randomness.
 *
 * The timestamp sorts chronologically as a string and the suffix keeps two runs
 * started in the same millisecond distinct.
 */
export function allocateRunId(now: Date = new Date()): RunId {
  const stamp = now.toISOString().replace(/[:.]/g, "-").replace(/Z$/, "");
  return `${stamp}-${randomBytes(4).toString("hex")}`;
}

/** sha256 over the canonical serialization of a value. */
export function digestOf(value: unknown): string {
  return createHash("sha256").update(canonicalize(value)).digest("hex");
}

/**
 * Deterministic JSON: object keys sorted, so two equivalent definitions hash
 * alike and a cosmetic key reorder does not look like a definition change.
 */
export function canonicalize(value: unknown): string {
  return JSON.stringify(sortKeys(value));
}

function sortKeys(value: unknown): unknown {
  if (Array.isArray(value)) return value.map(sortKeys);
  if (value !== null && typeof value === "object") {
    const out: Record<string, unknown> = {};
    for (const key of Object.keys(value as Record<string, unknown>).sort()) {
      out[key] = sortKeys((value as Record<string, unknown>)[key]);
    }
    return out;
  }
  return value;
}

function isNotFound(error: unknown): boolean {
  return typeof error === "object" && error !== null && (error as { code?: string }).code === "ENOENT";
}

function isExists(error: unknown): boolean {
  return typeof error === "object" && error !== null && (error as { code?: string }).code === "EEXIST";
}

function describe(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}
