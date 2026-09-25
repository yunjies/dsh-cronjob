/**
 * Session-bound notification outbox.
 *
 * A finished run produces one immutable record before any delivery is
 * attempted, so a delivery failure cannot erase the result — it only moves the
 * record from delivered back to pending. Delivery is deduplicated by an
 * idempotency key because a session resume event can arrive more than once.
 *
 * @module @deepseek-ai/dsh-cronjob/notifications
 */

import { randomBytes } from "node:crypto";

import type {
  NotificationRecord,
  NotificationStatus,
  RunId,
  RunStatus,
  RunTrigger,
} from "./contracts.js";
import { CronjobStorage } from "./storage.js";

/** A live target that can accept one follow-up message. */
export interface NotificationTarget {
  readonly sessionId: string;
  /** Queue one message; resolves once the follow-up is accepted. */
  deliver(message: string): Promise<void>;
}

/** Resolve a session id to a live target, or `undefined` when it is offline. */
export interface NotificationAgentRegistry {
  findLive(sessionId: string): NotificationTarget | undefined;
}

/** Observe sessions so an offline outbox can be drained when one returns. */
export interface NotificationSessionLifecycle {
  onSessionResumed(handler: (sessionId: string) => void): () => void;
}

export interface NotificationOutboxOptions {
  readonly storage: CronjobStorage;
  readonly agents: NotificationAgentRegistry;
  readonly sessions?: NotificationSessionLifecycle;
  readonly logger?: {
    info(event: string, data?: Record<string, unknown>): void;
    error(event: string, data?: Record<string, unknown>): void;
  };
  /** Delivery attempts before a record is parked as failed. */
  readonly maxAttempts?: number;
}

export interface EnqueueRequest {
  readonly cronjobId: string;
  readonly runId: RunId;
  readonly bindSessionId: string;
  readonly status: RunStatus;
  readonly trigger: RunTrigger;
  readonly summary: string;
  readonly logPath: string;
}

const DEFAULT_MAX_ATTEMPTS = 5;

/**
 * Deliver terminal-run summaries to their bound session.
 *
 * Records are keyed by an idempotency key derived from the run identity, so the
 * same run can be enqueued twice without delivering twice.
 */
export class NotificationOutbox {
  readonly #storage: CronjobStorage;
  readonly #agents: NotificationAgentRegistry;
  readonly #maxAttempts: number;
  readonly #logger: NotificationOutboxOptions["logger"];
  #detach: (() => void) | undefined;

  constructor(options: NotificationOutboxOptions) {
    this.#storage = options.storage;
    this.#agents = options.agents;
    this.#maxAttempts = options.maxAttempts ?? DEFAULT_MAX_ATTEMPTS;
    this.#logger = options.logger;
    if (options.sessions !== undefined) {
      this.#detach = options.sessions.onSessionResumed((sessionId) => {
        void this.drain(sessionId).catch((error) => {
          this.#logger?.error("notification.drain_failed", {
            sessionId,
            message: describe(error),
          });
        });
      });
    }
  }

  /**
   * Record one notification and try to deliver it immediately.
   *
   * Recording happens before delivery: the durable record is the authority, and
   * an in-memory delivery attempt is only a projection of it.
   */
  async enqueue(request: EnqueueRequest): Promise<NotificationRecord> {
    const record: NotificationRecord = {
      notificationId: `n-${randomBytes(8).toString("hex")}`,
      cronjobId: request.cronjobId,
      runId: request.runId,
      status: request.status,
      delivery: "pending",
      summary: request.summary,
      createdAt: new Date().toISOString(),
      attempts: 0,
      idempotencyKey: idempotencyKeyFor(request),
    };

    const existing = await this.#storage.listNotifications(request.bindSessionId);
    const duplicate = existing.find((item) => item.idempotencyKey === record.idempotencyKey);
    if (duplicate !== undefined) {
      // Already recorded for this run; delivery state belongs to that record.
      return duplicate;
    }

    await this.#storage.enqueueNotification(request.bindSessionId, record);
    this.#logger?.info("notification.enqueued", {
      cronjobId: request.cronjobId,
      runId: request.runId,
      notificationId: record.notificationId,
    });
    await this.#deliver(request.bindSessionId, record);
    return record;
  }

  /**
   * Attempt delivery of every pending record for one session.
   *
   * Called when a session becomes live. Returns how many records reached their
   * target, which is what a status command or a test asserts on.
   */
  async drain(sessionId: string): Promise<number> {
    const target = this.#agents.findLive(sessionId);
    if (target === undefined) return 0;

    const pending = await this.#storage.listNotifications(sessionId);
    let delivered = 0;
    for (const record of pending) {
      // Only records that have not yet arrived are (re)delivered. Without this
      // an already-delivered record would be sent again on every resume, which
      // is exactly the duplicate the idempotency key exists to prevent.
      if (record.delivery === "delivered") continue;
      if (record.delivery === "failed") continue;
      if (record.attempts >= this.#maxAttempts) continue;
      const ok = await this.#deliver(sessionId, record, target);
      if (ok) delivered += 1;
    }
    return delivered;
  }

  /** Notification records for a session, for `list` output and tests. */
  async list(sessionId: string): Promise<NotificationRecord[]> {
    return this.#storage.listNotifications(sessionId);
  }

  /** Release the session subscription. Idempotent. */
  dispose(): void {
    this.#detach?.();
    this.#detach = undefined;
  }

  async #deliver(
    sessionId: string,
    record: NotificationRecord,
    known?: NotificationTarget,
  ): Promise<boolean> {
    const target = known ?? this.#agents.findLive(sessionId);
    if (target === undefined) {
      // Offline: the record stays pending and is picked up on resume. No
      // attempt is counted, because nothing was actually attempted.
      return false;
    }
    const attempts = record.attempts + 1;
    try {
      await target.deliver(renderNotification(record));
      await this.#storage.enqueueNotification(sessionId, {
        ...record,
        attempts,
        delivery: "delivered",
      });
      this.#logger?.info("notification.delivered", {
        notificationId: record.notificationId,
        runId: record.runId,
      });
      return true;
    } catch (error) {
      const delivery: NotificationStatus = attempts >= this.#maxAttempts ? "failed" : "pending";
      await this.#storage.enqueueNotification(sessionId, {
        ...record,
        attempts,
        delivery,
      });
      this.#logger?.error("notification.delivery_failed", {
        notificationId: record.notificationId,
        runId: record.runId,
        attempts,
        parked: delivery === "failed",
        message: describe(error),
      });
      return false;
    }
  }
}

/**
 * Stable key for one run's terminal notification.
 *
 * Derived from the run identity rather than generated, so a retry produces the
 * same key and the duplicate check works.
 */
export function idempotencyKeyFor(request: EnqueueRequest): string {
  return `cronjob:${request.cronjobId}:run:${request.runId}`;
}

/**
 * Frame a summary for a session.
 *
 * The framing marks the body as a report about an external job rather than a
 * fresh instruction, and it deliberately carries no node output: script and
 * subagent output stay in the run artifacts and are never promoted into
 * something a session would treat as a directive.
 */
export function renderNotification(record: NotificationRecord): string {
  const lines = [
    "[CRONJOB RUN]",
    `job: ${record.cronjobId}`,
    `run: ${record.runId}`,
    `status: ${record.status}`,
    `at: ${record.createdAt}`,
    `summary: ${record.summary}`,
  ];
  return lines.join("\n");
}

function describe(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}
