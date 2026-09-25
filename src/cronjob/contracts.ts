/**
 * Shared, frozen data contracts.
 *
 * Every module boundary in this package passes these types and nothing else: no
 * Cordis service, Agent or Session instance crosses a seam. That keeps the
 * domain testable without a live Host and stops a later module from quietly
 * widening a persisted shape.
 *
 * @module @deepseek-ai/dsh-cronjob/contracts
 */

// ---------------------------------------------------------------------------
// Workflow nodes
// ---------------------------------------------------------------------------

/** Run one script from the controlled artifact root. */
export interface PythonScriptNode {
  readonly nodeType: "pythonScript";
  readonly nodeId: string;
  /** Path relative to the artifact scripts root; absolute paths are rejected. */
  readonly scriptPath: string;
  readonly scriptParams?: Readonly<Record<string, JsonValue>>;
}

/** Delegate one turn to a subagent provider. */
export interface SubagentNode {
  readonly nodeType: "subagent";
  readonly nodeId: string;
  readonly prompt: string;
  /** Provider name; the service default applies when omitted. */
  readonly provider?: string;
  readonly outputFormat?: "text" | "json";
}

export type WorkflowNode = PythonScriptNode | SubagentNode;

// ---------------------------------------------------------------------------
// Definitions
// ---------------------------------------------------------------------------

/** Misfire handling. The first version supports skipping only. */
export type MisfirePolicy = "skip";

/** A parsed, normalized job definition. */
export interface CronjobDefinition {
  readonly cronjobId: string;
  readonly cronjobName: string;
  /** Standard five-field cron: minute hour day month weekday. */
  readonly scheduleTime: string;
  /** Explicit IANA zone; the definition never inherits an ambient zone. */
  readonly timeZone: string;
  /** Session that receives this job's run notifications. */
  readonly bindSessionId?: string;
  readonly context?: Readonly<Record<string, JsonValue>>;
  readonly enabled: boolean;
  readonly timeoutSeconds: number;
  readonly maxConcurrentRuns: number;
  readonly misfirePolicy: MisfirePolicy;
  readonly workflow: readonly WorkflowNode[];
}

/** A definition plus the digest that identifies the exact version consumed. */
export interface CanonicalDefinition {
  readonly definition: CronjobDefinition;
  /** sha256 over the canonical serialization. */
  readonly digest: string;
}

// ---------------------------------------------------------------------------
// Runs
// ---------------------------------------------------------------------------

/**
 * Why a run exists. The trigger is recorded because it decides observable
 * semantics: a `cron` fire consumes a scheduled occurrence, `manual` does not.
 */
export type RunTrigger = "cron" | "manual" | "recovery";

/**
 * Terminal and transient run states.
 *
 * `rejected` and `skipped` are deliberately distinct: a rejection means the
 * definition itself is unusable, a skip means the system would not accept this
 * particular attempt. Conflating them leaves a reader unable to tell whether to
 * fix the config or tune concurrency.
 */
export type RunStatus =
  | "accepted"
  | "running"
  | "succeeded"
  | "failed"
  | "timed_out"
  | "cancelled"
  | "skipped"
  | "rejected";

export type NodeStatus =
  | "pending"
  | "running"
  | "succeeded"
  | "failed"
  | "timed_out"
  | "cancelled"
  | "skipped";

/** `<UTC-file-safe-timestamp>-<random>`; unique per accepted run. */
export type RunId = string;

export interface RunState {
  readonly runId: RunId;
  readonly cronjobId: string;
  readonly definitionDigest: string;
  readonly trigger: RunTrigger;
  readonly status: RunStatus;
  readonly startedAt: string;
  readonly finishedAt?: string;
  readonly nodes: Readonly<Record<string, NodeStatus>>;
  readonly resultSummary?: string;
  readonly errorCode?: string;
  /** Relative to the DSH home cronjobs root. */
  readonly logPath: string;
  readonly notificationStatus: NotificationStatus;
}

export type NotificationStatus = "pending" | "delivered" | "failed";

// ---------------------------------------------------------------------------
// Validation
// ---------------------------------------------------------------------------

/**
 * Stable error codes. A closed set: every rejection maps to exactly one, so a
 * consumer never has to parse a message to learn the cause.
 */
export const VALIDATION_CODES = [
  "yaml_syntax",
  "schema_invalid",
  "unknown_field",
  "duplicate_job_id",
  "duplicate_node_id",
  "invalid_cron",
  "invalid_timezone",
  "invalid_script_path",
  "invalid_node",
  "invalid_timeout",
  "invalid_misfire_policy",
  "io_error",
] as const;

export type ValidationCode = (typeof VALIDATION_CODES)[number];

/** One configuration defect, safe to show a user. */
export interface ValidationDiagnostic {
  readonly code: ValidationCode;
  /** YAML path, with line and column when the parser supplied them. */
  readonly path: string;
  readonly line?: number;
  readonly column?: number;
  readonly message: string;
}

// ---------------------------------------------------------------------------
// Logging
// ---------------------------------------------------------------------------

/** Two levels only; INFO traces progress, ERROR records a defect. */
export type LogLevel = "INFO" | "ERROR";

export interface LogRecord {
  readonly at: string;
  readonly level: LogLevel;
  readonly cronjobId: string;
  readonly runId: RunId;
  readonly nodeId: string | null;
  readonly event: string;
  readonly data?: Readonly<Record<string, JsonValue>>;
}

// ---------------------------------------------------------------------------
// Notifications
// ---------------------------------------------------------------------------

export interface NotificationRecord {
  readonly notificationId: string;
  readonly cronjobId: string;
  readonly runId: RunId;
  /** The run status this notification reports; not the delivery state. */
  readonly status: RunStatus;
  /** Delivery progress, kept separate from the reported run status. */
  readonly delivery: NotificationStatus;
  readonly summary: string;
  readonly createdAt: string;
  readonly attempts: number;
  /** Dedupe key: repeated resume events must not deliver twice. */
  readonly idempotencyKey: string;
}

// ---------------------------------------------------------------------------
// JSON
// ---------------------------------------------------------------------------

export type JsonValue =
  | string
  | number
  | boolean
  | null
  | readonly JsonValue[]
  | { readonly [key: string]: JsonValue };

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

/** Minimum accepted `timeoutSeconds`. */
export const MIN_TIMEOUT_SECONDS = 1;
/** Maximum accepted `timeoutSeconds`. */
export const MAX_TIMEOUT_SECONDS = 24 * 60 * 60;
/** Largest definition file read; a larger file is rejected before parsing. */
export const MAX_DEFINITION_BYTES = 1024 * 1024;
/** Node-result and stdout capture cap, so one chatty node cannot fill a disk. */
export const MAX_NODE_OUTPUT_BYTES = 64 * 1024;

/** Version tag on every persisted record shape. */
export const CONTRACT_VERSION = 1;
