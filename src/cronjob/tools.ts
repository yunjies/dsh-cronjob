/**
 * Model-facing tool definitions.
 *
 * These are pure consumers of the `cronjobs` service: this module owns no
 * scheduler and no storage. The split matters at mount time — the service is a
 * Host row, while these tools are what an agent preset adds to give one session
 * the cronjob surface.
 *
 * @module @deepseek-ai/dsh-cronjob/tools
 */

import type { RunStatus } from "./contracts.js";
import type { CronjobService, CronjobSummary, RunSummary } from "./service.js";
import type { ValidationResult } from "./validation.js";

/** The nine tools this package contributes, in registration order. */
export const CRONJOB_TOOL_NAMES = [
  "cronjob_validate",
  "cronjob_upsert",
  "cronjob_list",
  "cronjob_get",
  "cronjob_enable",
  "cronjob_delete",
  "cronjob_run_now",
  "cronjob_runs",
  "cronjob_log",
] as const;

export type CronjobToolName = (typeof CRONJOB_TOOL_NAMES)[number];

/** Result shapes are plain JSON; the tool layer never returns live objects. */
export type ToolResult =
  | { readonly ok: true; readonly [key: string]: unknown }
  | { readonly ok: false; readonly error: ToolError };

export interface ToolError {
  readonly code: string;
  readonly message: string;
  readonly diagnostics?: readonly { readonly code: string; readonly path: string; readonly message: string }[];
}

/**
 * Validate a candidate definition without persisting it.
 *
 * Present so the model can check its own YAML before writing, which keeps an
 * invalid definition from ever becoming the last good one on disk.
 */
export function cronjobValidate(
  service: CronjobService,
  args: { readonly cronjobId: string; readonly yaml: string },
): ToolResult {
  const result = service.validate(args.yaml, args.cronjobId);
  if (!result.ok) return validationFailure(result);
  return {
    ok: true,
    cronjobId: result.canonical.definition.cronjobId,
    digest: result.canonical.digest,
    scheduleTime: result.canonical.definition.scheduleTime,
    timeZone: result.canonical.definition.timeZone,
    nodeCount: result.canonical.definition.workflow.length,
  };
}

/** Validate and persist a definition, then reconcile timers. */
export async function cronjobUpsert(
  service: CronjobService,
  args: { readonly cronjobId: string; readonly yaml: string },
): Promise<ToolResult> {
  const result = await service.upsert(args.cronjobId, args.yaml);
  if (!result.ok) return validationFailure(result);
  const summary = service.get(args.cronjobId);
  return { ok: true, job: summary === undefined ? null : toPlainJob(summary) };
}

/** List every validated job. */
export function cronjobList(service: CronjobService): ToolResult {
  const jobs = service.list().map(toPlainJob);
  return { ok: true, count: jobs.length, jobs };
}

/** Read one job. */
export function cronjobGet(
  service: CronjobService,
  args: { readonly cronjobId: string },
): ToolResult {
  const summary = service.get(args.cronjobId);
  if (summary === undefined) {
    return { ok: false, error: { code: "cronjob_not_found", message: `no such job: ${args.cronjobId}` } };
  }
  return { ok: true, job: toPlainJob(summary) };
}

/** Enable or disable a job without deleting its history. */
export async function cronjobEnable(
  service: CronjobService,
  args: { readonly cronjobId: string; readonly enabled: boolean },
): Promise<ToolResult> {
  const changed = await service.setEnabled(args.cronjobId, args.enabled);
  if (!changed) {
    return {
      ok: false,
      error: {
        code: "cronjob_not_found",
        message: `no enabled definition found for ${args.cronjobId}`,
      },
    };
  }
  const summary = service.get(args.cronjobId);
  return { ok: true, job: summary === undefined ? null : toPlainJob(summary) };
}

/** Delete a definition; scheduling stops, history stays for retention. */
export async function cronjobDelete(
  service: CronjobService,
  args: { readonly cronjobId: string },
): Promise<ToolResult> {
  const deleted = await service.delete(args.cronjobId);
  if (!deleted) {
    return { ok: false, error: { code: "cronjob_not_found", message: `no such job: ${args.cronjobId}` } };
  }
  return { ok: true, cronjobId: args.cronjobId, deleted: true };
}

/**
 * Fire a job immediately.
 *
 * A manual run does not consume a scheduled occurrence, and it re-reads and
 * re-validates the definition exactly like a scheduled fire does.
 */
export async function cronjobRunNow(
  service: CronjobService,
  args: { readonly cronjobId: string },
): Promise<ToolResult> {
  const outcome = await service.runNow(args.cronjobId);
  const run = {
    runId: outcome.runId,
    cronjobId: outcome.cronjobId,
    status: outcome.status,
    summary: outcome.summary,
    ...(outcome.errorCode === undefined ? {} : { errorCode: outcome.errorCode }),
    startedAt: outcome.startedAt,
    finishedAt: outcome.finishedAt,
  };
  // A rejected or failed run is reported as a run result, not as a tool error:
  // the tool did its job, and the caller needs the run's status to react to.
  return { ok: true, run };
}

/** Recent runs of one job, newest first. */
export async function cronjobRuns(
  service: CronjobService,
  args: { readonly cronjobId: string; readonly limit?: number },
): Promise<ToolResult> {
  const runs = await service.listRuns(args.cronjobId, args.limit ?? 20);
  return { ok: true, count: runs.length, runs: runs.map(toPlainRun) };
}

/** Read one run's log, bounded. */
export async function cronjobLog(
  service: CronjobService,
  args: { readonly cronjobId: string; readonly runId: string; readonly maxBytes?: number },
): Promise<ToolResult> {
  const text = await service.readLog(args.cronjobId, args.runId, args.maxBytes ?? 64 * 1024);
  if (text === undefined) {
    return { ok: false, error: { code: "log_not_found", message: "no log for that run" } };
  }
  return { ok: true, cronjobId: args.cronjobId, runId: args.runId, log: text };
}

/** Dispatch table keyed by tool name, so a registrar stays declarative. */
export const CRONJOB_HANDLERS: {
  readonly [K in CronjobToolName]: (
    service: CronjobService,
    args: Record<string, unknown>,
  ) => ToolResult | Promise<ToolResult>;
} = {
  cronjob_validate: (service, args) =>
    cronjobValidate(service, {
      cronjobId: String(args["cronjobId"] ?? ""),
      yaml: String(args["yaml"] ?? ""),
    }),
  cronjob_upsert: (service, args) =>
    cronjobUpsert(service, {
      cronjobId: String(args["cronjobId"] ?? ""),
      yaml: String(args["yaml"] ?? ""),
    }),
  cronjob_list: (service) => cronjobList(service),
  cronjob_get: (service, args) => cronjobGet(service, { cronjobId: String(args["cronjobId"] ?? "") }),
  cronjob_enable: (service, args) =>
    cronjobEnable(service, {
      cronjobId: String(args["cronjobId"] ?? ""),
      enabled: Boolean(args["enabled"]),
    }),
  cronjob_delete: (service, args) =>
    cronjobDelete(service, { cronjobId: String(args["cronjobId"] ?? "") }),
  cronjob_run_now: (service, args) =>
    cronjobRunNow(service, { cronjobId: String(args["cronjobId"] ?? "") }),
  cronjob_runs: (service, args) =>
    cronjobRuns(service, {
      cronjobId: String(args["cronjobId"] ?? ""),
      ...(typeof args["limit"] === "number" ? { limit: args["limit"] } : {}),
    }),
  cronjob_log: (service, args) =>
    cronjobLog(service, {
      cronjobId: String(args["cronjobId"] ?? ""),
      runId: String(args["runId"] ?? ""),
      ...(typeof args["maxBytes"] === "number" ? { maxBytes: args["maxBytes"] } : {}),
    }),
};

function validationFailure(result: Extract<ValidationResult, { ok: false }>): ToolResult {
  return {
    ok: false,
    error: {
      code: "invalid_definition",
      message: "definition failed validation",
      diagnostics: result.diagnostics.map((d) => ({
        code: d.code,
        path: d.path,
        message: d.message,
      })),
    },
  };
}

/** Strip a service summary to primitive fields; no live object crosses out. */
function toPlainJob(summary: CronjobSummary): Record<string, unknown> {
  return {
    cronjobId: summary.cronjobId,
    cronjobName: summary.cronjobName,
    scheduleTime: summary.scheduleTime,
    timeZone: summary.timeZone,
    enabled: summary.enabled,
    nodeCount: summary.nodeCount,
    digest: summary.digest,
    ...(summary.nextFireAt === undefined ? {} : { nextFireAt: summary.nextFireAt }),
    ...(summary.bindSessionId === undefined ? {} : { bindSessionId: summary.bindSessionId }),
  };
}

function toPlainRun(run: RunSummary): Record<string, unknown> {
  return {
    runId: run.runId,
    cronjobId: run.cronjobId,
    status: run.status as RunStatus,
    trigger: run.trigger,
    startedAt: run.startedAt,
    ...(run.finishedAt === undefined ? {} : { finishedAt: run.finishedAt }),
    ...(run.summary === undefined ? {} : { summary: run.summary }),
    ...(run.errorCode === undefined ? {} : { errorCode: run.errorCode }),
  };
}
