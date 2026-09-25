/**
 * Host-plane composition entry that registers the nine `cronjob_*` tools.
 *
 * Why this is a separate row from `./index.ts`: the service provider and the
 * tool consumer belong to different planes.
 *
 * - `./index.ts` publishes the cross-session `cronjobs` service, so it is a
 *   Host row and must never sit in an agent preset (a second session mounting
 *   that preset would collide on the service name).
 * - This module publishes nothing. It is a pure consumer of `cronjobs` and the
 *   `tools` registry, so it is the row a preset mounts to give one agent the
 *   cronjob tool surface.
 *
 * `inject` lists both services, so the fiber waits for them and the `ctx.tools`
 * / `ctx.cronjobs` reads are legal. `apply` must not be `async`: Cordis
 * collects a plugin's effect synchronously and rejects a returned Promise.
 * Teardown goes through `ctx.effect` rather than a returned disposer, because a
 * returned disposer is keyed to the `apply` function's identity and is not
 * re-collected when this module-level plugin object is mounted twice in one
 * process — the second mount would register nine more tools and never remove
 * them.
 *
 * @module @deepseek-ai/dsh-cronjob/tool-cronjob
 */

import { defineTool, type ParameterSchemaSpec } from "@deepseek-ai/dsh-tools";

import type { CronjobService } from "./service.js";
import { CRONJOB_HANDLERS, type CronjobToolName } from "./tools.js";

/** Cordis plugin name; also the composition row id for the tool consumer. */
export const name = "tool-cronjob";

/** Services required before this row activates. */
export const inject: readonly string[] = ["cronjobs", "tools"];

/** Human-readable one-liners; the model sees these verbatim. */
const DESCRIPTIONS: Record<CronjobToolName, string> = {
  cronjob_validate:
    "Validate a cronjob definition's YAML without saving it. Use this before cronjob_upsert to check your own config; the result reports stable error codes with their positions.",
  cronjob_upsert:
    "Create or replace a cronjob definition from YAML. The definition is validated first and rejected as a whole if any part is defective, so a broken config never replaces a working one.",
  cronjob_list:
    "List every cronjob with its schedule, time zone, enabled state and next fire time.",
  cronjob_get: "Read one cronjob by id, including its next fire time.",
  cronjob_enable:
    "Enable or disable a cronjob. Disabling stops scheduling but keeps the job, its definition and its run history, and the job can still be fired manually.",
  cronjob_delete:
    "Delete a cronjob definition. Scheduling stops immediately; run history is retained until retention prunes it.",
  cronjob_run_now:
    "Run a cronjob immediately, outside its schedule. Re-reads and re-validates the definition exactly like a scheduled fire, and does not consume a scheduled occurrence.",
  cronjob_runs:
    "List recent runs of a cronjob, newest first, with each run's terminal status, trigger and error code.",
  cronjob_log: "Read one run's JSON Lines log, bounded by maxBytes.",
};

const JOB_ID = {
  type: "string",
  required: true,
  description: "Exact cronjob id, matching the definition file name.",
} as const;

const YAML_ARG = {
  type: "string",
  required: true,
  description: "Full definition document: one YAML sequence holding exactly one job.",
} as const;

const RUN_ID = {
  type: "string",
  required: true,
  description: "Exact run id.",
} as const;

const LIMIT = {
  type: "integer",
  description: "Maximum runs to return; defaults to 20.",
} as const;

const MAX_BYTES = {
  type: "integer",
  description: "Maximum bytes of log to return; defaults to 65536.",
} as const;

const ENABLED = {
  type: "boolean",
  required: true,
  description: "True to schedule the job, false to stop scheduling it.",
} as const;

/** One JSON value rendered as text; the runtime validated it already. */
function renderJson(_args: unknown, value: unknown): { type: "text"; text: string }[] {
  return [{ type: "text", text: JSON.stringify(value) }];
}

/**
 * Late-bound service handle.
 *
 * The tools close over this rather than over a service instance, so a reload
 * that republishes `cronjobs` is picked up without re-registering nine tool
 * definitions — which would collide on their names.
 */
const serviceRef: { current: CronjobService | undefined } = { current: undefined };

/**
 * Build one tool definition.
 *
 * The handler returns a discriminated `{ok, ...}` value rather than throwing: a
 * validation failure is information the model needs in order to fix its YAML,
 * not a transport error. Thrown errors are converted rather than allowed to
 * escape as an opaque failure.
 */
function jobTool(
  toolName: CronjobToolName,
  parameters: ParameterSchemaSpec,
  title: string,
) {
  return defineTool({
    name: toolName,
    description: DESCRIPTIONS[toolName],
    parameters,
    output: {
      // The value is the handler's own JSON projection; keys vary per action,
      // so the object is open and validated as lossless JSON.
      schema: { type: "object", additionalProperties: true },
      render: renderJson,
    },
    async execute(args, exec) {
      const service = serviceRef.current;
      if (service === undefined) {
        return {
          ok: false,
          error: { code: "service_unavailable", message: "the cronjobs service is not mounted" },
        };
      }
      if (exec.signal.aborted) {
        return { ok: false, error: { code: "cancelled", message: `${toolName} was cancelled` } };
      }
      try {
        return (await CRONJOB_HANDLERS[toolName](
          service,
          args as Record<string, unknown>,
        )) as unknown as Record<string, never>;
      } catch (error) {
        return {
          ok: false,
          error: {
            code: "internal_error",
            message: error instanceof Error ? error.message : String(error),
          },
        };
      }
    },
    presentCall(args) {
      return { card: "generic", title, kind: "other", rawInput: args };
    },
  });
}

interface CronjobToolContext {
  readonly cronjobs: CronjobService;
  readonly tools: {
    register(definition: unknown): () => void;
  };
  effect(callback: () => () => void): void;
}

/**
 * Register the tool surface.
 *
 * Every definition carries an explicit `output.schema`, because the runtime
 * validates the canonical value against it, and a `presentCall` so the UI shows
 * a title rather than a raw argument dump.
 */
export function apply(ctx: CronjobToolContext): void {
  serviceRef.current = ctx.cronjobs;
  const tools = ctx.tools as { register(definition: unknown): () => void };

  ctx.effect(() => {
    const disposers: (() => void)[] = [];
    const add = (definition: unknown): void => {
      disposers.push(tools.register(definition));
    };

    add(jobTool("cronjob_validate", { cronjobId: JOB_ID, yaml: YAML_ARG }, "Validate cronjob"));
    add(jobTool("cronjob_upsert", { cronjobId: JOB_ID, yaml: YAML_ARG }, "Save cronjob"));
    add(jobTool("cronjob_list", {}, "List cronjobs"));
    add(jobTool("cronjob_get", { cronjobId: JOB_ID }, "Read cronjob"));
    add(jobTool("cronjob_enable", { cronjobId: JOB_ID, enabled: ENABLED }, "Toggle cronjob"));
    add(jobTool("cronjob_delete", { cronjobId: JOB_ID }, "Delete cronjob"));
    add(jobTool("cronjob_run_now", { cronjobId: JOB_ID }, "Run cronjob now"));
    add(jobTool("cronjob_runs", { cronjobId: JOB_ID, limit: LIMIT }, "List cronjob runs"));
    add(jobTool("cronjob_log", { cronjobId: JOB_ID, runId: RUN_ID, maxBytes: MAX_BYTES }, "Read cronjob log"));

    return () => {
      for (const dispose of disposers) dispose();
      disposers.length = 0;
      serviceRef.current = undefined;
    };
  });
}

/** The plugin value, for loaders that mount an object directly. */
const plugin = { name, inject, apply };
export default plugin;
