/**
 * Definition validation.
 *
 * The rule this module exists to enforce: **every fire re-reads and fully
 * re-validates the current YAML**, and any defect rejects the run before a
 * subprocess or a subagent is created. A misconfigured job that runs anyway
 * burns quota on every tick and alerts every time; refusing it before the first
 * model call is what makes that failure cheap.
 *
 * @module @deepseek-ai/dsh-cronjob/validation
 */

import { parse as parseYaml, YAMLParseError } from "yaml";

import type {
  CanonicalDefinition,
  CronjobDefinition,
  JsonValue,
  MisfirePolicy,
  ValidationCode,
  ValidationDiagnostic,
  WorkflowNode,
} from "./contracts.js";
import {
  MAX_TIMEOUT_SECONDS,
  MIN_TIMEOUT_SECONDS,
} from "./contracts.js";
import { canonicalize, digestOf } from "./storage.js";
import { computeNextFire, isValidTimeZone, parseCronExpression } from "./scheduler.js";

/** Result of validating one definition: a canonical form or its diagnostics. */
export type ValidationResult =
  | { readonly ok: true; readonly canonical: CanonicalDefinition }
  | { readonly ok: false; readonly diagnostics: readonly ValidationDiagnostic[] };

/** Fields accepted at the top level of a definition; anything else is unknown. */
const DEFINITION_FIELDS = new Set([
  "cronjobId",
  "cronjobName",
  "scheduleTime",
  "timeZone",
  "bindSessionId",
  "context",
  "enabled",
  "timeoutSeconds",
  "maxConcurrentRuns",
  "misfirePolicy",
  "workflow",
]);

const SCRIPT_NODE_FIELDS = new Set([
  "nodeId",
  "nodeType",
  "scriptPath",
  "scriptParams",
]);

const SUBAGENT_NODE_FIELDS = new Set([
  "nodeId",
  "nodeType",
  "prompt",
  "provider",
  "outputFormat",
]);

class DiagnosticSink {
  readonly items: ValidationDiagnostic[] = [];

  add(
    code: ValidationCode,
    path: string,
    message: string,
    position?: { line?: number; column?: number },
  ): void {
    this.items.push({
      code,
      path,
      message,
      ...(position?.line === undefined ? {} : { line: position.line }),
      ...(position?.column === undefined ? {} : { column: position.column }),
    });
  }

  get failed(): boolean {
    return this.items.length > 0;
  }
}

/**
 * Validate one definition text for a known job identity.
 *
 * `expectedId` binds the parsed document to the file it came from: a file whose
 * inner id disagrees with its name would otherwise let one file impersonate
 * another job's schedule and notification target.
 */
export function validateDefinitionText(
  text: string,
  expectedId: string,
  options: { readonly now?: Date } = {},
): ValidationResult {
  const sink = new DiagnosticSink();

  let document: unknown;
  try {
    document = parseYaml(text, { maxAliasCount: 100 });
  } catch (error) {
    // Only include a position when the parser actually supplied one: under
    // `exactOptionalPropertyTypes` an explicit `undefined` is not the same as
    // an absent key.
    const line = error instanceof YAMLParseError ? error.linePos?.[0]?.line : undefined;
    const column = error instanceof YAMLParseError ? error.linePos?.[0]?.col : undefined;
    sink.add(
      "yaml_syntax",
      "<root>",
      safeParserMessage(error),
      line === undefined || column === undefined ? undefined : { line, column },
    );
    return { ok: false, diagnostics: sink.items };
  }

  const list = normalizeDefinitionList(document);
  if (list === undefined) {
    sink.add("schema_invalid", "<root>", "definition file must be a YAML sequence of jobs");
    return { ok: false, diagnostics: sink.items };
  }
  if (list.length === 0) {
    sink.add("schema_invalid", "<root>", "definition file contains no job");
    return { ok: false, diagnostics: sink.items };
  }

  const definitions: CronjobDefinition[] = [];
  const seenIds = new Set<string>();

  for (const [index, entry] of list.entries()) {
    const at = `[${index}]`;
    if (!isRecord(entry)) {
      sink.add("schema_invalid", at, "job entry must be a mapping");
      continue;
    }
    const id = entry["cronjobId"];
    if (typeof id !== "string" || id.length === 0) {
      sink.add("schema_invalid", `${at}.cronjobId`, "cronjobId is required and must be a string");
      continue;
    }
    if (id !== expectedId) {
      sink.add(
        "schema_invalid",
        `${at}.cronjobId`,
        `cronjobId ${JSON.stringify(id)} does not match its file name ${JSON.stringify(expectedId)}`,
      );
      continue;
    }
    if (seenIds.has(id)) {
      sink.add("duplicate_job_id", `${at}.cronjobId`, `job id ${JSON.stringify(id)} appears twice`);
      continue;
    }
    seenIds.add(id);

    const definition = readDefinition(entry, at, sink, options.now ?? new Date());
    if (definition !== undefined) definitions.push(definition);
  }

  if (sink.failed || definitions.length !== 1) {
    if (!sink.failed && definitions.length !== 1) {
      sink.add("schema_invalid", "<root>", "exactly one job is required per file");
    }
    return { ok: false, diagnostics: sink.items };
  }

  const definition = definitions[0]!;
  return {
    ok: true,
    canonical: { definition, digest: digestOf(canonicalize(definition)) },
  };
}

function readDefinition(
  entry: Record<string, unknown>,
  at: string,
  sink: DiagnosticSink,
  now: Date,
): CronjobDefinition | undefined {
  for (const key of Object.keys(entry)) {
    if (!DEFINITION_FIELDS.has(key)) {
      sink.add("unknown_field", `${at}.${key}`, `unknown field ${JSON.stringify(key)}`);
    }
  }

  const cronjobName = entry["cronjobName"];
  if (typeof cronjobName !== "string" || cronjobName.trim().length === 0) {
    sink.add("schema_invalid", `${at}.cronjobName`, "cronjobName is required and must be a non-empty string");
  }

  const scheduleTime = entry["scheduleTime"];
  if (typeof scheduleTime !== "string") {
    sink.add("schema_invalid", `${at}.scheduleTime`, "scheduleTime is required and must be a string");
  } else if (!parseCronExpression(scheduleTime)) {
    sink.add("invalid_cron", `${at}.scheduleTime`, `not a valid 5-field cron expression: ${JSON.stringify(scheduleTime)}`);
  }

  const timeZone = entry["timeZone"];
  if (typeof timeZone !== "string") {
    sink.add("schema_invalid", `${at}.timeZone`, "timeZone is required and must be a string");
  } else if (!isValidTimeZone(timeZone)) {
    sink.add("invalid_timezone", `${at}.timeZone`, `unknown IANA time zone: ${JSON.stringify(timeZone)}`);
  }

  // A schedule that can never fire is a definition defect, not a runtime one.
  if (typeof scheduleTime === "string" && typeof timeZone === "string" && !sink.failed) {
    if (computeNextFire(scheduleTime, timeZone, now) === undefined) {
      sink.add("invalid_cron", `${at}.scheduleTime`, "expression has no future occurrence");
    }
  }

  const bindSessionId = entry["bindSessionId"];
  if (bindSessionId !== undefined && typeof bindSessionId !== "string") {
    sink.add("schema_invalid", `${at}.bindSessionId`, "bindSessionId must be a string when present");
  }

  const enabled = entry["enabled"];
  if (enabled !== undefined && typeof enabled !== "boolean") {
    sink.add("schema_invalid", `${at}.enabled`, "enabled must be a boolean");
  }

  const timeoutSeconds = readPositiveInt(
    entry["timeoutSeconds"],
    `${at}.timeoutSeconds`,
    sink,
    "invalid_timeout",
    { min: MIN_TIMEOUT_SECONDS, max: MAX_TIMEOUT_SECONDS },
  );

  const maxConcurrentRuns = readPositiveInt(
    entry["maxConcurrentRuns"],
    `${at}.maxConcurrentRuns`,
    sink,
    "schema_invalid",
    { min: 1, max: 64 },
  );

  let misfirePolicy: MisfirePolicy | undefined;
  const rawMisfire = entry["misfirePolicy"];
  if (rawMisfire === undefined) {
    misfirePolicy = "skip";
  } else if (rawMisfire === "skip") {
    misfirePolicy = "skip";
  } else {
    sink.add(
      "invalid_misfire_policy",
      `${at}.misfirePolicy`,
      `only "skip" is supported; got ${JSON.stringify(rawMisfire)}`,
    );
  }

  const context = entry["context"];
  if (context !== undefined && !isJsonValue(context)) {
    sink.add("schema_invalid", `${at}.context`, "context must be JSON-serializable");
  }

  const workflow = readWorkflow(entry["workflow"], at, sink);

  if (sink.failed) return undefined;

  return {
    cronjobId: entry["cronjobId"] as string,
    cronjobName: cronjobName as string,
    scheduleTime: scheduleTime as string,
    timeZone: timeZone as string,
    ...(bindSessionId === undefined ? {} : { bindSessionId: bindSessionId as string }),
    ...(context === undefined ? {} : { context: context as Record<string, JsonValue> }),
    enabled: (enabled as boolean | undefined) ?? true,
    timeoutSeconds: timeoutSeconds!,
    maxConcurrentRuns: maxConcurrentRuns!,
    misfirePolicy: misfirePolicy!,
    workflow: workflow!,
  };
}

function readWorkflow(
  raw: unknown,
  at: string,
  sink: DiagnosticSink,
): WorkflowNode[] | undefined {
  if (!Array.isArray(raw) || raw.length === 0) {
    sink.add("schema_invalid", `${at}.workflow`, "workflow must be a non-empty sequence");
    return undefined;
  }
  const nodes: WorkflowNode[] = [];
  const seen = new Set<string>();
  for (const [index, nodeRaw] of raw.entries()) {
    const nodeAt = `${at}.workflow[${index}]`;
    if (!isRecord(nodeRaw)) {
      sink.add("schema_invalid", nodeAt, "node must be a mapping");
      continue;
    }
    const nodeId = nodeRaw["nodeId"];
    if (typeof nodeId !== "string" || nodeId.length === 0) {
      sink.add("schema_invalid", `${nodeAt}.nodeId`, "nodeId is required and must be a string");
      continue;
    }
    if (seen.has(nodeId)) {
      sink.add("duplicate_node_id", `${nodeAt}.nodeId`, `node id ${JSON.stringify(nodeId)} appears twice`);
      continue;
    }
    seen.add(nodeId);

    const nodeType = nodeRaw["nodeType"];
    if (nodeType === "pythonScript") {
      readScriptNode(nodeRaw, nodeId, nodeAt, sink, nodes);
    } else if (nodeType === "subagent") {
      readSubagentNode(nodeRaw, nodeId, nodeAt, sink, nodes);
    } else {
      sink.add(
        "invalid_node",
        `${nodeAt}.nodeType`,
        `nodeType must be "pythonScript" or "subagent"; got ${JSON.stringify(nodeType)}`,
      );
    }
  }
  return sink.failed ? undefined : nodes;
}

/**
 * A script path is only ever relative and only ever resolved under the
 * controlled scripts root. An absolute path, a traversal segment or a
 * non-`.py` suffix is refused here rather than at exec time, so a definition
 * cannot name a target the executor would then have to defend against.
 */
function readScriptNode(
  node: Record<string, unknown>,
  nodeId: string,
  at: string,
  sink: DiagnosticSink,
  out: WorkflowNode[],
): void {
  for (const key of Object.keys(node)) {
    if (!SCRIPT_NODE_FIELDS.has(key)) {
      sink.add("unknown_field", `${at}.${key}`, `unknown field ${JSON.stringify(key)}`);
    }
  }
  const scriptPath = node["scriptPath"];
  if (typeof scriptPath !== "string" || scriptPath.length === 0) {
    sink.add("schema_invalid", `${at}.scriptPath`, "scriptPath is required for pythonScript nodes");
    return;
  }
  const defect = describeScriptPathDefect(scriptPath);
  if (defect !== undefined) {
    sink.add("invalid_script_path", `${at}.scriptPath`, defect);
    return;
  }
  const scriptParams = node["scriptParams"];
  if (scriptParams !== undefined && !isJsonValue(scriptParams)) {
    sink.add("schema_invalid", `${at}.scriptParams`, "scriptParams must be JSON-serializable");
    return;
  }
  out.push({
    nodeType: "pythonScript",
    nodeId,
    scriptPath,
    ...(scriptParams === undefined
      ? {}
      : { scriptParams: scriptParams as Record<string, JsonValue> }),
  });
}

function readSubagentNode(
  node: Record<string, unknown>,
  nodeId: string,
  at: string,
  sink: DiagnosticSink,
  out: WorkflowNode[],
): void {
  for (const key of Object.keys(node)) {
    if (!SUBAGENT_NODE_FIELDS.has(key)) {
      sink.add("unknown_field", `${at}.${key}`, `unknown field ${JSON.stringify(key)}`);
    }
  }
  const prompt = node["prompt"];
  if (typeof prompt !== "string" || prompt.trim().length === 0) {
    sink.add("invalid_node", `${at}.prompt`, "prompt is required for subagent nodes");
    return;
  }
  const provider = node["provider"];
  if (provider !== undefined && typeof provider !== "string") {
    sink.add("invalid_node", `${at}.provider`, "provider must be a string when present");
    return;
  }
  const outputFormat = node["outputFormat"];
  if (outputFormat !== undefined && outputFormat !== "text" && outputFormat !== "json") {
    sink.add("invalid_node", `${at}.outputFormat`, 'outputFormat must be "text" or "json"');
    return;
  }
  out.push({
    nodeType: "subagent",
    nodeId,
    prompt,
    ...(provider === undefined ? {} : { provider }),
    ...(outputFormat === undefined ? {} : { outputFormat }),
  });
}

/** Why this script path is unacceptable, or `undefined` when it is fine. */
export function describeScriptPathDefect(scriptPath: string): string | undefined {
  if (scriptPath.includes("\0")) return "scriptPath must not contain a NUL byte";
  if (scriptPath.startsWith("/") || /^[A-Za-z]:[\\/]/.test(scriptPath)) {
    return "scriptPath must be relative to the artifact scripts root";
  }
  if (scriptPath.split(/[\\/]/).some((segment) => segment === "..")) {
    return "scriptPath must not traverse outside the artifact scripts root";
  }
  if (!scriptPath.endsWith(".py")) return "scriptPath must name a .py file";
  return undefined;
}

function readPositiveInt(
  raw: unknown,
  path: string,
  sink: DiagnosticSink,
  code: ValidationCode,
  bounds: { readonly min: number; readonly max: number },
): number | undefined {
  if (raw === undefined) {
    sink.add(code, path, "required field is missing");
    return undefined;
  }
  if (typeof raw !== "number" || !Number.isSafeInteger(raw)) {
    sink.add(code, path, "must be a safe integer");
    return undefined;
  }
  if (raw < bounds.min || raw > bounds.max) {
    sink.add(code, path, `must be between ${bounds.min} and ${bounds.max}`);
    return undefined;
  }
  return raw;
}

/** A definition file holds exactly one job; a bare mapping is accepted as that job. */
function normalizeDefinitionList(document: unknown): unknown[] | undefined {
  if (Array.isArray(document)) return document;
  if (isRecord(document)) return [document];
  return undefined;
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function isJsonValue(value: unknown, depth = 0): boolean {
  if (depth > 32) return false;
  if (value === null) return true;
  const type = typeof value;
  if (type === "string" || type === "boolean") return true;
  if (type === "number") return Number.isFinite(value as number);
  if (Array.isArray(value)) return value.every((item) => isJsonValue(item, depth + 1));
  if (type === "object") {
    return Object.values(value as Record<string, unknown>).every((item) =>
      isJsonValue(item, depth + 1),
    );
  }
  return false;
}

/**
 * Reduce a parser error to a safe message.
 *
 * The raw message can quote the offending source line, which would copy
 * definition content into an error channel that may be surfaced to a session.
 */
function safeParserMessage(error: unknown): string {
  if (error instanceof YAMLParseError) {
    const line = error.linePos?.[0];
    return line === undefined
      ? "YAML syntax error"
      : `YAML syntax error at line ${line.line}, column ${line.col}`;
  }
  return "YAML syntax error";
}
