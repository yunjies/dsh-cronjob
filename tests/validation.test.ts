/**
 * Definition validation.
 *
 * The behaviour under test is the one the whole package leans on: a defect is
 * found before anything executes, and the verdict is a stable code rather than
 * a message a caller has to parse.
 */

import { describe, expect, it } from "vitest";

import { validateDefinitionText } from "../src/cronjob/validation.js";
import { definitionYaml } from "./helpers.js";

const AT = new Date("2026-03-01T08:00:00Z");

function codes(text: string, id = "daily-report"): string[] {
  const result = validateDefinitionText(text, id, { now: AT });
  return result.ok ? [] : result.diagnostics.map((d) => d.code);
}

describe("validateDefinitionText", () => {
  it("accepts a well-formed definition and reports its digest", () => {
    const result = validateDefinitionText(definitionYaml(), "daily-report", { now: AT });
    expect(result.ok).toBe(true);
    if (!result.ok) return;
    expect(result.canonical.definition.cronjobId).toBe("daily-report");
    expect(result.canonical.definition.timeZone).toBe("UTC");
    expect(result.canonical.digest).toMatch(/^[0-9a-f]{64}$/);
  });

  it("reports a YAML syntax error at a position", () => {
    const result = validateDefinitionText("- cronjobId: [unclosed\n", "daily-report", { now: AT });
    expect(result.ok).toBe(false);
    if (result.ok) return;
    expect(result.diagnostics[0]?.code).toBe("yaml_syntax");
    expect(result.diagnostics[0]?.line).toBeTypeOf("number");
  });

  it("refuses a file whose inner id disagrees with its name", () => {
    const text = definitionYaml({ cronjobId: "other-job" });
    const result = validateDefinitionText(text, "daily-report", { now: AT });
    expect(result.ok).toBe(false);
    if (result.ok) return;
    expect(result.diagnostics[0]?.message).toContain("does not match its file name");
  });

  it("rejects an unknown field instead of ignoring it", () => {
    const text = `${definitionYaml()}  typoField: 1\n`;
    expect(codes(text)).toContain("unknown_field");
  });

  it("rejects an invalid cron expression and an invalid zone", () => {
    expect(codes(definitionYaml({ scheduleTime: "not a cron" }))).toContain("invalid_cron");
    expect(codes(definitionYaml({ timeZone: "Mars/Olympus" }))).toContain("invalid_timezone");
  });

  it("rejects an out-of-range timeout", () => {
    expect(codes(definitionYaml({ timeoutSeconds: 0 }))).toContain("invalid_timeout");
    expect(codes(definitionYaml({ timeoutSeconds: 999_999 }))).toContain("invalid_timeout");
  });

  it("rejects a script path that escapes the artifact root", () => {
    for (const scriptPath of ["/etc/passwd.py", "../../secret.py", "a/../../b.py", "notes.txt"]) {
      const text = definitionYaml({
        nodes: [
          "    - nodeId: collect",
          "      nodeType: pythonScript",
          `      scriptPath: "${scriptPath}"`,
        ].join("\n"),
      });
      expect(codes(text), scriptPath).toContain("invalid_script_path");
    }
  });

  it("rejects duplicate node ids", () => {
    const text = definitionYaml({
      nodes: [
        "    - nodeId: same",
        "      nodeType: pythonScript",
        '      scriptPath: "a.py"',
        "    - nodeId: same",
        "      nodeType: pythonScript",
        '      scriptPath: "b.py"',
      ].join("\n"),
    });
    expect(codes(text)).toContain("duplicate_node_id");
  });

  it("rejects a misfire policy other than skip", () => {
    const text = definitionYaml().replace("misfirePolicy: skip", "misfirePolicy: replay");
    expect(codes(text)).toContain("invalid_misfire_policy");
  });

  it("rejects an unknown node type", () => {
    const text = definitionYaml({
      nodes: [
        "    - nodeId: n1",
        "      nodeType: shellCommand",
        '      command: "rm -rf /"',
      ].join("\n"),
    });
    expect(codes(text)).toContain("invalid_node");
  });

  it("accepts a subagent node with a json output format", () => {
    const text = definitionYaml({
      nodes: [
        "    - nodeId: summarize",
        "      nodeType: subagent",
        '      prompt: "Summarize the collected data."',
        "      outputFormat: json",
      ].join("\n"),
    });
    const result = validateDefinitionText(text, "daily-report", { now: AT });
    expect(result.ok).toBe(true);
    if (!result.ok) return;
    expect(result.canonical.definition.workflow[0]?.nodeType).toBe("subagent");
  });

  it("keeps the digest stable across an unchanged re-validation", () => {
    const a = validateDefinitionText(definitionYaml(), "daily-report", { now: AT });
    const b = validateDefinitionText(definitionYaml(), "daily-report", {
      now: new Date(AT.getTime() + 3_600_000),
    });
    // The digest identifies the definition, not the moment it was read, so an
    // unrelated clock difference must not look like a definition change.
    expect(a.ok && b.ok && a.canonical.digest === b.canonical.digest).toBe(true);
  });

  it("changes the digest when the definition changes", () => {
    const a = validateDefinitionText(definitionYaml(), "daily-report", { now: AT });
    const b = validateDefinitionText(definitionYaml({ scheduleTime: "30 9 * * *" }), "daily-report", {
      now: AT,
    });
    expect(a.ok && b.ok && a.canonical.digest !== b.canonical.digest).toBe(true);
  });
});

describe("shipped example", () => {
  it("validates, so the documented example cannot drift from the schema", async () => {
    const { readFile } = await import("node:fs/promises");
    const { fileURLToPath } = await import("node:url");
    const { dirname, join } = await import("node:path");
    const here = dirname(fileURLToPath(import.meta.url));
    const text = await readFile(join(here, "..", "examples", "daily-report.yaml"), "utf8");

    const result = validateDefinitionText(text, "daily-report", { now: AT });
    if (!result.ok) {
      throw new Error(
        `examples/daily-report.yaml no longer validates: ${JSON.stringify(result.diagnostics)}`,
      );
    }
    expect(result.canonical.definition.workflow).toHaveLength(2);
  });
});
