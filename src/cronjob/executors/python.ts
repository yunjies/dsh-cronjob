/**
 * Python script node executor.
 *
 * Two boundaries define this module. First, the script's real path must sit
 * inside the controlled artifact root, so a definition cannot aim execution at
 * an arbitrary file. Second, the process is spawned from an argv array and
 * never through a shell, so no value reaching this executor can turn into a
 * command.
 *
 * @module @deepseek-ai/dsh-cronjob/executors/python
 */

import { spawn } from "node:child_process";
import { realpath } from "node:fs/promises";
import { isAbsolute, relative, resolve, sep } from "node:path";

import type { JsonValue, PythonScriptNode } from "../contracts.js";
import { MAX_NODE_OUTPUT_BYTES } from "../contracts.js";
import { resolveWithin } from "../storage.js";

/** Outcome of one node execution; always JSON-safe. */
export interface NodeResult {
  readonly status: "succeeded" | "failed" | "timed_out" | "cancelled";
  readonly errorCode?: string;
  readonly message?: string;
  readonly exitCode?: number;
  /** Truncated stdout; the full text is written to the run's artifact. */
  readonly stdout?: string;
  readonly stderr?: string;
  readonly durationMs: number;
}

/** The subprocess seam, so tests can run without spawning. */
export interface SubprocessRunner {
  run(request: {
    readonly command: string;
    readonly args: readonly string[];
    readonly cwd: string;
    readonly env: Readonly<Record<string, string>>;
    readonly stdin?: string;
    readonly timeoutMs: number;
    readonly signal?: AbortSignal;
  }): Promise<{
    readonly code: number | null;
    readonly signal: NodeJS.Signals | null;
    readonly stdout: string;
    readonly stderr: string;
    readonly timedOut: boolean;
    readonly truncated: boolean;
  }>;
}

export interface PythonExecutorOptions {
  /** Absolute path of the controlled scripts root. */
  readonly scriptsRoot: string;
  readonly runner: SubprocessRunner;
  /** Interpreter; the deployment's pinned python. */
  readonly interpreter?: string;
  /** Environment handed to the child. */
  readonly baseEnv?: Readonly<Record<string, string>>;
}

export interface ExecuteContext {
  readonly context?: Readonly<Record<string, JsonValue>>;
  readonly previous?: Readonly<Record<string, JsonValue>>;
  readonly runId: string;
  readonly cronjobId: string;
}

export class PythonExecutor {
  readonly #scriptsRoot: string;
  readonly #runner: SubprocessRunner;
  readonly #interpreter: string;
  readonly #baseEnv: Readonly<Record<string, string>>;

  constructor(options: PythonExecutorOptions) {
    this.#scriptsRoot = resolve(options.scriptsRoot);
    this.#runner = options.runner;
    this.#interpreter = options.interpreter ?? "python3";
    this.#baseEnv = options.baseEnv ?? {};
  }

  /**
   * Resolve the script under the scripts root and prove it stays there.
   *
   * `realpath` is what actually enforces the boundary: a symlink placed inside
   * the root could otherwise point anywhere, and validation only saw the
   * lexical path.
   */
  async resolveScript(scriptPath: string): Promise<string> {
    const lexical = resolveWithin(this.#scriptsRoot, scriptPath);
    let real: string;
    try {
      real = await realpath(lexical);
    } catch {
      throw new ScriptPathError(`script not found: ${scriptPath}`);
    }
    const rootReal = await realpath(this.#scriptsRoot).catch(() => this.#scriptsRoot);
    const rel = relative(rootReal, real);
    if (rel.startsWith("..") || isAbsolute(rel) || rel.split(sep).includes("..")) {
      throw new ScriptPathError(`script resolves outside the artifact root: ${scriptPath}`);
    }
    return real;
  }

  async execute(
    node: PythonScriptNode,
    context: ExecuteContext,
    options: { readonly timeoutMs: number; readonly signal?: AbortSignal },
  ): Promise<NodeResult> {
    const startedAt = Date.now();
    let script: string;
    try {
      script = await this.resolveScript(node.scriptPath);
    } catch (error) {
      return {
        status: "failed",
        errorCode: error instanceof ScriptPathError ? "invalid_script_path" : "io_error",
        message: describe(error),
        durationMs: Date.now() - startedAt,
      };
    }

    // Parameters and context reach the child as data on stdin, never as
    // arguments that a shell could reinterpret.
    const stdin = JSON.stringify({
      params: node.scriptParams ?? {},
      context: context.context ?? {},
      previous: context.previous ?? {},
      run: { runId: context.runId, cronjobId: context.cronjobId, nodeId: node.nodeId },
    });

    const outcome = await this.#runner.run({
      command: this.#interpreter,
      args: [script],
      cwd: this.#scriptsRoot,
      env: { ...this.#baseEnv, PYTHONUNBUFFERED: "1" },
      stdin,
      timeoutMs: options.timeoutMs,
      ...(options.signal === undefined ? {} : { signal: options.signal }),
    });

    const durationMs = Date.now() - startedAt;
    const stdout = truncate(outcome.stdout);
    const stderr = truncate(outcome.stderr);

    if (outcome.timedOut) {
      return { status: "timed_out", errorCode: "node_timeout", message: "script exceeded its timeout", stdout, stderr, durationMs };
    }
    if (options.signal?.aborted === true) {
      return { status: "cancelled", errorCode: "node_cancelled", message: "script was cancelled", stdout, stderr, durationMs };
    }
    if (outcome.code !== 0) {
      return {
        status: "failed",
        errorCode: "node_exit_nonzero",
        message: `script exited with ${outcome.code ?? outcome.signal ?? "unknown"}`,
        ...(outcome.code === null ? {} : { exitCode: outcome.code }),
        stdout,
        stderr,
        durationMs,
      };
    }
    return { status: "succeeded", exitCode: 0, stdout, stderr, durationMs };
  }
}

export class ScriptPathError extends Error {
  override readonly name = "ScriptPathError";
}

function truncate(text: string): string {
  const buffer = Buffer.from(text, "utf8");
  if (buffer.byteLength <= MAX_NODE_OUTPUT_BYTES) return text;
  return `${buffer.subarray(0, MAX_NODE_OUTPUT_BYTES).toString("utf8")}\n… (truncated)`;
}

/**
 * The production subprocess runner.
 *
 * Cancellation kills the whole process group: a script that forked children
 * would otherwise leave them running after the run was abandoned.
 */
export function createSubprocessRunner(): SubprocessRunner {
  return {
    run(request) {
      return new Promise((resolvePromise) => {
        const child = spawn(request.command, [...request.args], {
          cwd: request.cwd,
          env: { ...request.env },
          stdio: ["pipe", "pipe", "pipe"],
          detached: process.platform !== "win32",
        });

        let stdout = "";
        let stderr = "";
        let truncated = false;
        let timedOut = false;
        let settled = false;

        const capture = (chunk: Buffer, sink: "out" | "err"): void => {
          const text = chunk.toString("utf8");
          if (sink === "out") {
            if (stdout.length < MAX_NODE_OUTPUT_BYTES * 2) stdout += text;
            else truncated = true;
          } else if (stderr.length < MAX_NODE_OUTPUT_BYTES * 2) stderr += text;
        };

        child.stdout.on("data", (chunk: Buffer) => capture(chunk, "out"));
        child.stderr.on("data", (chunk: Buffer) => capture(chunk, "err"));

        const killTree = (): void => {
          if (child.pid === undefined) return;
          try {
            if (process.platform === "win32") child.kill();
            else process.kill(-child.pid, "SIGKILL");
          } catch {
            child.kill("SIGKILL");
          }
        };

        const timer = setTimeout(() => {
          timedOut = true;
          killTree();
        }, request.timeoutMs);

        const onAbort = (): void => killTree();
        request.signal?.addEventListener("abort", onAbort, { once: true });

        const finish = (code: number | null, signal: NodeJS.Signals | null): void => {
          if (settled) return;
          settled = true;
          clearTimeout(timer);
          request.signal?.removeEventListener("abort", onAbort);
          resolvePromise({ code, signal, stdout, stderr, timedOut, truncated });
        };

        child.on("error", (error) => {
          stderr += describe(error);
          finish(null, null);
        });
        child.on("close", (code, signal) => finish(code, signal));

        if (request.stdin !== undefined) {
          child.stdin.end(request.stdin);
        } else {
          child.stdin.end();
        }
      });
    },
  };
}

function describe(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}
