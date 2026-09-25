/**
 * Subagent node executor.
 *
 * The child's answer is untrusted data. It is captured, size-limited and
 * recorded as an artifact; it is never re-interpreted as an instruction for a
 * later node. Cancelling the parent must also interrupt the child, so no
 * background work outlives the run that started it.
 *
 * @module @deepseek-ai/dsh-cronjob/executors/subagent
 */

import type { JsonValue, SubagentNode } from "../contracts.js";
import { MAX_NODE_OUTPUT_BYTES } from "../contracts.js";
import type { NodeResult } from "./python.js";

/** One delegation request, in this package's own vocabulary. */
export interface SubagentRunRequest {
  readonly provider: string;
  readonly prompt: string;
  readonly outputFormat: "text" | "json";
  readonly context: Readonly<Record<string, JsonValue>>;
  readonly runId: string;
  readonly cronjobId: string;
  readonly nodeId: string;
}

export interface SubagentHandle {
  readonly childId: string;
  /** Await the child's answer. Rejects only on transport failure. */
  readonly result: Promise<{ readonly text: string; readonly structured?: JsonValue }>;
  /** Interrupt the child; safe to call after settlement. */
  interrupt(): Promise<void>;
}

/** The delegation seam, mirroring the Host subagent service. */
export interface SubagentProvider {
  readonly name: string;
  start(request: SubagentRunRequest): Promise<SubagentHandle>;
}

export interface SubagentExecutorOptions {
  readonly providers: readonly SubagentProvider[];
  /** Provider used when the node does not name one. */
  readonly defaultProvider?: string;
  /** Called with the provider actually used, so the log records the truth. */
  readonly onProviderResolved?: (info: {
    readonly cronjobId: string;
    readonly runId: string;
    readonly nodeId: string;
    readonly provider: string;
    readonly explicit: boolean;
  }) => void;
}

export class SubagentExecutor {
  readonly #providers: Map<string, SubagentProvider>;
  readonly #defaultProvider: string | undefined;
  readonly #onProviderResolved: SubagentExecutorOptions["onProviderResolved"];

  constructor(options: SubagentExecutorOptions) {
    this.#providers = new Map(options.providers.map((provider) => [provider.name, provider]));
    this.#defaultProvider = options.defaultProvider ?? options.providers[0]?.name;
    this.#onProviderResolved = options.onProviderResolved;
  }

  /** Provider names this executor can reach. */
  get availableProviders(): string[] {
    return [...this.#providers.keys()].sort();
  }

  async execute(
    node: SubagentNode,
    context: {
      readonly context?: Readonly<Record<string, JsonValue>>;
      readonly previous?: Readonly<Record<string, JsonValue>>;
      readonly runId: string;
      readonly cronjobId: string;
    },
    options: { readonly timeoutMs: number; readonly signal?: AbortSignal },
  ): Promise<NodeResult> {
    const startedAt = Date.now();
    const explicit = node.provider !== undefined;
    const providerName = node.provider ?? this.#defaultProvider;

    if (providerName === undefined) {
      return {
        status: "failed",
        errorCode: "no_provider",
        message: "no subagent provider is registered and the node names none",
        durationMs: Date.now() - startedAt,
      };
    }
    const provider = this.#providers.get(providerName);
    if (provider === undefined) {
      return {
        status: "failed",
        errorCode: "unknown_provider",
        message: `unknown subagent provider: ${providerName}`,
        durationMs: Date.now() - startedAt,
      };
    }

    this.#onProviderResolved?.({
      cronjobId: context.cronjobId,
      runId: context.runId,
      nodeId: node.nodeId,
      provider: providerName,
      explicit,
    });

    let handle: SubagentHandle;
    try {
      handle = await provider.start({
        provider: providerName,
        prompt: node.prompt,
        outputFormat: node.outputFormat ?? "text",
        context: { ...(context.context ?? {}), ...(context.previous ?? {}) },
        runId: context.runId,
        cronjobId: context.cronjobId,
        nodeId: node.nodeId,
      });
    } catch (error) {
      return {
        status: "failed",
        errorCode: "subagent_start_failed",
        message: describe(error),
        durationMs: Date.now() - startedAt,
      };
    }

    // Cancelling the parent must interrupt the child too, otherwise the child
    // keeps consuming quota after the run it belonged to is gone.
    const onAbort = (): void => {
      void handle.interrupt().catch(() => undefined);
    };
    options.signal?.addEventListener("abort", onAbort, { once: true });

    try {
      const answer = await withTimeout(handle.result, options.timeoutMs);
      if (answer === TIMEOUT) {
        await handle.interrupt().catch(() => undefined);
        return {
          status: "timed_out",
          errorCode: "node_timeout",
          message: "subagent exceeded its timeout",
          durationMs: Date.now() - startedAt,
        };
      }
      if (options.signal?.aborted === true) {
        await handle.interrupt().catch(() => undefined);
        return {
          status: "cancelled",
          errorCode: "node_cancelled",
          message: "subagent was cancelled",
          durationMs: Date.now() - startedAt,
        };
      }
      return {
        status: "succeeded",
        stdout: truncate(answer.text),
        durationMs: Date.now() - startedAt,
      };
    } catch (error) {
      return {
        status: "failed",
        errorCode: "subagent_failed",
        message: describe(error),
        durationMs: Date.now() - startedAt,
      };
    } finally {
      options.signal?.removeEventListener("abort", onAbort);
    }
  }
}

const TIMEOUT = Symbol("timeout");

async function withTimeout<T>(promise: Promise<T>, ms: number): Promise<T | typeof TIMEOUT> {
  let timer: NodeJS.Timeout | undefined;
  try {
    return await Promise.race([
      promise,
      new Promise<typeof TIMEOUT>((resolve) => {
        timer = setTimeout(() => resolve(TIMEOUT), ms);
      }),
    ]);
  } finally {
    if (timer !== undefined) clearTimeout(timer);
  }
}

function truncate(text: string): string {
  const buffer = Buffer.from(text, "utf8");
  if (buffer.byteLength <= MAX_NODE_OUTPUT_BYTES) return text;
  return `${buffer.subarray(0, MAX_NODE_OUTPUT_BYTES).toString("utf8")}\n… (truncated)`;
}

function describe(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}
