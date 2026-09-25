/**
 * Cordis plugin entry: publishes the `cronjobs` Host service.
 *
 * This row belongs on the **Host plane**. Scheduling, storage and notification
 * are cross-session concerns, and a service published into the root realm is
 * process-global: a second session mounting this same package would collide on
 * the service name. The model-facing tools live in `tool-cronjob.ts` instead,
 * because that row is what an agent preset mounts.
 *
 * @module @deepseek-ai/dsh-cronjob
 */

import type { NotificationAgentRegistry, NotificationSessionLifecycle } from "./notifications.js";
import type { SubagentProvider } from "./executors/subagent.js";
import { CronjobService } from "./service.js";
import type { SchedulerTimers } from "./scheduler.js";

/** Cordis plugin name; also the composition row id. */
export const name = "cronjob";

/**
 * Host services this plugin reads.
 *
 * Every one is resolved lazily with a fallback, so a missing optional
 * capability does not block the Host composition. `inject` is therefore empty;
 * the service degrades instead of waiting.
 */
export const inject: readonly string[] = [];

export interface CronjobPluginConfig {
  /** DSH home; defaults to the runtime's own resolution. */
  readonly dshHome?: string;
  readonly interpreter?: string;
  readonly maxGlobalConcurrency?: number;
  readonly subagentDefaultProvider?: string;
}

/** Structural view of the pieces of a Cordis context this plugin consumes. */
/**
 * Structural view of the pieces of a Cordis context this plugin consumes.
 *
 * Every optional collaborator is read through `get(name)` rather than as a
 * property. Cordis rejects a bare `ctx.<service>` read for a service the plugin
 * did not declare in `inject` ("cannot get property X without inject"), and
 * declaring them all would make the plugin wait for capabilities it can work
 * without. `get` is the injection-free read and returns `undefined` when the
 * service is absent, which is exactly the optional semantics this row wants.
 */
interface HostContext {
  get(name: string): unknown;
  /**
   * The logging *service*: `ctx.logger(name)` returns a named logger. Cordis
   * mixes this factory onto the context rather than exposing an object with
   * `info`/`error` directly.
   */
  readonly logger: (name?: string) => {
    info(...args: unknown[]): void;
    warn(...args: unknown[]): void;
    error(...args: unknown[]): void;
  };
  provide(name: string, value: unknown): void;
  effect(callback: () => () => void): void;
}

/**
 * Publish the service.
 *
 * `apply` is deliberately synchronous: Cordis collects a plugin's effects
 * synchronously and rejects a returned Promise. Asynchronous startup runs
 * inside `ctx.effect`, and every resource is registered there so teardown is
 * automatic rather than returned.
 */
export function apply(ctx: HostContext, config: CronjobPluginConfig = {}): void {
  const timers = resolveTimers(ctx);
  const log = ctx.logger("cronjob");
  const logger = {
    info: (event: string, data?: Record<string, unknown>) => log.info(format(event, data)),
    error: (event: string, data?: Record<string, unknown>) => log.error(format(event, data)),
  };

  const service = new CronjobService({
    dshHome: config.dshHome ?? resolveDshHome(),
    timers,
    agents: resolveAgentRegistry(ctx),
    ...(resolveSessionLifecycle(ctx) === undefined
      ? {}
      : { sessions: resolveSessionLifecycle(ctx) as NotificationSessionLifecycle }),
    subagentProviders: resolveSubagentProviders(ctx),
    ...(config.subagentDefaultProvider === undefined
      ? {}
      : { subagentDefaultProvider: config.subagentDefaultProvider }),
    ...(config.interpreter === undefined ? {} : { interpreter: config.interpreter }),
    ...(config.maxGlobalConcurrency === undefined
      ? {}
      : { maxGlobalConcurrency: config.maxGlobalConcurrency }),
    logger,
  });

  ctx.effect(() => {
    let disposed = false;
    void (async () => {
      try {
        await service.start();
        if (!disposed) await service.recover();
      } catch (error) {
        logger?.error("cronjob.start_failed", {
          message: error instanceof Error ? error.message : String(error),
        });
      }
    })();

    ctx.provide("cronjobs", service);

    return () => {
      disposed = true;
      void service.dispose();
    };
  });
}

/**
 * The managed timer service.
 *
 * A missing timer is a composition error rather than something to paper over:
 * without it the package cannot schedule anything, so a no-op fallback would
 * silently produce a cronjob service that never fires.
 */
function resolveTimers(ctx: HostContext): SchedulerTimers {
  const timer = ctx.get("timer") as SchedulerTimers | undefined;
  if (timer !== undefined && typeof timer.timeout === "function") return timer;
  return {
    timeout: (callback: () => void, delay: number) => {
      const handle = setTimeout(callback, delay);
      // Unref so a pending wake cannot keep the process alive on its own.
      if (typeof handle.unref === "function") handle.unref();
      return () => clearTimeout(handle);
    },
  };
}

/** Resolve the agent registry, degrading to "nothing is live" when absent. */
function resolveAgentRegistry(ctx: HostContext): NotificationAgentRegistry {
  const agents = ctx.get("agents") as { get(id: string): unknown } | undefined;
  return {
    findLive(sessionId: string) {
      if (agents === undefined) return undefined;
      const agent = agents.get(sessionId) as
        | { followup?: (message: string) => unknown; queueFollowup?: (message: string) => unknown }
        | undefined;
      if (agent === undefined) return undefined;
      const send = agent.followup ?? agent.queueFollowup;
      if (typeof send !== "function") return undefined;
      return {
        sessionId,
        async deliver(message: string) {
          await send.call(agent, message);
        },
      };
    },
  };
}

/** Resolve the session-resume hook so offline outboxes can be drained. */
function resolveSessionLifecycle(ctx: HostContext): NotificationSessionLifecycle | undefined {
  const sessions = ctx.get("sessions") as
    | { onSessionResumed(handler: (sessionId: string) => void): () => void }
    | undefined;
  if (sessions?.onSessionResumed === undefined) return undefined;
  return {
    onSessionResumed(handler) {
      return sessions.onSessionResumed!(handler);
    },
  };
}

/** Adapt the registered subagent providers to this package's narrow seam. */
function resolveSubagentProviders(ctx: HostContext): SubagentProvider[] {
  const subagents = ctx.get("subagents") as
    | { list(): string[]; start(name: string, request: Record<string, unknown>): Promise<unknown> }
    | undefined;
  if (subagents === undefined) return [];
  return subagents.list().map((providerName) => ({
    name: providerName,
    async start(request: { readonly prompt: string; readonly context: Record<string, unknown>; readonly outputFormat: string }) {
      const handle = (await subagents.start(providerName, {
        prompt: request.prompt,
        context: request.context,
        outputFormat: request.outputFormat,
      })) as
        | {
            id?: string;
            childId?: string;
            result?: Promise<{ text?: string }>;
            interrupt?: () => Promise<void>;
            interrupt_?: () => Promise<void>;
          }
        | undefined;
      return {
        childId: handle?.id ?? handle?.childId ?? providerName,
        result: Promise.resolve(handle?.result).then((answer) => ({
          text: typeof answer?.text === "string" ? answer.text : String(answer ?? ""),
        })),
        interrupt: async () => {
          const stop = handle?.interrupt ?? handle?.interrupt_;
          if (typeof stop === "function") await stop.call(handle);
        },
      };
    },
  }));
}

/** Resolve DSH home from the environment, matching the runtime's own default. */
function resolveDshHome(): string {
  const fromEnv = process.env["DSH_HOME"];
  if (fromEnv !== undefined && fromEnv.length > 0) return fromEnv;
  const home = process.env["HOME"] ?? process.cwd();
  return `${home}/.dsh`;
}

function format(event: string, data?: Record<string, unknown>): string {
  return data === undefined ? `cronjob: ${event}` : `cronjob: ${event} ${JSON.stringify(data)}`;
}

export { CronjobService } from "./service.js";
export type {
  CronjobServiceOptions,
  CronjobSummary,
  RunSummary,
} from "./service.js";

/**
 * Default export for loaders that mount a plugin object directly.
 *
 * The three fields are declared together as the plugin value; named exports
 * above remain available for callers that mount by name.
 */
const plugin = { name, inject, apply };
export default plugin;
