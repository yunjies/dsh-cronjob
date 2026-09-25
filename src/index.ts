/**
 * Package entry: re-export the domain and the composition entry.
 *
 * Kept separate from `cronjob/index.ts` so a consumer can import the frozen
 * contracts without pulling in the Cordis plugin object.
 *
 * @module @deepseek-ai/dsh-cronjob
 */

export * from "./cronjob/contracts.js";
export * from "./cronjob/storage.js";
export * from "./cronjob/validation.js";
export * from "./cronjob/scheduler.js";
export * from "./cronjob/logging.js";
export * from "./cronjob/notifications.js";
export * from "./cronjob/orchestrator.js";
export * from "./cronjob/service.js";
export * from "./cronjob/tools.js";
export * from "./cronjob/executors/python.js";
export * from "./cronjob/executors/subagent.js";

export { apply, inject, name } from "./cronjob/index.js";
export type { CronjobPluginConfig } from "./cronjob/index.js";
export { default } from "./cronjob/index.js";
