# dsh-cronjob

Host-level, persistent cronjob service for [DeepSeek Harness](https://github.com/deepseek-ai/deepseek-harness).

`@deepseek-ai/dsh-cronjob` schedules recurring work inside the DSH Host process. Jobs,
scripts, run artifacts, logs, locks and notifications all live under `$DSH_HOME/cronjobs/`,
so nothing is bound to a particular workspace, and no system `crontab` or detached daemon is
ever used.

## Scope

- Standard five-field Cron (`minute hour day month weekday`) with an explicit IANA `timeZone`.
- A job's workflow is a sequence of nodes; only `pythonScript` and `subagent` are supported.
- Scheduling is driven by managed Host timers, not by system cron.
- Notifications go to DSH Sessions only — no IM, email or other channels.
- `misfirePolicy` supports `skip` only; backlogged runs are not replayed.
- Every scheduled fire and every `run_now` re-reads and re-validates the current YAML; a
  validation failure rejects the run before any model call or subprocess.

## Architecture

```
Agent preset tools ──▶ cronjobs Host service
                            │
      ┌─────────────┬───────┴────────┬──────────────┐
      ▼             ▼                ▼              ▼
  Validation     Storage        Scheduler     Notifications
                                   │
                                   ▼
                           Run Orchestrator
                  ┌────────┬───────┴───────┬────────┐
                  ▼        ▼               ▼        ▼
              Validation  Storage        Logger   Executors
                                              (python / subagent)
```

Every module sits on the shared contracts in `src/cronjob/contracts.ts` and passes only
serializable data across a seam — never a Cordis service, Agent or Session instance.

## Modules

| Module | Source | Purpose |
| --- | --- | --- |
| Contracts | `src/cronjob/contracts.ts` | YAML, run status, log, error and tool contracts |
| Storage | `src/cronjob/storage.ts` | DSH Home layout, atomic writes, locking, retention |
| Validation | `src/cronjob/validation.ts` | YAML syntax and semantic validation before every run |
| Scheduler | `src/cronjob/scheduler.ts` | Five-field cron, time zones, next fire time, timer lifecycle |
| Orchestrator | `src/cronjob/orchestrator.ts` | Single-flight, global concurrency, cancellation, state machine, recovery |
| Executors | `src/cronjob/executors/{python,subagent}.ts` | Python and subagent node executors |
| Logging | `src/cronjob/logging.ts` | Unified INFO/ERROR JSON Lines log per run |
| Notifications | `src/cronjob/notifications.ts` | Session-bound online/offline notification outbox |
| Service | `src/cronjob/service.ts` | Host `cronjobs` service assembling the modules |
| Tools | `src/cronjob/tools.ts` | Agent-facing tool definitions |
| Entry | `src/cronjob/index.ts` | Cordis plugin object publishing the service |

## Cross-cutting invariants

- Every side effect is owned by a Cordis fiber or an explicit disposer.
- Untrusted YAML, script output and subagent output are handled as data only, never as
  commands or privileged instructions.
- Python runs through an argv array, never shell interpolation; a script's realpath must stay
  inside the controlled artifact root.
- A run allocates its run ID, status directory and log before any node executes.
- Every failure produces status plus an ERROR log; a logging failure never blocks timer, lock
  or subprocess cleanup.
- After delete/disable/stop no timer, subprocess, queue entry or unreleased lock remains.

## Composition

The plugin is mounted as two rows on different planes:

- **Host plane** — `@deepseek-ai/dsh-cronjob` publishes the cross-session `cronjobs` service.
  It must not sit in an agent preset: a second session mounting that preset would collide on
  the service name.
- **Agent preset** — the `cronjob_*` tools are what a preset adds to give one session the
  cronjob surface.

## Development

```bash
npm install
npm run build      # tsc -p tsconfig.build.json
npm test           # vitest run
npm run typecheck  # tsc --noEmit
```

## Status

This is a fresh implementation. The task table, scheduler, orchestrator, executors, logging
and notification outbox are implemented and unit-tested; the tool *registration* against the
DSH `tools` registry and a live end-to-end mount are **not yet wired**, so `src/cronjob/tools.ts`
currently defines the handlers and their contracts without registering them.

## License

MIT
