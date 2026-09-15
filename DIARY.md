# Development diary

## 2026-09-15 — Initial scaffold

- Read the approved Disputatio brief and earlier read-only research references.
- Recorded the approved plan: a persisted local multi-agent forum with CLI adapters, bounded discussion budgets, recovery, isolated coding worktrees, and a loopback web UI.
- Initialized the local `main` branch and configured the approved empty public origin; no commit or push was made.
- Began implementation with package metadata and project documentation only. No real 100-agent benchmark was run; the specification calls for fake agents in regular automated tests.
- Validated the project metadata with Python's TOML parser, compiled the two package entry files, and checked the Git diff for whitespace errors.

## 2026-09-15 — Implementation in progress

- Installed the development dependencies with `uv sync --extra dev` and committed the lockfile.
- Independent scaffold review approved the package. Pushed commit `fa17868` to `main`.
- Switched implementation and review work to GPT-5.6 Luna at the user's request. Usage resets are explicitly prohibited.
- The provider, Git-worktree, and core modules are initial drafts. Python compilation passes; their behavioral tests and integration review are still pending.
- Early review found that the first core draft used regular-round barriers and did not yet persist provider sessions or wire per-agent worktrees. These issues were returned to its owner before completion.
- Publishing draft work on `feat/agent-forum` for progress visibility; this branch is not yet a runnable release.
- Backend usage-limit responses interrupted both agents. Continued only with the same requested model; no usage-reset credit was used.

## 2026-09-15 — Scheduler smoke checks

- A fake-provider run with three agents and four allowed attempts produced three seed calls, three passing regular turns, and three closing calls: nine calls total. Seed prompts contained the user question and agent role.
- Eight fake agents configured with seven concurrent slots reached seven active calls, confirming that concurrency is not capped at the default of three.
- Pausing a three-agent, one-slot run during its first seed call allowed that call to finish and dispatched no further calls. Resuming completed the remaining seeds and closing turns with six total calls and no duplicate first seed.
- These are targeted development checks using temporary SQLite databases. Full automated regression tests and the independent package review remain in progress.
