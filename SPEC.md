# Disputatio specification

## Scope

Disputatio is a local, single-user web application that runs a configurable forum of CLI-backed agents, records the deliberation, and can coordinate coding work in isolated Git worktrees. It binds to loopback by default and offers JSON and Markdown exports.

## Configuration and budgets

- A run configures `N` agents, each with a provider, model, and role. The defaults are `N = 3` and `P = 3` concurrent jobs.
- At most `P` jobs run concurrently. Each agent has at most one active job.
- Version one executes one discussion at a time, so its selected `P` is the process-wide cap. Other discussions remain stored; an active discussion must finish or drain its in-flight calls before another starts.
- Agents receive four discussion attempts by default; one is reserved for the closing turn. The designated integrator receives one additional attempt.
- Every started external call consumes an attempt, including a failed call. Calls are bounded by the run deadline.
- A configured existing Git repository may be the target for coding work.

## Forum protocol

1. Each agent begins with one independent seed turn. No forum turn starts until every seed result has completed or failed.
2. The asynchronous forum phase follows. Inputs for each job are frozen exactly when the job is created.
3. Notifications are coalesced while preserving the immutable visible transcript. A closing turn sees the final regular transcript.
4. Every completed turn records its position, self-reported confidence from 0 through 100, phase, index, time, and the IDs of its actual inputs. Missing or invalid confidence is stored as `NULL`; confidence is a self-assessment and is never a weighting signal.
5. Passes and failures remain explicit transcript events. Raw CLI output is available alongside the visible transcript.
6. The stored input includes the exact prompt sent to the provider: user question, agent role, phase instructions, relevant diary state, and the selected transcript. A pass or an agent's own post does not wake that same agent for another regular turn.

## Providers

- Codex and Claude are argv-based CLI adapters with explicit provider sessions.
- Adapters never use `--last` and never automatically replay an uncertain call.
- Provider and worktree builders publish independent dict/dataclass APIs.

## Persistence and recovery

- SQLite persists run configuration, jobs, turns, position data, raw outputs, and exports.
- Queued work resumes. A recovered running job is marked interrupted and requires an explicit retry, which consumes budget.
- Publication is idempotent.
- The core `Engine` and `Store` publish plain-dict APIs. The root coordinator wires the independently published provider, worktree, core, and web APIs together.

## Coding workflow

- Each coding agent receives a separate branch and worktree from the same base `HEAD`.
- Checkpoint code and test artifacts are retained.
- The integrator receives a separate result worktree and produces an integration report.
- The result worktree initially contains the common base. The integrator receives the candidate branch references and chooses which changes to combine; the controller does not merge all alternatives automatically.

## Web interface

The local web UI configures agents and runs, shows live forum turns and positions, presents coding artifacts, and supports pause, resume, stop, JSON export, and Markdown export.

## Validation plan

Automated tests use 100 fake agents and cover concurrency, frozen snapshots, budgets, failures, recovery, Git worktrees, integration, and web behavior. Regular tests never call real model CLIs. An optional smoke suite may use no more than three real calls. Evaluating a single agent thinking budget is deferred.

## References

- Research paper: <https://arxiv.org/abs/2609.08016>
- Related source: <https://github.com/chenmoneygithub/llm-committee>
