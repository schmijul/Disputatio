# Disputatio

Disputatio is a local, single-user forum for structured deliberation among configurable CLI-backed agents. It stores the discussion, positions, call metadata, and coding artifacts so a run can be inspected and resumed.

## Requirements

- Python 3.11 or later
- [uv](https://docs.astral.sh/uv/)
- An existing authenticated installation of each CLI provider you configure (for example, Codex or Claude)

The fake provider is for tests and demos only. Disputatio makes no quality claim about agent output.

## Launch

```bash
uv sync --extra dev
uv run disputatio
```

Open <http://127.0.0.1:8765>. The server binds to loopback by default.

See [SPEC.md](SPEC.md) for the approved runtime behavior and persistence model.
