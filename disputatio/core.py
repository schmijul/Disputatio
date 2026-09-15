"""Persistence and asynchronous forum orchestration.

The core deliberately knows very little about a provider or a worktree.  Both
are small, independently replaceable objects exposing async methods and plain
dictionaries.  This makes the scheduler useful with fake providers in tests as
well as the CLI adapters used by the application.
"""

from __future__ import annotations

import asyncio
import copy
import inspect
import json
import os
import sqlite3
import threading
import tempfile
import uuid
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable, Iterator, Mapping


def _id(prefix: str) -> str:
    return f"{prefix}_{uuid.uuid4().hex}"


def _time() -> str:
    return datetime.now(timezone.utc).isoformat()


def _json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, ensure_ascii=False, default=str)


def _loads(value: str | None, fallback: Any = None) -> Any:
    if value is None:
        return fallback
    try:
        return json.loads(value)
    except (TypeError, ValueError):
        return fallback


class Store:
    """SQLite persistence with a small, stable dict API.

    A single connection is intentionally retained.  It supports ``:memory:``
    stores (especially useful for tests) and is protected by a lock so
    provider completion tasks cannot interleave writes.
    """

    def __init__(self, path: str | os.PathLike[str] = "disputatio.sqlite3") -> None:
        self.path = str(path)
        if self.path != ":memory:":
            Path(self.path).expanduser().parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.RLock()
        self._db = sqlite3.connect(self.path, check_same_thread=False)
        self._db.row_factory = sqlite3.Row
        self._db.execute("PRAGMA foreign_keys = ON")
        self._db.execute("PRAGMA journal_mode = WAL")
        self._init_schema()

    def _init_schema(self) -> None:
        with self._lock:
            self._db.executescript(
                """
                CREATE TABLE IF NOT EXISTS configs (
                    id TEXT PRIMARY KEY, payload TEXT NOT NULL, created_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS runs (
                    id TEXT PRIMARY KEY, config_id TEXT NOT NULL, config TEXT NOT NULL,
                    status TEXT NOT NULL, created_at TEXT NOT NULL, updated_at TEXT NOT NULL,
                    deadline TEXT, pause_reason TEXT, stop_reason TEXT,
                    designated_integrator TEXT, metadata TEXT NOT NULL DEFAULT '{}',
                    FOREIGN KEY(config_id) REFERENCES configs(id)
                );
                CREATE TABLE IF NOT EXISTS jobs (
                    id TEXT PRIMARY KEY, run_id TEXT NOT NULL, agent_id TEXT NOT NULL,
                    phase TEXT NOT NULL, idx INTEGER NOT NULL, status TEXT NOT NULL,
                    inputs TEXT NOT NULL, input_ids TEXT NOT NULL, payload TEXT NOT NULL,
                    result TEXT, raw_output TEXT, error TEXT, session_id TEXT,
                    workspace TEXT, artifacts TEXT, started_at TEXT, completed_at TEXT,
                    created_at TEXT NOT NULL, FOREIGN KEY(run_id) REFERENCES runs(id)
                );
                CREATE INDEX IF NOT EXISTS jobs_run_idx ON jobs(run_id, idx, created_at);
                CREATE TABLE IF NOT EXISTS turns (
                    id TEXT PRIMARY KEY, run_id TEXT NOT NULL, job_id TEXT NOT NULL,
                    agent_id TEXT NOT NULL, phase TEXT NOT NULL, idx INTEGER NOT NULL,
                    status TEXT NOT NULL, position TEXT, confidence INTEGER,
                    input_ids TEXT NOT NULL, at TEXT NOT NULL, raw_output TEXT,
                    FOREIGN KEY(run_id) REFERENCES runs(id), FOREIGN KEY(job_id) REFERENCES jobs(id)
                );
                CREATE TABLE IF NOT EXISTS events (
                    id TEXT PRIMARY KEY, run_id TEXT NOT NULL, kind TEXT NOT NULL,
                    agent_id TEXT, job_id TEXT, text TEXT, payload TEXT NOT NULL, at TEXT NOT NULL,
                    FOREIGN KEY(run_id) REFERENCES runs(id)
                );
                CREATE TABLE IF NOT EXISTS publications (
                    job_id TEXT PRIMARY KEY, run_id TEXT NOT NULL, payload TEXT NOT NULL,
                    published_at TEXT NOT NULL, FOREIGN KEY(run_id) REFERENCES runs(id)
                );
                CREATE TABLE IF NOT EXISTS exports (
                    id TEXT PRIMARY KEY, run_id TEXT NOT NULL, format TEXT NOT NULL,
                    content TEXT NOT NULL, created_at TEXT NOT NULL,
                    UNIQUE(run_id, format), FOREIGN KEY(run_id) REFERENCES runs(id)
                );
                """
            )
            self._db.commit()

    @contextmanager
    def transaction(self) -> Iterator[sqlite3.Connection]:
        with self._lock:
            try:
                yield self._db
                self._db.commit()
            except Exception:
                self._db.rollback()
                raise

    def close(self) -> None:
        with self._lock:
            self._db.close()

    def _one(self, sql: str, args: tuple[Any, ...] = ()) -> sqlite3.Row | None:
        with self._lock:
            return self._db.execute(sql, args).fetchone()

    def _all(self, sql: str, args: tuple[Any, ...] = ()) -> list[sqlite3.Row]:
        with self._lock:
            return self._db.execute(sql, args).fetchall()

    @staticmethod
    def _config(row: sqlite3.Row) -> dict[str, Any]:
        value = _loads(row["payload"], {})
        value.setdefault("id", row["id"])
        value.setdefault("created_at", row["created_at"])
        return value

    def create_config(self, config: Mapping[str, Any]) -> dict[str, Any]:
        value = copy.deepcopy(dict(config))
        agents = value.get("agents", [])
        if not isinstance(agents, list) or not agents:
            raise ValueError("config.agents must be a non-empty list")
        ids: list[str] = []
        normalized: list[dict[str, Any]] = []
        for number, agent in enumerate(agents):
            if not isinstance(agent, Mapping):
                raise ValueError("each agent must be an object")
            item = dict(agent)
            item.setdefault("id", f"agent-{number + 1}")
            item["id"] = str(item["id"])
            if item["id"] in ids:
                raise ValueError(f"duplicate agent id: {item['id']}")
            ids.append(item["id"])
            normalized.append(item)
        value["agents"] = normalized
        value.setdefault("max_parallel", min(3, len(normalized)))
        value.setdefault("attempts", value.get("discussion_attempts", 4))
        value.setdefault("discussion_attempts", value["attempts"])
        value.setdefault("integrator", next((a["id"] for a in normalized if a.get("role") == "integrator"), normalized[0]["id"]))
        config_id = str(value.get("id") or _id("cfg"))
        value["id"] = config_id
        value.setdefault("created_at", _time())
        with self.transaction() as db:
            db.execute("INSERT INTO configs(id,payload,created_at) VALUES(?,?,?)", (config_id, _json(value), value["created_at"]))
        return copy.deepcopy(value)

    def get_config(self, config_id: str) -> dict[str, Any] | None:
        row = self._one("SELECT * FROM configs WHERE id=?", (str(config_id),))
        return self._config(row) if row else None

    def list_configs(self) -> list[dict[str, Any]]:
        return [self._config(r) for r in self._all("SELECT * FROM configs ORDER BY created_at, id")]

    def create_run(self, config: Mapping[str, Any] | str) -> dict[str, Any]:
        if isinstance(config, str):
            value = self.get_config(config)
            if value is None:
                raise KeyError(f"unknown config: {config}")
        else:
            value = dict(config)
            if not value.get("id") or self.get_config(str(value["id"])) is None:
                value = self.create_config(value)
            else:
                value = self.get_config(str(value["id"])) or value
        run_id = _id("run")
        created = _time()
        run = {
            "id": run_id, "run_id": run_id, "config_id": value["id"], "config": copy.deepcopy(value),
            "status": "created", "created_at": created, "updated_at": created,
            "deadline": value.get("deadline"), "pause_reason": None, "stop_reason": None,
            "integrator": value.get("integrator"), "designated_integrator": value.get("integrator"),
            "metadata": {},
        }
        with self.transaction() as db:
            db.execute(
                "INSERT INTO runs(id,config_id,config,status,created_at,updated_at,deadline,pause_reason,stop_reason,designated_integrator,metadata) VALUES(?,?,?,?,?,?,?,?,?,?,?)",
                (run_id, value["id"], _json(value), "created", created, created, run["deadline"], None, None, run["integrator"], "{}"),
            )
        return run

    @staticmethod
    def _run(row: sqlite3.Row) -> dict[str, Any]:
        config = _loads(row["config"], {})
        return {
            "id": row["id"], "run_id": row["id"], "config_id": row["config_id"], "config": config,
            "status": row["status"], "created_at": row["created_at"], "updated_at": row["updated_at"],
            "deadline": row["deadline"], "pause_reason": row["pause_reason"], "stop_reason": row["stop_reason"],
            "integrator": row["designated_integrator"], "designated_integrator": row["designated_integrator"],
            "metadata": _loads(row["metadata"], {}),
        }

    def get_run(self, run_id: str, *, expanded: bool = True) -> dict[str, Any] | None:
        row = self._one("SELECT * FROM runs WHERE id=?", (str(run_id),))
        if not row:
            return None
        run = self._run(row)
        if expanded:
            run["jobs"] = self.list_jobs(run_id)
            run["turns"] = self.list_turns(run_id)
            run["events"] = self.list_events(run_id)
        return run

    def list_runs(self) -> list[dict[str, Any]]:
        return [self.get_run(r["id"], expanded=False) for r in self._all("SELECT * FROM runs ORDER BY created_at, id")]

    def update_run(self, run_id: str, **fields: Any) -> None:
        fields["updated_at"] = fields.get("updated_at") or _time()
        allowed = {"status", "updated_at", "deadline", "pause_reason", "stop_reason", "designated_integrator", "metadata"}
        fields = {k: v for k, v in fields.items() if k in allowed}
        if "metadata" in fields:
            fields["metadata"] = _json(fields["metadata"])
        if not fields:
            return
        sql = "UPDATE runs SET " + ", ".join(f"{k}=?" for k in fields) + " WHERE id=?"
        with self.transaction() as db:
            db.execute(sql, tuple(fields.values()) + (str(run_id),))

    def create_job(self, job: Mapping[str, Any]) -> dict[str, Any]:
        value = copy.deepcopy(dict(job))
        value.setdefault("id", value.get("job_id") or _id("job"))
        value["job_id"] = value["id"]
        value.setdefault("status", "queued")
        value.setdefault("phase", "regular")
        value.setdefault("index", value.get("idx", 0))
        value.setdefault("inputs", [])
        value.setdefault("input_ids", [x.get("id") for x in value["inputs"] if isinstance(x, Mapping) and x.get("id")])
        value.setdefault("payload", {})
        value.setdefault("input_snapshot", {"input_ids": value["input_ids"], "context": value["inputs"]})
        value.setdefault("prompt", _json(value["input_snapshot"]))
        value["payload"] = {**dict(value.get("payload") or {}), "input_snapshot": copy.deepcopy(value["input_snapshot"]), "prompt": value["prompt"]}
        value.setdefault("created_at", _time())
        with self.transaction() as db:
            db.execute(
                "INSERT INTO jobs(id,run_id,agent_id,phase,idx,status,inputs,input_ids,payload,result,raw_output,error,session_id,workspace,artifacts,started_at,completed_at,created_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (value["id"], value["run_id"], str(value["agent_id"]), value["phase"], int(value["index"]), value["status"], _json(value["inputs"]), _json(value["input_ids"]), _json(value.get("payload", {})), None, None, None, value.get("session_id"), value.get("workspace"), _json(value.get("artifacts", [])), value.get("started_at"), value.get("completed_at"), value["created_at"]),
            )
        return value

    @staticmethod
    def _job(row: sqlite3.Row) -> dict[str, Any]:
        result = _loads(row["result"], None)
        payload = _loads(row["payload"], {})
        value = {
            "id": row["id"], "job_id": row["id"], "run_id": row["run_id"], "agent_id": row["agent_id"],
            "phase": row["phase"], "index": row["idx"], "idx": row["idx"], "status": row["status"],
            "inputs": _loads(row["inputs"], []), "input_ids": _loads(row["input_ids"], []), "payload": payload,
            "result": result, "raw_output": row["raw_output"], "error": row["error"], "session_id": row["session_id"],
            "workspace": row["workspace"], "artifacts": _loads(row["artifacts"], []), "started_at": row["started_at"],
            "completed_at": row["completed_at"], "created_at": row["created_at"],
        }
        value["input_snapshot"] = copy.deepcopy(payload.get("input_snapshot", {"input_ids": value["input_ids"], "context": value["inputs"]}))
        value["prompt"] = payload.get("prompt", _json(value["input_snapshot"]))
        value.update(payload)
        return value

    def get_job(self, job_id: str) -> dict[str, Any] | None:
        row = self._one("SELECT * FROM jobs WHERE id=?", (str(job_id),))
        return self._job(row) if row else None

    def list_jobs(self, run_id: str, *, statuses: tuple[str, ...] | None = None) -> list[dict[str, Any]]:
        if statuses:
            marks = ",".join("?" for _ in statuses)
            rows = self._all(f"SELECT * FROM jobs WHERE run_id=? AND status IN ({marks}) ORDER BY created_at,id", (str(run_id), *statuses))
        else:
            rows = self._all("SELECT * FROM jobs WHERE run_id=? ORDER BY created_at,id", (str(run_id),))
        return [self._job(r) for r in rows]

    def update_job(self, job_id: str, **fields: Any) -> None:
        fields = dict(fields)
        mapping = {"index": "idx"}
        fields = {mapping.get(k, k): v for k, v in fields.items()}
        allowed = {"status", "result", "raw_output", "error", "session_id", "workspace", "artifacts", "started_at", "completed_at", "payload"}
        fields = {k: (_json(v) if k in {"result", "artifacts", "payload"} else v) for k, v in fields.items() if k in allowed}
        if not fields:
            return
        with self.transaction() as db:
            db.execute("UPDATE jobs SET " + ", ".join(f"{k}=?" for k in fields) + " WHERE id=?", tuple(fields.values()) + (str(job_id),))

    def create_turn(self, turn: Mapping[str, Any]) -> dict[str, Any]:
        value = dict(turn)
        value.setdefault("id", _id("turn")); value.setdefault("at", _time()); value.setdefault("status", "completed")
        confidence = value.get("confidence")
        if not isinstance(confidence, int) or isinstance(confidence, bool) or not 0 <= confidence <= 100:
            confidence = None
        value["confidence"] = confidence
        value.setdefault("input_ids", [])
        with self.transaction() as db:
            db.execute("INSERT INTO turns(id,run_id,job_id,agent_id,phase,idx,status,position,confidence,input_ids,at,raw_output) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)", (value["id"], value["run_id"], value["job_id"], value["agent_id"], value.get("phase", "regular"), int(value.get("index", value.get("idx", 0))), value["status"], value.get("position"), confidence, _json(value["input_ids"]), value["at"], value.get("raw_output")))
        return value

    @staticmethod
    def _turn(row: sqlite3.Row) -> dict[str, Any]:
        return {"id": row["id"], "run_id": row["run_id"], "job_id": row["job_id"], "agent_id": row["agent_id"], "phase": row["phase"], "index": row["idx"], "idx": row["idx"], "status": row["status"], "position": row["position"], "confidence": row["confidence"], "input_ids": _loads(row["input_ids"], []), "at": row["at"], "raw_output": row["raw_output"]}

    def list_turns(self, run_id: str) -> list[dict[str, Any]]:
        return [self._turn(r) for r in self._all("SELECT * FROM turns WHERE run_id=? ORDER BY at,id", (str(run_id),))]

    def add_event(self, event: Mapping[str, Any]) -> dict[str, Any]:
        value = dict(event); value.setdefault("id", _id("event")); value.setdefault("at", _time()); value.setdefault("payload", {})
        with self.transaction() as db:
            db.execute("INSERT INTO events(id,run_id,kind,agent_id,job_id,text,payload,at) VALUES(?,?,?,?,?,?,?,?)", (value["id"], value["run_id"], value.get("kind", "event"), value.get("agent_id"), value.get("job_id"), value.get("text"), _json(value["payload"]), value["at"]))
        return value

    def list_events(self, run_id: str) -> list[dict[str, Any]]:
        return [{"id": r["id"], "run_id": r["run_id"], "kind": r["kind"], "agent_id": r["agent_id"], "job_id": r["job_id"], "text": r["text"], "payload": _loads(r["payload"], {}), "at": r["at"]} for r in self._all("SELECT * FROM events WHERE run_id=? ORDER BY at,id", (str(run_id),))]

    def publish(self, run_id: str, job_id: str, payload: Mapping[str, Any]) -> dict[str, Any]:
        with self.transaction() as db:
            existing = db.execute("SELECT payload FROM publications WHERE job_id=?", (str(job_id),)).fetchone()
            if existing:
                return _loads(existing["payload"], {})
            value = copy.deepcopy(dict(payload)); at = _time()
            db.execute("INSERT INTO publications(job_id,run_id,payload,published_at) VALUES(?,?,?,?)", (str(job_id), str(run_id), _json(value), at))
            return value

    def export_json(self, run_id: str) -> str:
        run = self.get_run(run_id)
        if run is None: raise KeyError(f"unknown run: {run_id}")
        text = json.dumps(run, indent=2, ensure_ascii=False, sort_keys=True, default=str)
        with self.transaction() as db:
            db.execute("INSERT INTO exports(id,run_id,format,content,created_at) VALUES(?,?,?,?,?) ON CONFLICT(run_id,format) DO UPDATE SET content=excluded.content,created_at=excluded.created_at", (_id("export"), str(run_id), "json", text, _time()))
        return text

    def export_markdown(self, run_id: str) -> str:
        run = self.get_run(run_id)
        if run is None: raise KeyError(f"unknown run: {run_id}")
        lines = [f"# Disputatio run {run['id']}", "", f"- Status: `{run['status']}`", f"- Created: {run['created_at']}", "", "## Transcript", ""]
        for event in run.get("events", []):
            who = event.get("agent_id") or "system"
            text = event.get("text") or event.get("payload", {}).get("text") or ""
            lines.extend([f"### {who} · {event['kind']}", "", str(text), ""])
        for turn in run.get("turns", []):
            lines.extend([f"### {turn['agent_id']} · {turn['phase']} {turn['index']}", "", str(turn.get("position") or "(no position)"), ""])
        text = "\n".join(lines)
        with self.transaction() as db:
            db.execute("INSERT INTO exports(id,run_id,format,content,created_at) VALUES(?,?,?,?,?) ON CONFLICT(run_id,format) DO UPDATE SET content=excluded.content,created_at=excluded.created_at", (_id("export"), str(run_id), "markdown", text, _time()))
        return text


class Engine:
    """Async bounded scheduler for a persisted forum run."""

    def __init__(self, store: Store, provider: Any, worktrees: Any = None, max_parallel: int = 3, now: Callable[[], Any] | Any = None) -> None:
        self.store, self.provider, self.worktrees = store, provider, worktrees
        self.max_parallel = max(1, int(max_parallel))
        self.now = now or _time
        self._tasks: dict[str, asyncio.Task[Any]] = {}
        self._job_tasks: dict[str, asyncio.Task[Any]] = {}
        self._cancelled: set[str] = set()
        self._sessions: dict[tuple[str, str], str] = {}
        self._prepared: dict[str, list[dict[str, Any]]] = {}
        self._lock = asyncio.Lock()

    def _now(self) -> str:
        value = self.now() if callable(self.now) else self.now
        if isinstance(value, datetime): return value.astimezone(timezone.utc).isoformat()
        return str(value)

    async def create(self, config: Mapping[str, Any]) -> dict[str, Any]:
        value = dict(config)
        if value.get("time_limit_seconds") and not value.get("deadline"):
            try:
                seconds = float(value["time_limit_seconds"])
                now = self._now()
                stamp = datetime.fromisoformat(now.replace("Z", "+00:00")) + timedelta(seconds=seconds)
                value["deadline"] = stamp.astimezone(timezone.utc).isoformat()
            except (TypeError, ValueError):
                pass
        return self.store.create_run(value)

    def get(self, run_id: str) -> dict[str, Any] | None: return self.store.get_run(run_id)
    def list(self) -> list[dict[str, Any]]: return self.store.list_runs()

    async def start(self, run_id: str) -> dict[str, Any]:
        run = self.store.get_run(run_id, expanded=False)
        if run is None: raise KeyError(f"unknown run: {run_id}")
        for other_id, other_task in self._tasks.items():
            other = self.store.get_run(other_id, expanded=False)
            if other_id != run_id and not other_task.done() and other and other["status"] in {"running", "pausing", "stopping"}:
                raise RuntimeError("another run is active; pause/stop it and wait for its calls to drain")
        if run_id not in self._tasks or self._tasks[run_id].done():
            self.store.update_run(run_id, status="running", pause_reason=None, stop_reason=None)
            self._tasks[run_id] = asyncio.create_task(self._drive(run_id), name=f"disputatio:{run_id}")
        return self.store.get_run(run_id) or run

    async def wait(self, run_id: str) -> dict[str, Any] | None:
        task = self._tasks.get(run_id)
        if task is not None: await asyncio.shield(task)
        return self.store.get_run(run_id)

    async def pause(self, run_id: str, reason: str = "paused") -> dict[str, Any] | None:
        run = self.store.get_run(run_id)
        if run is None: return None
        self.store.update_run(run_id, status="pausing", pause_reason=reason)
        return self.store.get_run(run_id)

    async def resume(self, run_id: str) -> dict[str, Any]:
        return await self.start(run_id)

    async def stop(self, run_id: str, reason: str = "stopped") -> dict[str, Any] | None:
        run = self.store.get_run(run_id)
        if run is None: return None
        self._cancelled.add(run_id)
        self.store.update_run(run_id, status="stopping", stop_reason=reason)
        for job_id, task in list(self._job_tasks.items()):
            job = self.store.get_job(job_id)
            if job and job["run_id"] == run_id and not task.done():
                await self._provider_cancel(job_id)
                if task is not asyncio.current_task():
                    task.cancel()
        task = self._tasks.get(run_id)
        if task and not task.done() and task is not asyncio.current_task():
            task.cancel()
        self.store.update_run(run_id, status="stopped", stop_reason=reason)
        return self.store.get_run(run_id)

    async def retry(self, run_id: str, job_id: str) -> str:
        original = self.store.get_job(job_id)
        if original is None or original["run_id"] != run_id: raise KeyError(f"unknown job: {job_id}")
        run = self.store.get_run(run_id)
        if run is None: raise KeyError(f"unknown run: {run_id}")
        config = run["config"]
        agent = next((a for a in config.get("agents", []) if str(a.get("id")) == original["agent_id"]), {})
        used = len([j for j in self.store.list_jobs(run_id) if j["agent_id"] == original["agent_id"]])
        limit = int(config.get("attempts", config.get("discussion_attempts", 4))) + (1 if original["agent_id"] == run.get("integrator") else 0)
        if used >= limit: raise RuntimeError("agent attempt budget exhausted")
        payload = dict(original.get("payload") or {})
        payload.update({"retry_of": job_id})
        value = self.store.create_job({"run_id": run_id, "agent_id": original["agent_id"], "phase": original["phase"], "index": original["index"], "inputs": original["inputs"], "input_ids": original["input_ids"], "input_snapshot": original.get("input_snapshot"), "prompt": original.get("prompt"), "payload": payload, "session_id": original.get("session_id"), "workspace": original.get("workspace")})
        self.store.update_run(run_id, status="running", pause_reason=None)
        self._job_tasks[value["id"]] = asyncio.create_task(self._execute(value["id"], agent, run["config"]), name=f"retry:{value['id']}")
        return value["id"]

    async def userpost(self, run_id: str, text: str, *, author: str = "user", payload: Mapping[str, Any] | None = None) -> dict[str, Any]:
        if self.store.get_run(run_id) is None: raise KeyError(f"unknown run: {run_id}")
        return self.store.add_event({"run_id": run_id, "kind": "userpost", "agent_id": author, "text": text, "payload": dict(payload or {})})

    def export_json(self, run_id: str) -> str: return self.store.export_json(run_id)
    def export_markdown(self, run_id: str) -> str: return self.store.export_markdown(run_id)

    async def recover(self) -> list[str]:
        interrupted: list[str] = []
        for run in self.store.list_runs():
            if run["status"] in {"running", "pausing", "stopping"}:
                for job in self.store.list_jobs(run["id"], statuses=("running",)):
                    self.store.update_job(job["id"], status="interrupted", completed_at=self._now(), error="process recovery interrupted this call")
                    self.store.add_event({"run_id": run["id"], "kind": "failure", "agent_id": job["agent_id"], "job_id": job["id"], "text": "interrupted during recovery", "payload": {"error": "interrupted"}})
                self.store.update_run(run["id"], status="paused", pause_reason="recovered; interrupted jobs require explicit retry")
                interrupted.append(run["id"])
        return interrupted

    async def startup(self) -> list[str]: return await self.recover()

    async def shutdown(self) -> None:
        for run_id in list(self._tasks):
            task = self._tasks[run_id]
            if not task.done(): await self.stop(run_id, "engine shutdown")
        if self._tasks:
            await asyncio.gather(*self._tasks.values(), return_exceptions=True)

    async def _provider_cancel(self, job_id: str) -> None:
        cancel = getattr(self.provider, "cancel", None)
        if cancel is not None:
            value = cancel(job_id)
            if inspect.isawaitable(value): await value

    def _transcript(self, run_id: str) -> list[dict[str, Any]]:
        run = self.store.get_run(run_id)
        if run is None: return []
        events = run.get("events", [])
        turns = run.get("turns", [])
        result: list[dict[str, Any]] = []
        for event in events:
            result.append(copy.deepcopy(event))
        for turn in turns:
            result.append(copy.deepcopy(turn))
        result.sort(key=lambda x: (x.get("at", ""), x.get("id", "")))
        return result

    async def _drive(self, run_id: str) -> None:
        try:
            run = self.store.get_run(run_id)
            if run is None: return
            config = run["config"]
            agents = list(config.get("agents", []))
            await self._prepare_agents(run, agents)
            # Seed jobs are created together, giving all agents an identical empty snapshot.
            seed_jobs = [j for j in self.store.list_jobs(run_id) if j["phase"] == "seed"]
            if not seed_jobs:
                await self._batch(run_id, config, agents, "seed", 0, [])
            elif any(j["status"] == "queued" for j in seed_jobs):
                await self._run_queued_phase(run_id, config, agents, "seed")
            if await self._should_halt(run_id):
                await self._mark_halted(run_id)
                return
            attempts = int(config.get("attempts", config.get("discussion_attempts", 4)))
            regular_rounds = max(0, attempts - 2)
            await self._regular(run_id, config, agents, regular_rounds)
            await self._checkpoint_agents(run_id, config, agents)
            if await self._should_halt(run_id):
                await self._mark_halted(run_id)
                return
            regular_transcript = self._transcript(run_id)
            closing_jobs = [j for j in self.store.list_jobs(run_id) if j["phase"] == "closing"]
            if not closing_jobs:
                await self._batch(run_id, config, agents, "closing", attempts - 1, regular_transcript)
            elif any(j["status"] == "queued" for j in closing_jobs):
                await self._run_queued_phase(run_id, config, agents, "closing")
            if await self._should_halt(run_id):
                await self._mark_halted(run_id)
                return
            if self._needs_worktrees(config):
                integrator_id = str(config.get("integrator") or agents[0].get("id"))
                integrator = next((a for a in agents if str(a.get("id")) == integrator_id), agents[0])
                await self._integration(run_id, config, integrator, self._transcript(run_id))
            final = self.store.get_run(run_id)
            failures = [j for j in self.store.list_jobs(run_id) if j["status"] in {"failed", "cancelled", "interrupted"}]
            self.store.update_run(run_id, status="partial" if failures else "complete", metadata={"failures": len(failures), "open_issues": [j.get("error") for j in failures if j.get("error")]})
        except asyncio.CancelledError:
            # stop() has already recorded the externally visible state.
            return
        except Exception as exc:
            self.store.update_run(run_id, status="partial", metadata={"error": str(exc)})

    async def _mark_halted(self, run_id: str) -> None:
        run = self.store.get_run(run_id)
        if run and run["status"] == "pausing":
            self.store.update_run(run_id, status="paused")

    def _needs_worktrees(self, config: Mapping[str, Any]) -> bool:
        return self.worktrees is not None and any(config.get(k) for k in ("target_repo", "repository", "repo", "repo_path", "source_repo"))

    async def _prepare_agents(self, run: Mapping[str, Any], agents: list[Mapping[str, Any]]) -> None:
        if not self._needs_worktrees(run["config"]):
            return
        prepared = self._prepared.setdefault(str(run["id"]), [])
        existing = {str(item.get("agent_id", item.get("id"))) for item in prepared}
        prepare = getattr(self.worktrees, "prepare", None) or getattr(self.worktrees, "asyncprepare", None)
        if prepare is None:
            return
        for agent in agents:
            if str(agent.get("id")) in existing:
                continue
            try:
                run_input = {**dict(run), **dict(run.get("config", {}))}
                item = prepare(run_input, dict(agent))
                if inspect.isawaitable(item): item = await item
                if item:
                    prepared.append(dict(item))
            except Exception as exc:
                self.store.add_event({"run_id": run["id"], "kind": "failure", "agent_id": agent.get("id"), "text": str(exc), "payload": {"phase": "worktree_prepare", "error": str(exc)}})

    async def _checkpoint_agents(self, run_id: str, config: Mapping[str, Any], agents: list[Mapping[str, Any]]) -> None:
        if not self._needs_worktrees(config):
            return
        checkpoint = getattr(self.worktrees, "checkpoint", None)
        if checkpoint is None:
            return
        for item in self._prepared.get(str(run_id), []):
            try:
                value = checkpoint(item, testreport=item.get("testreport"), message=f"Disputatio checkpoint for {item.get('agent_id', 'agent')}")
                if inspect.isawaitable(value): value = await value
                if value:
                    item.update(dict(value))
            except Exception as exc:
                self.store.add_event({"run_id": run_id, "kind": "failure", "agent_id": item.get("agent_id"), "text": str(exc), "payload": {"phase": "checkpoint", "error": str(exc)}})

    async def _regular(self, run_id: str, config: Mapping[str, Any], agents: list[Mapping[str, Any]], rounds: int) -> None:
        """Run regular turns as independent per-agent streams.

        A completed fast agent is immediately eligible for its next turn while
        slower agents' current calls continue.  The transcript is immutable per
        job at creation time; notifications are therefore naturally coalesced.
        """
        if rounds <= 0:
            return
        # The run's P is the process-global discussion cap.  The constructor
        # value is only the default for callers that omit P.
        limit = int(config.get("max_parallel", self.max_parallel))
        semaphore = asyncio.Semaphore(max(1, limit))
        active: dict[asyncio.Task[Any], tuple[str, dict[str, Any]]] = {}

        async def launch(agent: Mapping[str, Any], job: Mapping[str, Any]) -> None:
            async with semaphore:
                if await self._should_halt(run_id):
                    return
                await self._execute(job["id"], agent, config)

        async def add(agent: Mapping[str, Any], job: dict[str, Any]) -> None:
            task = asyncio.create_task(launch(agent, job), name=f"regular:{run_id}:{agent.get('id')}:{job['index']}")
            active[task] = (str(agent.get("id")), job)
            self._job_tasks[job["id"]] = task

        by_id = {str(a.get("id")): a for a in agents}
        def jobs_for(agent_id: str) -> list[dict[str, Any]]:
            return sorted([j for j in self.store.list_jobs(run_id) if j["phase"] == "regular" and j["agent_id"] == agent_id], key=lambda j: (j["index"], j["created_at"], j["id"]))
        for agent in agents:
            existing = jobs_for(str(agent.get("id")))
            queued = next((j for j in existing if j["status"] == "queued"), None)
            if queued is not None:
                await add(agent, queued)
            elif not existing:
                await add(agent, self._make_job(run_id, config, agent, "regular", 1, self._transcript(run_id)))
        while active:
            done, _ = await asyncio.wait(active, return_when=asyncio.FIRST_COMPLETED)
            for task in done:
                agent_id, finished = active.pop(task)
                try:
                    task.result()
                except (asyncio.CancelledError, Exception):
                    pass
                self._job_tasks.pop(finished["id"], None)
            if await self._should_halt(run_id):
                continue
            active_agents = {value[0] for value in active.values()}
            for agent in agents:
                agent_id = str(agent.get("id"))
                if agent_id in active_agents:
                    continue
                existing = jobs_for(agent_id)
                latest = existing[-1] if existing else None
                if latest is None or latest["status"] not in {"completed", "failed"} or latest["index"] >= rounds:
                    continue
                if self._has_foreign_public_event(latest.get("input_ids", []), agent_id, run_id):
                    await add(by_id[agent_id], self._make_job(run_id, config, agent, "regular", latest["index"] + 1, self._transcript(run_id)))

    def _has_foreign_public_event(self, input_ids: list[str], agent_id: str, run_id: str) -> bool:
        seen = set(input_ids)
        return any(event["id"] not in seen and event.get("kind") in {"turn", "userpost"} and event.get("agent_id") != agent_id for event in self.store.list_events(run_id))

    def _make_job(self, run_id: str, config: Mapping[str, Any], agent: Mapping[str, Any], phase: str, index: int, transcript: list[dict[str, Any]]) -> dict[str, Any]:
        context = copy.deepcopy(transcript)
        input_ids = [x.get("id") for x in context if isinstance(x, Mapping) and x.get("id")]
        schema = {"post": "string", "position": "string|null", "confidence": "integer 0..100|null", "diary": "string|null", "replyrefs": "array", "evidence": "array", "testreport": "object|string|null"}
        instructions = {
            "task": config.get("prompt", "Deliberate on the user's question."),
            "role": agent.get("role", "participant"),
            "phase": phase,
            "turn_index": index,
            "visible_transcript": context,
            "response_schema": schema,
        }
        if phase == "closing":
            instructions["task"] = "Give a final position based on the frozen regular transcript; report confidence and unresolved issues."
        if phase == "integration":
            instructions["task"] = "Choose the best code changes from the agent branches, merge or cherry-pick them, resolve conflicts, run tests, and report the result."
        prompt = _json(instructions)
        snapshot = {"run_id": run_id, "agent_id": str(agent.get("id")), "phase": phase, "index": index, "input_ids": copy.deepcopy(input_ids), "context": context, "prompt": prompt}
        session = self._sessions.get((str(run_id), str(agent.get("id"))))
        prepared = next((p for p in self._prepared.get(str(run_id), []) if str(p.get("agent_id")) == str(agent.get("id"))), {})
        return self.store.create_job({"run_id": run_id, "agent_id": str(agent.get("id")), "phase": phase, "index": index, "inputs": context, "input_ids": input_ids, "input_snapshot": snapshot, "prompt": prompt, "payload": {"context": context, "config": copy.deepcopy(dict(config)), "agent": copy.deepcopy(dict(agent))}, "session_id": session, "workspace": prepared.get("workspace"), "artifacts": prepared.get("artifacts", [])})

    async def _should_halt(self, run_id: str) -> bool:
        run = self.store.get_run(run_id, expanded=False)
        if run is None or run_id in self._cancelled: return True
        if run["status"] in {"stopping", "stopped", "paused", "pausing"}: return True
        deadline = run.get("deadline")
        if deadline and self._now() >= str(deadline):
            await self.stop(run_id, "deadline exceeded")
            return True
        return False

    def _remaining_deadline(self, run_id: str) -> float | None:
        run = self.store.get_run(run_id, expanded=False)
        deadline = run.get("deadline") if run else None
        if not deadline:
            return None
        try:
            end = datetime.fromisoformat(str(deadline).replace("Z", "+00:00"))
            current = datetime.fromisoformat(self._now().replace("Z", "+00:00"))
            return max(0.0, (end - current).total_seconds())
        except (TypeError, ValueError):
            return None

    async def _batch(self, run_id: str, config: Mapping[str, Any], agents: list[Mapping[str, Any]], phase: str, index: int, transcript: list[dict[str, Any]]) -> None:
        jobs: list[dict[str, Any]] = []
        for agent in agents:
            jobs.append(self._make_job(run_id, config, agent, phase, index, transcript))
        limit = int(config.get("max_parallel", self.max_parallel))
        semaphore = asyncio.Semaphore(max(1, limit))
        async def one(job: dict[str, Any]) -> None:
            async with semaphore:
                if await self._should_halt(run_id):
                    return
                await self._execute(job["id"], next(a for a in agents if str(a.get("id")) == job["agent_id"]), config)
        tasks = [asyncio.create_task(one(j), name=f"job:{j['id']}") for j in jobs]
        self._job_tasks.update({j["id"]: task for j, task in zip(jobs, tasks)})
        await asyncio.gather(*tasks, return_exceptions=True)
        for job in jobs:
            self._job_tasks.pop(job["id"], None)

    async def _run_queued_phase(self, run_id: str, config: Mapping[str, Any], agents: list[Mapping[str, Any]], phase: str) -> None:
        queued = [j for j in self.store.list_jobs(run_id) if j["phase"] == phase and j["status"] == "queued"]
        if not queued:
            return
        limit = int(config.get("max_parallel", self.max_parallel))
        semaphore = asyncio.Semaphore(max(1, limit))
        by_id = {str(a.get("id")): a for a in agents}
        async def one(job: dict[str, Any]) -> None:
            async with semaphore:
                if await self._should_halt(run_id):
                    return
                await self._execute(job["id"], by_id.get(job["agent_id"], {"id": job["agent_id"]}), config)
        tasks = [asyncio.create_task(one(j), name=f"queued:{j['id']}") for j in queued]
        self._job_tasks.update({j["id"]: task for j, task in zip(queued, tasks)})
        await asyncio.gather(*tasks, return_exceptions=True)
        for j in queued:
            self._job_tasks.pop(j["id"], None)

    async def _execute(self, job_id: str, agent: Mapping[str, Any], config: Mapping[str, Any]) -> None:
        job = self.store.get_job(job_id)
        if job is None: return
        self.store.update_job(job_id, status="running", started_at=self._now())
        try:
            request = copy.deepcopy(job)
            request.update({"context": copy.deepcopy(job["inputs"]), "input_snapshot": copy.deepcopy(job.get("input_snapshot")), "prompt": job.get("prompt"), "config": copy.deepcopy(dict(config)), "agent": copy.deepcopy(dict(agent)), "session_id": job.get("session_id")})
            result = self.provider.start(request)
            if inspect.isawaitable(result):
                remaining = self._remaining_deadline(job["run_id"])
                result = await asyncio.wait_for(result, remaining) if remaining is not None else await result
            if result is None: result = {}
            if not isinstance(result, Mapping): result = {"position": str(result), "raw_output": str(result)}
            result = dict(result)
            raw_value = result.get("raw_output", result.get("raw_stdout", result.get("raw", result.get("output"))))
            stderr = result.get("raw_stderr")
            raw = f"{raw_value or ''}{('\\n' + str(stderr)) if stderr else ''}" or None
            status = "failed" if result.get("ok") is False or result.get("error") else "completed"
            error = str(result.get("error")) if result.get("error") else None
            self.store.update_job(job_id, status=status, result=result, raw_output=None if raw is None else str(raw), error=error, completed_at=self._now(), session_id=result.get("session_id") or job.get("session_id"), workspace=result.get("workspace") or job.get("workspace"), artifacts=result.get("artifacts", job.get("artifacts", [])))
            if result.get("session_id"):
                self._sessions[(job["run_id"], job["agent_id"])] = str(result["session_id"])
            self.store.create_turn({"run_id": job["run_id"], "job_id": job_id, "agent_id": job["agent_id"], "phase": job["phase"], "index": job["index"], "status": status, "position": result.get("position", result.get("text", result.get("output"))), "confidence": result.get("confidence"), "input_ids": job["input_ids"], "at": self._now(), "raw_output": None if raw is None else str(raw)})
            event_kind = "failure" if status == "failed" else ("pass" if result.get("pass") else ("ownpost" if result.get("ownpost") else "turn"))
            self.store.add_event({"run_id": job["run_id"], "kind": event_kind, "agent_id": job["agent_id"], "job_id": job_id, "text": error or result.get("position", result.get("text", result.get("output", ""))), "payload": result})
            self._record_diary(job, result)
            prepared = next((p for p in self._prepared.get(str(job["run_id"]), []) if str(p.get("agent_id")) == job["agent_id"]), None)
            if prepared is not None:
                for key in ("testreport", "artifacts", "branch_ref", "branch"):
                    if result.get(key) is not None:
                        prepared[key] = copy.deepcopy(result[key])
        except asyncio.TimeoutError:
            await self._provider_cancel(job_id)
            message = "provider call exceeded run deadline"
            self.store.update_job(job_id, status="failed", completed_at=self._now(), error=message, raw_output=message)
            self.store.create_turn({"run_id": job["run_id"], "job_id": job_id, "agent_id": job["agent_id"], "phase": job["phase"], "index": job["index"], "status": "failed", "input_ids": job["input_ids"], "at": self._now(), "raw_output": message})
            self.store.add_event({"run_id": job["run_id"], "kind": "failure", "agent_id": job["agent_id"], "job_id": job_id, "text": message, "payload": {"error": message, "deadline": True}})
        except asyncio.CancelledError:
            self.store.update_job(job_id, status="cancelled", completed_at=self._now(), error="cancelled")
            self.store.create_turn({"run_id": job["run_id"], "job_id": job_id, "agent_id": job["agent_id"], "phase": job["phase"], "index": job["index"], "status": "cancelled", "input_ids": job["input_ids"], "at": self._now(), "raw_output": ""})
            self.store.add_event({"run_id": job["run_id"], "kind": "failure", "agent_id": job["agent_id"], "job_id": job_id, "text": "cancelled", "payload": {"error": "cancelled"}})
            raise
        except Exception as exc:
            self.store.update_job(job_id, status="failed", completed_at=self._now(), error=str(exc), raw_output=str(exc))
            self.store.create_turn({"run_id": job["run_id"], "job_id": job_id, "agent_id": job["agent_id"], "phase": job["phase"], "index": job["index"], "status": "failed", "input_ids": job["input_ids"], "at": self._now(), "raw_output": str(exc)})
            self.store.add_event({"run_id": job["run_id"], "kind": "failure", "agent_id": job["agent_id"], "job_id": job_id, "text": str(exc), "payload": {"error": str(exc)}})

    def _record_diary(self, job: Mapping[str, Any], result: Mapping[str, Any]) -> None:
        diary = result.get("diary")
        if diary in (None, ""):
            return
        root = Path(tempfile.gettempdir()) / "disputatio-diaries" / str(job["run_id"])
        root.mkdir(parents=True, exist_ok=True)
        path = root / f"{job['agent_id']}.md"
        path.write_text(str(diary), encoding="utf-8")
        self.store.add_event({"run_id": job["run_id"], "kind": "diary", "agent_id": job["agent_id"], "job_id": job["id"], "text": str(path), "payload": {"path": str(path)}})

    async def _integration(self, run_id: str, config: Mapping[str, Any], agent: Mapping[str, Any], transcript: list[dict[str, Any]]) -> None:
        """Create a dedicated integration worktree and one fresh provider session."""
        run = self.store.get_run(run_id, expanded=False) or {"id": run_id, "run_id": run_id, "config": dict(config)}
        run = {**run, **dict(config)}
        prepared_agents = self._prepared.get(str(run_id), [])
        integrate = getattr(self.worktrees, "integrate", None)
        prepared: dict[str, Any] = {}
        if integrate is not None:
            value = integrate(dict(run), copy.deepcopy(prepared_agents), integrator=dict(agent))
            if inspect.isawaitable(value): value = await value
            prepared = dict(value or {})
        input_ids = [x.get("id") for x in transcript if isinstance(x, Mapping) and x.get("id")]
        branches = prepared.get("source_branches", prepared.get("branch_refs", []))
        instructions = {"task": "Choose the best changes from these agent branches, merge or cherry-pick them, resolve conflicts, run tests, and report the result.", "role": agent.get("role", "integrator"), "phase": "integration", "visible_transcript": copy.deepcopy(transcript), "branch_refs": branches, "response_schema": {"post": "string", "position": "string|null", "confidence": "integer 0..100|null", "diary": "string|null", "replyrefs": "array", "evidence": "array", "testreport": "object|string|null"}}
        prompt = _json(instructions)
        snapshot = {"run_id": run_id, "agent_id": str(agent.get("id")), "phase": "integration", "index": 0, "input_ids": input_ids, "context": copy.deepcopy(transcript), "prompt": prompt, "branch_refs": branches}
        request = {"run_id": run_id, "job_id": _id("integration"), "agent_id": str(agent.get("id")), "phase": "integration", "index": 0, "inputs": copy.deepcopy(transcript), "input_ids": input_ids, "input_snapshot": snapshot, "prompt": prompt, "context": copy.deepcopy(transcript), "config": copy.deepcopy(dict(config)), "agent": copy.deepcopy(dict(agent)), "workspace": prepared.get("workspace"), "artifacts": prepared.get("artifacts", []), "session_id": None, "branch_refs": branches}
        job = self.store.create_job(request)
        try:
            result = self.provider.start(copy.deepcopy({**job, **request}))
            if inspect.isawaitable(result): result = await result
            result = dict(result or {})
            status = "failed" if result.get("error") or result.get("ok") is False else "completed"
            raw = result.get("raw_output", result.get("raw_stdout", result.get("output", "")))
            self.store.update_job(job["id"], status=status, result=result, raw_output=str(raw), error=str(result["error"]) if result.get("error") else None, completed_at=self._now(), session_id=result.get("session_id"), workspace=prepared.get("workspace"), artifacts=result.get("artifacts", prepared.get("artifacts", [])))
            self.store.create_turn({"run_id": run_id, "job_id": job["id"], "agent_id": agent.get("id"), "phase": "integration", "index": 0, "status": status, "position": result.get("position", result.get("text", result.get("output"))), "confidence": result.get("confidence"), "input_ids": input_ids, "at": self._now(), "raw_output": str(raw)})
            self.store.add_event({"run_id": run_id, "kind": "failure" if status == "failed" else "integration", "agent_id": agent.get("id"), "job_id": job["id"], "text": result.get("error") or result.get("position", result.get("text", result.get("output", ""))), "payload": result})
            self.store.publish(run_id, job["id"], {"workspace": prepared.get("workspace"), "artifacts": result.get("artifacts", prepared.get("artifacts", [])), "result": result, "worktree": prepared})
        except Exception as exc:
            self.store.update_job(job["id"], status="failed", error=str(exc), raw_output=str(exc), completed_at=self._now())
            self.store.add_event({"run_id": run_id, "kind": "failure", "agent_id": agent.get("id"), "job_id": job["id"], "text": str(exc), "payload": {"phase": "integration", "error": str(exc)}})
