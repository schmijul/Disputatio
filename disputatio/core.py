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
from datetime import datetime, timezone
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
        self._lock = asyncio.Lock()

    def _now(self) -> str:
        value = self.now() if callable(self.now) else self.now
        if isinstance(value, datetime): return value.astimezone(timezone.utc).isoformat()
        return str(value)

    async def create(self, config: Mapping[str, Any]) -> dict[str, Any]:
        return self.store.create_run(config)

    def get(self, run_id: str) -> dict[str, Any] | None: return self.store.get_run(run_id)
    def list(self) -> list[dict[str, Any]]: return self.store.list_runs()

    async def start(self, run_id: str) -> dict[str, Any]:
        run = self.store.get_run(run_id)
        if run is None: raise KeyError(f"unknown run: {run_id}")
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
                task.cancel()
        task = self._tasks.get(run_id)
        if task and not task.done(): task.cancel()
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
        value = self.store.create_job({"run_id": run_id, "agent_id": original["agent_id"], "phase": original["phase"], "index": original["index"], "inputs": original["inputs"], "input_ids": original["input_ids"], "payload": payload, "session_id": _id("session"), "workspace": original.get("workspace")})
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
            # Seed jobs are created together, giving all agents an identical empty snapshot.
            if not self.store.list_jobs(run_id):
                await self._batch(run_id, config, agents, "seed", 0, [])
            if await self._should_halt(run_id): return
            attempts = int(config.get("attempts", config.get("discussion_attempts", 4)))
            regular_rounds = max(0, attempts - 2)
            for index in range(1, regular_rounds + 1):
                if await self._should_halt(run_id): return
                await self._batch(run_id, config, agents, "regular", index, self._transcript(run_id))
            if await self._should_halt(run_id): return
            regular_transcript = self._transcript(run_id)
            await self._batch(run_id, config, agents, "closing", attempts - 1, regular_transcript)
            if await self._should_halt(run_id): return
            if self.worktrees is not None:
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

    async def _should_halt(self, run_id: str) -> bool:
        run = self.store.get_run(run_id)
        if run is None or run_id in self._cancelled: return True
        if run["status"] in {"stopping", "stopped", "paused", "pausing"}: return True
        deadline = run.get("deadline")
        if deadline and self._now() >= str(deadline):
            await self.stop(run_id, "deadline exceeded")
            return True
        return False

    async def _batch(self, run_id: str, config: Mapping[str, Any], agents: list[Mapping[str, Any]], phase: str, index: int, transcript: list[dict[str, Any]]) -> None:
        jobs: list[dict[str, Any]] = []
        for agent in agents:
            context = copy.deepcopy(transcript)
            job = self.store.create_job({"run_id": run_id, "agent_id": str(agent.get("id")), "phase": phase, "index": index, "inputs": context, "input_ids": [x.get("id") for x in context if x.get("id")], "payload": {"context": context, "config": copy.deepcopy(dict(config)), "agent": copy.deepcopy(dict(agent))}, "session_id": _id("session")})
            jobs.append(job)
        limit = min(self.max_parallel, int(config.get("max_parallel", self.max_parallel)))
        semaphore = asyncio.Semaphore(max(1, limit))
        async def one(job: dict[str, Any]) -> None:
            async with semaphore:
                await self._execute(job["id"], next(a for a in agents if str(a.get("id")) == job["agent_id"]), config)
        tasks = [asyncio.create_task(one(j), name=f"job:{j['id']}") for j in jobs]
        await asyncio.gather(*tasks, return_exceptions=True)

    async def _execute(self, job_id: str, agent: Mapping[str, Any], config: Mapping[str, Any]) -> None:
        job = self.store.get_job(job_id)
        if job is None: return
        self.store.update_job(job_id, status="running", started_at=self._now())
        try:
            request = copy.deepcopy(job)
            request.update({"context": copy.deepcopy(job["inputs"]), "config": copy.deepcopy(dict(config)), "agent": copy.deepcopy(dict(agent)), "session_id": job.get("session_id") or _id("session")})
            result = self.provider.start(request)
            if inspect.isawaitable(result): result = await result
            if result is None: result = {}
            if not isinstance(result, Mapping): result = {"position": str(result), "raw_output": str(result)}
            result = dict(result)
            raw = result.get("raw_output", result.get("raw", result.get("output")))
            status = "failed" if result.get("ok") is False or result.get("error") else "completed"
            error = str(result.get("error")) if result.get("error") else None
            self.store.update_job(job_id, status=status, result=result, raw_output=None if raw is None else str(raw), error=error, completed_at=self._now(), workspace=result.get("workspace"), artifacts=result.get("artifacts", []))
            self.store.create_turn({"run_id": job["run_id"], "job_id": job_id, "agent_id": job["agent_id"], "phase": job["phase"], "index": job["index"], "status": status, "position": result.get("position", result.get("text", result.get("output"))), "confidence": result.get("confidence"), "input_ids": job["input_ids"], "at": self._now(), "raw_output": None if raw is None else str(raw)})
            self.store.add_event({"run_id": job["run_id"], "kind": "failure" if status == "failed" else ("pass" if result.get("pass") else "turn"), "agent_id": job["agent_id"], "job_id": job_id, "text": error or result.get("position", result.get("text", result.get("output", ""))), "payload": result})
        except asyncio.CancelledError:
            self.store.update_job(job_id, status="cancelled", completed_at=self._now(), error="cancelled")
            self.store.create_turn({"run_id": job["run_id"], "job_id": job_id, "agent_id": job["agent_id"], "phase": job["phase"], "index": job["index"], "status": "cancelled", "input_ids": job["input_ids"], "at": self._now(), "raw_output": ""})
            self.store.add_event({"run_id": job["run_id"], "kind": "failure", "agent_id": job["agent_id"], "job_id": job_id, "text": "cancelled", "payload": {"error": "cancelled"}})
            raise
        except Exception as exc:
            self.store.update_job(job_id, status="failed", completed_at=self._now(), error=str(exc), raw_output=str(exc))
            self.store.create_turn({"run_id": job["run_id"], "job_id": job_id, "agent_id": job["agent_id"], "phase": job["phase"], "index": job["index"], "status": "failed", "input_ids": job["input_ids"], "at": self._now(), "raw_output": str(exc)})
            self.store.add_event({"run_id": job["run_id"], "kind": "failure", "agent_id": job["agent_id"], "job_id": job_id, "text": str(exc), "payload": {"error": str(exc)}})

    async def _integration(self, run_id: str, config: Mapping[str, Any], agent: Mapping[str, Any], transcript: list[dict[str, Any]]) -> None:
        prepared: dict[str, Any] = {}
        prepare = getattr(self.worktrees, "prepare", None) or getattr(self.worktrees, "asyncprepare", None)
        if prepare:
            prepared = prepare(run_id, dict(agent))
            if inspect.isawaitable(prepared): prepared = await prepared
            prepared = dict(prepared or {})
        request = {"run_id": run_id, "job_id": _id("integration"), "agent_id": str(agent.get("id")), "phase": "integration", "index": 0, "inputs": copy.deepcopy(transcript), "input_ids": [x.get("id") for x in transcript if x.get("id")], "context": copy.deepcopy(transcript), "config": copy.deepcopy(dict(config)), "agent": copy.deepcopy(dict(agent)), "workspace": prepared.get("workspace"), "artifacts": prepared.get("artifacts", []), "session_id": _id("session"), "branch_refs": prepared.get("branch_refs", {})}
        job = self.store.create_job(request)
        result = self.provider.start(copy.deepcopy({**job, **request}))
        if inspect.isawaitable(result): result = await result
        result = dict(result or {})
        self.store.update_job(job["id"], status="completed" if not result.get("error") else "failed", result=result, raw_output=str(result.get("raw_output", result.get("output", ""))), completed_at=self._now(), workspace=prepared.get("workspace"), artifacts=result.get("artifacts", prepared.get("artifacts", [])))
        self.store.create_turn({"run_id": run_id, "job_id": job["id"], "agent_id": agent.get("id"), "phase": "integration", "index": 0, "status": "completed" if not result.get("error") else "failed", "position": result.get("position", result.get("text", result.get("output"))), "confidence": result.get("confidence"), "input_ids": request["input_ids"], "at": self._now(), "raw_output": str(result.get("raw_output", result.get("output", "")))})
        self.store.publish(run_id, job["id"], {"workspace": prepared.get("workspace"), "artifacts": result.get("artifacts", prepared.get("artifacts", [])), "result": result})
