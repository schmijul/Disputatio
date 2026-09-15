"""Safe, recoverable Git worktrees for coding agents.

All Git invocations use argument vectors and are scoped to the configured
repository.  Worktrees intentionally live under a runtime directory outside
the source repository and are retained for inspection and recovery.
"""

from __future__ import annotations

import asyncio
import copy
import os
import re
import subprocess
import tempfile
import threading
import uuid
from collections.abc import Callable, Mapping, Sequence
from pathlib import Path
from typing import Any


class WorktreeError(RuntimeError):
    """A requested worktree operation could not be completed."""


def _slug(value: Any) -> str:
    text = re.sub(r"[^A-Za-z0-9_.-]+", "-", str(value or "item")).strip("-")
    return text[:48] or "item"


def _value(value: Any, *keys: str, default: Any = None) -> Any:
    if isinstance(value, Mapping):
        for key in keys:
            if key in value:
                return value[key]
    else:
        for key in keys:
            if hasattr(value, key):
                return getattr(value, key)
    return default


class WorktreeManager:
    """Create agent branches and retained integration workspaces."""

    _repo_locks: dict[str, threading.Lock] = {}
    _repo_locks_guard = threading.Lock()

    def __init__(self, runtime_dir: str | os.PathLike[str] | None = None) -> None:
        self.runtime_dir = Path(runtime_dir) if runtime_dir else Path(tempfile.mkdtemp(prefix="disputatio-worktrees-"))
        self.runtime_dir.mkdir(parents=True, exist_ok=True)
        self._bases: dict[tuple[str, str], str] = {}

    @classmethod
    def _lock_for(cls, repo: Path) -> threading.Lock:
        key = str(repo.resolve())
        with cls._repo_locks_guard:
            return cls._repo_locks.setdefault(key, threading.Lock())

    @staticmethod
    def _git(repo: Path, *args: str, check: bool = True) -> str:
        try:
            completed = subprocess.run(
                ["git", *args],
                cwd=repo,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                check=False,
            )
        except OSError as exc:
            raise WorktreeError(f"unable to run git: {exc}") from exc
        if check and completed.returncode:
            detail = completed.stderr.strip() or completed.stdout.strip()
            raise WorktreeError(f"git {' '.join(args)} failed ({completed.returncode}): {detail}")
        return completed.stdout

    def _repository(self, run: Mapping[str, Any]) -> Path:
        raw = _value(run, "target_repo", "repository", "repo", "repo_path", "source_repo")
        if raw is None:
            raise WorktreeError("run does not specify a target Git repository")
        path = Path(os.fspath(raw)).expanduser().resolve()
        if not path.is_dir():
            raise WorktreeError(f"target repository does not exist: {path}")
        return path

    def _base(self, run: Mapping[str, Any]) -> tuple[Path, str]:
        repo = self._repository(run)
        run_id = str(_value(run, "id", "run_id", default="default"))
        key = (str(repo), run_id)
        if key in self._bases:
            return repo, self._bases[key]
        lock = self._lock_for(repo)
        with lock:
            # Validate the configured checkout exactly once per run/repository.
            root = Path(self._git(repo, "rev-parse", "--show-toplevel").strip()).resolve()
            if root != repo:
                repo = root
                key = (str(repo), run_id)
                if key in self._bases:
                    return repo, self._bases[key]
            status = self._git(repo, "status", "--porcelain", check=True).strip()
            if status:
                raise WorktreeError("target repository must have a clean checkout")
            try:
                base = self._git(repo, "rev-parse", "--verify", "HEAD", check=True).strip()
            except WorktreeError as exc:
                raise WorktreeError("target repository must have a committed HEAD") from exc
            if not base:
                raise WorktreeError("target repository must have a committed HEAD")
            self._bases[key] = base
            return repo, base

    def prepare(self, run: Mapping[str, Any], agent: Mapping[str, Any] | Any) -> Any:
        # Snapshot inputs before the first await for the same reason as the
        # provider adapter: caller mutation cannot alter a queued job.
        run_snapshot = copy.deepcopy(dict(run))
        agent_snapshot = copy.deepcopy(dict(agent)) if isinstance(agent, Mapping) else copy.deepcopy(vars(agent))
        return self._prepare(run_snapshot, agent_snapshot)

    async def _prepare(self, run: dict[str, Any], agent: dict[str, Any]) -> dict[str, Any]:
        repo, base = await asyncio.to_thread(self._base, run)
        run_id = str(_value(run, "id", "run_id", default="run"))
        agent_id = str(_value(agent, "id", "agent_id", "name", default=str(uuid.uuid4())))
        token = uuid.uuid4().hex
        branch = f"disputatio/{_slug(run_id)}/{_slug(agent_id)}-{token[:10]}"
        workspace = self.runtime_dir / f"{_slug(run_id)}-{_slug(agent_id)}-{token}"
        workspace.parent.mkdir(parents=True, exist_ok=True)
        lock = self._lock_for(repo)
        try:
            with lock:
                self._git(repo, "worktree", "add", "--quiet", "-b", branch, str(workspace), base)
        except Exception:
            workspace.rmdir() if workspace.exists() and not any(workspace.iterdir()) else None
            raise
        return {
            "workspace": str(workspace),
            "artifacts": [],
            "run_id": run_id,
            "agent_id": agent_id,
            "base_sha": base,
            "branch": branch,
            "branch_ref": branch,
            "repository": str(repo),
            "status": "prepared",
            "testreport": None,
        }

    def checkpoint(
        self,
        prepared: Mapping[str, Any],
        *,
        testreport: Mapping[str, Any] | str | None = None,
        message: str | None = None,
    ) -> Any:
        snapshot = copy.deepcopy(dict(prepared))
        return self._checkpoint(snapshot, testreport=testreport, message=message)

    async def _checkpoint(
        self,
        prepared: dict[str, Any],
        *,
        testreport: Mapping[str, Any] | str | None,
        message: str | None,
    ) -> dict[str, Any]:
        workspace = Path(str(prepared["workspace"])).resolve()
        repo = Path(str(prepared.get("repository") or workspace)).resolve()
        if not workspace.is_dir() or not (workspace / ".git").exists():
            raise WorktreeError(f"prepared workspace is unavailable: {workspace}")
        lock = self._lock_for(repo)
        with lock:
            status = self._git(workspace, "status", "--porcelain")
            changed = bool(status.strip())
            if changed:
                self._git(workspace, "add", "--all")
                commit_message = message or f"Disputatio checkpoint for {prepared.get('agent_id', 'agent')}"
                self._git(
                    workspace,
                    "-c", "user.name=Disputatio agent",
                    "-c", "user.email=disputatio@localhost",
                    "commit", "--no-verify", "-m", commit_message,
                )
            commit_sha = self._git(workspace, "rev-parse", "HEAD").strip()
            base_sha = str(prepared.get("base_sha") or commit_sha)
            diff = self._git(workspace, "diff", f"{base_sha}..HEAD", "--stat")
            after_status = self._git(workspace, "status", "--porcelain")
        report = copy.deepcopy(testreport)
        prepared.update({
            "commit_sha": commit_sha,
            "changed": changed,
            "diff": diff,
            "status_output": after_status,
            "testreport": report,
            "checkpoint": {"commit_sha": commit_sha, "diff": diff, "status": after_status, "testreport": report},
            "status": "checkpointed",
        })
        return prepared

    def integrate(
        self,
        run: Mapping[str, Any],
        prepared_agents: Sequence[Mapping[str, Any]],
        *,
        integrator: Mapping[str, Any] | Any | None = None,
    ) -> Any:
        run_snapshot = copy.deepcopy(dict(run))
        agents_snapshot = [copy.deepcopy(dict(item)) for item in prepared_agents]
        integrator_snapshot = copy.deepcopy(dict(integrator)) if isinstance(integrator, Mapping) else copy.deepcopy(vars(integrator)) if integrator is not None else {}
        return self._integrate(run_snapshot, agents_snapshot, integrator_snapshot)

    async def _integrate(
        self,
        run: dict[str, Any],
        prepared_agents: list[dict[str, Any]],
        integrator: dict[str, Any],
    ) -> dict[str, Any]:
        repo, base = await asyncio.to_thread(self._base, run)
        run_id = str(_value(run, "id", "run_id", default="run"))
        token = uuid.uuid4().hex
        name = _value(integrator, "id", "agent_id", "name", default="integrator")
        branch = f"disputatio/{_slug(run_id)}/integration-{_slug(name)}-{token[:10]}"
        workspace = self.runtime_dir / f"{_slug(run_id)}-integration-{token}"
        lock = self._lock_for(repo)
        with lock:
            self._git(repo, "worktree", "add", "--quiet", "-b", branch, str(workspace), base)
        refs = [str(item.get("branch_ref") or item.get("branch")) for item in prepared_agents if item.get("branch_ref") or item.get("branch")]
        result: dict[str, Any] = {
            "workspace": str(workspace),
            "artifacts": [],
            "run_id": run_id,
            "agent_id": str(name),
            "base_sha": base,
            "branch": branch,
            "branch_ref": branch,
            "repository": str(repo),
            "source_branches": refs,
            "status": "ready",
            "conflicts": [],
            "report": None,
            "testreport": None,
        }
        # Merge in branch order.  A conflict is left in this result worktree so
        # the integrator can inspect and resolve it there; never auto-resolve
        # files or call the outcome successful.
        for ref in refs:
            merge = await asyncio.to_thread(self._git, workspace, "merge", "--no-commit", "--no-ff", ref, check=False)
            if self._git(workspace, "diff", "--name-only", "--diff-filter=U", check=True).strip():
                conflicts = self._git(workspace, "diff", "--name-only", "--diff-filter=U").splitlines()
                result["conflicts"] = conflicts
                result["status"] = "conflict"
                result["report"] = {"status": "conflict", "conflicts": conflicts, "source_branches": refs}
                result["status_output"] = merge
                return result
            # A clean merge is staged; commit it before the next branch so each
            # branch ref remains independently auditable.
            await asyncio.to_thread(
                self._git,
                workspace,
                "-c", "user.name=Disputatio integrator", "-c", "user.email=disputatio@localhost",
                "commit", "--no-verify", "-m", f"Disputatio integration: {ref}",
            )
        result["status"] = "integrated" if refs else "ready"
        result["commit_sha"] = await asyncio.to_thread(self._git, workspace, "rev-parse", "HEAD")
        result["report"] = {"status": result["status"], "conflicts": [], "source_branches": refs}
        return result

    def report(self, prepared: Mapping[str, Any], *, testreport: Any = None) -> Any:
        snapshot = copy.deepcopy(dict(prepared))
        return self._report(snapshot, testreport=testreport)

    async def _report(self, prepared: dict[str, Any], *, testreport: Any) -> dict[str, Any]:
        workspace = Path(str(prepared["workspace"])).resolve()
        if not workspace.is_dir():
            raise WorktreeError(f"workspace is unavailable: {workspace}")
        status = await asyncio.to_thread(self._git, workspace, "status", "--porcelain")
        conflicts = await asyncio.to_thread(self._git, workspace, "diff", "--name-only", "--diff-filter=U")
        prepared["status_output"] = status
        prepared["conflicts"] = conflicts.splitlines()
        prepared["testreport"] = copy.deepcopy(testreport if testreport is not None else prepared.get("testreport"))
        if prepared["conflicts"]:
            state = "conflict"
        else:
            state = str(prepared.get("status", "reported"))
        prepared["report"] = {
            "status": state,
            "conflicts": prepared["conflicts"],
            "status_output": status,
            "testreport": prepared["testreport"],
            "commit_sha": prepared.get("commit_sha"),
        }
        prepared["status"] = state
        return prepared

    def recover(self, prepared: Mapping[str, Any]) -> dict[str, Any]:
        """Validate a retained prepared workspace after process restart."""

        result = copy.deepcopy(dict(prepared))
        workspace = Path(str(result.get("workspace", ""))).resolve()
        if not workspace.is_dir():
            raise WorktreeError(f"retained workspace is unavailable: {workspace}")
        result["workspace"] = str(workspace)
        result["recovered"] = True
        return result


# A small functional facade keeps integration with dict-oriented core code easy.
_default_manager = WorktreeManager()


def prepare(run: Mapping[str, Any], agent: Mapping[str, Any] | Any) -> Any:
    return _default_manager.prepare(run, agent)


def checkpoint(prepared: Mapping[str, Any], **kwargs: Any) -> Any:
    return _default_manager.checkpoint(prepared, **kwargs)


def integrate(run: Mapping[str, Any], prepared_agents: Sequence[Mapping[str, Any]], **kwargs: Any) -> Any:
    return _default_manager.integrate(run, prepared_agents, **kwargs)


def report(prepared: Mapping[str, Any], **kwargs: Any) -> Any:
    return _default_manager.report(prepared, **kwargs)


__all__ = ["WorktreeError", "WorktreeManager", "checkpoint", "integrate", "prepare", "report"]
