"""Async adapters for the locally installed Codex and Claude CLIs.

The adapters deliberately keep the provider boundary small: a job is an input
mapping and a result is a plain mapping.  This lets the persistence layer keep
all output, including output it does not understand yet.
"""

from __future__ import annotations

import asyncio
import copy
import inspect
import json
import os
import signal
import shutil
import time
import uuid
from collections.abc import Awaitable, Callable, Mapping
from typing import Any


EventCallback = Callable[[dict[str, Any]], Any]


class ProviderError(Exception):
    """A provider call failed in a way that can be recorded by the core."""

    code = "provider_error"
    retryable = False

    def __init__(self, message: str, *, job_id: str | None = None) -> None:
        super().__init__(message)
        self.job_id = job_id


class RateLimitError(ProviderError):
    code = "rate_limited"
    retryable = True


class ProviderTimeoutError(ProviderError):
    code = "timeout"
    retryable = True


class ProviderCancelledError(ProviderError):
    code = "cancelled"
    retryable = True


# Names used by a few callers that prefer the shorter terminology.
TimeoutError = ProviderTimeoutError
CancelledError = ProviderCancelledError


def _as_text(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, str):
        return value
    return json.dumps(value, ensure_ascii=False, sort_keys=True)


def _job_snapshot(job: Mapping[str, Any]) -> dict[str, Any]:
    """Copy a job before yielding, so later caller mutations cannot leak in."""

    result = copy.deepcopy(dict(job))
    result.setdefault("id", result.get("job_id") or str(uuid.uuid4()))
    result.setdefault("job_id", result["id"])
    agent = result.get("agent")
    if not isinstance(agent, Mapping):
        agent = {
            key: result[key]
            for key in ("provider", "model", "role")
            if key in result
        }
    result["agent"] = copy.deepcopy(dict(agent))
    result.setdefault("phase", "seed")
    result.setdefault("session_id", None)
    result.setdefault("workspace", None)
    if "input_snapshot" not in result:
        # Keep only the already supplied job payload.  The core is responsible
        # for choosing what transcript is placed in that payload.
        result["input_snapshot"] = copy.deepcopy(
            result.get("input", result.get("prompt", ""))
        )
    else:
        result["input_snapshot"] = copy.deepcopy(result["input_snapshot"])
    return result


def _prompt(job: Mapping[str, Any]) -> str:
    value = job.get("prompt", job.get("input_snapshot", ""))
    if isinstance(value, str):
        return value
    return json.dumps(value, ensure_ascii=False, sort_keys=True)


def _normalise_final(value: Any) -> dict[str, Any]:
    """Return the stable final envelope required by the forum protocol."""

    if isinstance(value, Mapping):
        envelope = dict(value)
    else:
        envelope = {"post": _as_text(value)}
    if "post" not in envelope and "response" in envelope:
        envelope["post"] = envelope["response"]
    confidence = envelope.get("confidence")
    if isinstance(confidence, bool) or not isinstance(confidence, (int, float)):
        confidence = None
    elif not 0 <= confidence <= 100:
        confidence = None
    envelope["confidence"] = confidence
    for field in ("position", "diary", "replyrefs", "evidence", "testreport"):
        envelope.setdefault(field, None)
    return envelope


def _error_data(exc: BaseException) -> dict[str, Any]:
    return {
        "type": getattr(exc, "code", exc.__class__.__name__.lower()),
        "message": str(exc),
        "retryable": bool(getattr(exc, "retryable", False)),
    }


class AsyncCLIProvider:
    """Shared process and result handling for argv based provider CLIs."""

    provider = "cli"

    def __init__(
        self,
        executable: str | os.PathLike[str] | None = None,
        *,
        timeout: float | None = None,
        on_event: EventCallback | None = None,
    ) -> None:
        self.executable = os.fspath(executable or self.provider)
        self.timeout = timeout
        self.on_event = on_event
        self._processes: dict[str, asyncio.subprocess.Process] = {}
        self._cancelled: set[str] = set()
        self._guard = asyncio.Lock()

    def build_argv(self, job: Mapping[str, Any], *, resume: bool = False) -> list[str]:
        raise NotImplementedError

    def inspect(self) -> dict[str, Any]:
        """Return installation metadata without making a model call."""

        resolved = shutil.which(self.executable)
        return {"provider": self.provider, "executable": resolved, "available": bool(resolved)}

    async def ainspect(self) -> dict[str, Any]:
        """Run the provider's read-only help command, when installed."""

        resolved = shutil.which(self.executable) or self.executable
        try:
            process = await asyncio.create_subprocess_exec(
                resolved,
                "--help",
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
                start_new_session=True,
            )
            stdout, stderr = await process.communicate()
        except (OSError, asyncio.CancelledError) as exc:
            return {
                **self.inspect(),
                "help_returncode": None,
                "help_stdout": "",
                "help_stderr": str(exc),
            }
        return {
            **self.inspect(),
            "help_returncode": process.returncode,
            "help_stdout": stdout.decode(errors="replace"),
            "help_stderr": stderr.decode(errors="replace"),
        }

    def start(self, job: Mapping[str, Any]) -> Awaitable[dict[str, Any]]:
        # This method is intentionally synchronous at its outer edge.  An
        # async def would not snapshot a mutable dict until the first await.
        snapshot = _job_snapshot(job)
        return self._start(snapshot)

    async def _emit(self, event: dict[str, Any], callback: EventCallback | None) -> None:
        callback = callback or self.on_event
        if callback is None:
            return
        try:
            result = callback(event)
            if inspect.isawaitable(result):
                await result
        except Exception:
            # Persistence callbacks must never prevent process cleanup or turn
            # completion.  The event remains in the returned result.
            return

    async def _start(self, job: dict[str, Any]) -> dict[str, Any]:
        job_id = str(job["job_id"])
        callback = job.get("on_event") or job.get("event_callback")
        session_id = job.get("session_id")
        if not session_id:
            session_id = str(uuid.uuid4())
        result: dict[str, Any] = {
            "id": job["id"],
            "job_id": job_id,
            "provider": self.provider,
            "phase": job.get("phase"),
            "agent": copy.deepcopy(job.get("agent", {})),
            "session_id": session_id,
            "workspace": job.get("workspace"),
            "input_snapshot": copy.deepcopy(job.get("input_snapshot")),
            "status": "running",
            "raw_stdout": "",
            "raw_stderr": "",
            "events": [],
            "tool_events": [],
        }
        started = time.monotonic()
        async with self._guard:
            cancelled_before_start = job_id in self._cancelled
        if cancelled_before_start:
            return self._finish_error(result, ProviderCancelledError("job cancelled", job_id=job_id))
        try:
            argv = self.build_argv(job | {"session_id": session_id})
            result["argv"] = list(argv)
            process = await asyncio.create_subprocess_exec(
                *argv,
                cwd=job.get("workspace") or None,
                stdin=asyncio.subprocess.DEVNULL,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
                start_new_session=True,
            )
            async with self._guard:
                self._processes[job_id] = process
                already_cancelled = job_id in self._cancelled
            if already_cancelled:
                await self._kill(process)
                raise ProviderCancelledError("job cancelled", job_id=job_id)
            stdout_task = asyncio.create_task(self._read_stream(process.stdout, True, result, callback, job_id))
            stderr_task = asyncio.create_task(self._read_stream(process.stderr, False, result, callback, job_id))
            try:
                if self.timeout is None:
                    await process.wait()
                else:
                    await asyncio.wait_for(process.wait(), self.timeout)
            except asyncio.TimeoutError as exc:
                await self._kill(process)
                raise ProviderTimeoutError(f"provider call exceeded {self.timeout}s", job_id=job_id) from exc
            finally:
                # wait() only observes process termination; readers must still
                # drain any bytes written just before it exited.
                await asyncio.gather(stdout_task, stderr_task, return_exceptions=True)
            if job_id in self._cancelled:
                raise ProviderCancelledError("job cancelled", job_id=job_id)
            if process.returncode:
                message = result["raw_stderr"].strip() or f"provider exited with {process.returncode}"
                if self._is_rate_limited(message, result["events"]):
                    raise RateLimitError(message, job_id=job_id)
                raise ProviderError(message, job_id=job_id)
            final = self._extract_final(result["events"], result["raw_stdout"])
            for event in reversed(result["events"]):
                if isinstance(event, Mapping):
                    discovered = event.get("session_id", event.get("sessionId"))
                    if discovered:
                        result["session_id"] = str(discovered)
                        break
                    thread = event.get("thread_id", event.get("threadId"))
                    if thread:
                        result["session_id"] = str(thread)
                        break
            result["final"] = _normalise_final(final)
            result["output"] = result["final"].get("post")
            result["raw_output"] = result["raw_stdout"]
            result["status"] = "completed"
        except asyncio.CancelledError:
            process = self._processes.get(job_id)
            if process is not None:
                await self._kill(process)
            exc = ProviderCancelledError("provider task cancelled", job_id=job_id)
            self._finish_error(result, exc)
        except (ProviderError, OSError) as exc:
            if isinstance(exc, OSError) and self._is_rate_limited(str(exc), result["events"]):
                exc = RateLimitError(str(exc), job_id=job_id)
            self._finish_error(result, exc)
        finally:
            async with self._guard:
                self._processes.pop(job_id, None)
                self._cancelled.discard(job_id)
            result["duration_ms"] = round((time.monotonic() - started) * 1000, 3)
            result.setdefault("raw_output", result["raw_stdout"])
            result["stderr"] = result["raw_stderr"]
            await self._emit({"job_id": job_id, "kind": "result", "result": result}, callback)
        return result

    async def _read_stream(
        self,
        stream: asyncio.StreamReader | None,
        is_stdout: bool,
        result: dict[str, Any],
        callback: EventCallback | None,
        job_id: str,
    ) -> None:
        if stream is None:
            return
        chunks: list[str] = []
        while True:
            raw = await stream.readline()
            if not raw:
                break
            text = raw.decode(errors="replace")
            chunks.append(text)
            if is_stdout:
                event = self._parse_event(text)
                if event is not None:
                    result["events"].append(event)
                    if self._is_tool_event(event):
                        result["tool_events"].append(event)
                    await self._emit({"job_id": job_id, "kind": "tool" if self._is_tool_event(event) else "event", "event": event}, callback)
            else:
                await self._emit({"job_id": job_id, "kind": "stderr", "data": text}, callback)
        target = "".join(chunks)
        if is_stdout:
            result["raw_stdout"] = target
        else:
            result["raw_stderr"] = target

    @staticmethod
    def _parse_event(line: str) -> Any:
        try:
            return json.loads(line)
        except json.JSONDecodeError:
            return {"type": "stdout", "text": line.rstrip("\n")}

    @staticmethod
    def _is_tool_event(event: Any) -> bool:
        if not isinstance(event, Mapping):
            return False
        typ = str(event.get("type", "")).lower()
        item = event.get("item")
        item_type = str(item.get("type", "")).lower() if isinstance(item, Mapping) else ""
        return any(token in typ or token in item_type for token in ("tool", "function", "command"))

    @staticmethod
    def _is_rate_limited(message: str, events: list[Any]) -> bool:
        haystack = message.lower()
        if any(token in haystack for token in ("rate limit", "rate_limit", "too many requests", "429", "quota")):
            return True
        return any("rate" in str(event).lower() and "limit" in str(event).lower() for event in events)

    @classmethod
    def _extract_final(cls, events: list[Any], raw_stdout: str) -> Any:
        candidates: list[Any] = []
        for event in events:
            if not isinstance(event, Mapping):
                continue
            if "result" in event:
                candidates.append(event["result"])
            if "response" in event:
                candidates.append(event["response"])
            item = event.get("item")
            if isinstance(item, Mapping):
                if item.get("type") in {"agent_message", "assistant", "message"}:
                    candidates.append(item.get("text", item.get("content")))
                elif item.get("text") is not None:
                    candidates.append(item["text"])
            if event.get("type") in {"message", "assistant", "result", "final"}:
                candidates.append(event.get("text", event.get("content", event)))
        if candidates:
            return candidates[-1]
        text = raw_stdout.strip()
        if text:
            try:
                return json.loads(text)
            except json.JSONDecodeError:
                return text
        return ""

    @staticmethod
    def _finish_error(result: dict[str, Any], exc: BaseException) -> dict[str, Any]:
        result["status"] = getattr(exc, "code", "failed")
        result["error"] = _error_data(exc)
        result["final"] = _normalise_final("")
        return result

    @staticmethod
    async def _kill(process: asyncio.subprocess.Process) -> None:
        if process.returncode is not None:
            return
        try:
            if os.name == "posix":
                os.killpg(process.pid, signal.SIGTERM)
            else:
                process.terminate()
            await asyncio.wait_for(process.wait(), 2)
        except (ProcessLookupError, asyncio.TimeoutError):
            try:
                if os.name == "posix":
                    os.killpg(process.pid, signal.SIGKILL)
                else:
                    process.kill()
            except ProcessLookupError:
                pass
            await process.wait()

    def cancel(self, job_id: str) -> Awaitable[dict[str, Any]]:
        return self._cancel(str(job_id))

    async def _cancel(self, job_id: str) -> dict[str, Any]:
        async with self._guard:
            self._cancelled.add(job_id)
            process = self._processes.get(job_id)
        if process is not None:
            await self._kill(process)
        return {"job_id": job_id, "status": "cancel_requested"}


class CodexProvider(AsyncCLIProvider):
    provider = "codex"

    def build_argv(self, job: Mapping[str, Any], *, resume: bool = False) -> list[str]:
        model = job.get("agent", {}).get("model") or job.get("model")
        session = job.get("session_id")
        argv = [self.executable, "exec", "--json"]
        if model:
            argv.extend(["--model", str(model)])
        if resume or job.get("resume"):
            argv.extend(["resume", str(session)] if session else ["resume"])
        elif session:
            argv.extend(["--session-id", str(session)])
        argv.append(_prompt(job))
        return argv


class ClaudeProvider(AsyncCLIProvider):
    provider = "claude"

    def build_argv(self, job: Mapping[str, Any], *, resume: bool = False) -> list[str]:
        model = job.get("agent", {}).get("model") or job.get("model")
        session = job.get("session_id")
        argv = [self.executable, "-p", _prompt(job), "--output-format", "json"]
        if model:
            argv.extend(["--model", str(model)])
        if resume or job.get("resume"):
            if session:
                argv.extend(["--resume", str(session)])
        elif session:
            argv.extend(["--session-id", str(session)])
        return argv


# Friendly aliases for code that calls these adapters by their provider name.
Codex = CodexProvider
Claude = ClaudeProvider


def build_provider(config: Mapping[str, Any] | str, **kwargs: Any) -> AsyncCLIProvider:
    """Construct an adapter from a provider name or an agent mapping."""

    if isinstance(config, Mapping):
        provider = str(config.get("provider", "")).lower()
        kwargs = {**dict(config), **kwargs}
    else:
        provider = str(config).lower()
    if provider == "codex":
        return CodexProvider(**{k: v for k, v in kwargs.items() if k in {"executable", "timeout", "on_event"}})
    if provider == "claude":
        return ClaudeProvider(**{k: v for k, v in kwargs.items() if k in {"executable", "timeout", "on_event"}})
    raise ValueError(f"unsupported provider: {provider}")


__all__ = [
    "AsyncCLIProvider", "CancelledError", "Claude", "ClaudeProvider", "Codex", "CodexProvider",
    "ProviderCancelledError", "ProviderError", "ProviderTimeoutError", "RateLimitError", "TimeoutError",
    "build_provider",
]
