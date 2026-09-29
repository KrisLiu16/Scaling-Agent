"""pi coding agent adapter (https://pi.dev, npm `@earendil-works/pi-coding-agent`), driven over RPC.

* One long-lived `pi --mode rpc` subprocess per session. A turn is one `prompt` command and ends at
  `agent_settled`, which comes only after pi's own retries, overflow recovery and compaction are done
  (`agent_end` can fire several times inside one turn).
* pi has no MCP client. `pi_extension/org.ts` registers the coordination server's tools from its MCP
  `tools/list`, denies pushes to the main branch and appends pending updates after every other tool.
* The model is any OpenAI-compatible endpoint described in `harness_options` (see `PiOptions`); the
  API key is read from the environment variable it names, never written to disk.
* The session id is chosen up front and passed with `--session-id`; resuming needs the transcript in
  the session directory, otherwise `start` fails so the runtime opens a fresh session with a handoff.
"""

from __future__ import annotations

import asyncio
import collections
import contextlib
import itertools
import json
import logging
import os
import signal
import uuid
from pathlib import Path
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict

from .base import DrainFn, HarnessAdapter, TurnOutcome
from .guards import push_denied_reason

log = logging.getLogger(__name__)

EXTENSION = Path(__file__).parent / "pi_extension" / "org.ts"
STREAM_LIMIT = 128 * 1024 * 1024  # one JSONL record, e.g. a large tool result
START_TIMEOUT_S = 120.0
COMMAND_TIMEOUT_S = 60.0
SHUTDOWN_GRACE_S = 10.0
SESSION_TAG_VAR = "SA_PI_SESSION_TAG"


class PiOptions(BaseModel):
    """`harness_options` of the run file for `harness: pi`."""

    model_config = ConfigDict(extra="forbid")

    base_url: str  # OpenAI-compatible root including /v1
    api: Literal["openai-responses", "openai-completions"] = "openai-responses"
    api_key_env: str = "OPENAI_API_KEY"  # name of the environment variable that holds the key
    provider: str = "gateway"
    context_window: int = 272_000
    max_tokens: int = 32_000  # pi caps a request's output at 32000 regardless
    reasoning: bool = True
    thinking: Literal["off", "minimal", "low", "medium", "high", "xhigh", "max"] | None = None
    headers: dict[str, str] = {}
    # Agent-level retries of transient provider errors (overload, 5xx); pi's own default is 3.
    max_retries: int = 8
    bash_timeout_s: int = 600  # applied to bash commands the model gives no timeout (pi has no default)
    binary: str = "pi"


def _kill_tagged(tag: str) -> None:
    """SIGKILL every process whose environment carries this session's tag: the shell commands pi started,
    including background jobs that outlived them (they inherit the environment, not the process group)."""
    if not tag:
        return
    needle = f"{SESSION_TAG_VAR}={tag}".encode()
    for entry in Path("/proc").iterdir():
        if not entry.name.isdigit() or int(entry.name) == os.getpid():
            continue
        try:
            if needle in (entry / "environ").read_bytes().split(b"\0"):
                os.kill(int(entry.name), signal.SIGKILL)
        except (OSError, ProcessLookupError):
            continue  # gone already, or not ours to read


class PiExited(RuntimeError):
    pass


class PiAdapter(HarnessAdapter):
    def __init__(
        self,
        coord_url: str,
        token: str,
        workdir: str,
        drain: DrainFn | None = None,  # unused: the extension drains updates itself
        model: str | None = None,
        env: dict[str, str] | None = None,
        main_branch: str = "main",
        options: dict[str, Any] | None = None,
        **_: object,
    ) -> None:
        if not model:
            raise ValueError("harness pi needs a model")
        self._coord_url = coord_url.rstrip("/")
        self._token = token
        self._workdir = workdir
        self._model = model
        self._env = env or {}
        self._main = main_branch
        self._options = PiOptions.model_validate(options or {})
        agent_dir = self._env.get("PI_CODING_AGENT_DIR")
        if not agent_dir:
            import tempfile

            agent_dir = tempfile.mkdtemp(prefix="sa-pi-")
        self._agent_dir = Path(agent_dir)
        self._sessions = self._agent_dir / "sessions"
        self._proc: asyncio.subprocess.Process | None = None
        self._tasks: list[asyncio.Task[None]] = []
        self._events: asyncio.Queue[dict[str, Any] | None] = asyncio.Queue()
        self._pending: dict[str, asyncio.Future[dict[str, Any]]] = {}
        self._ids = itertools.count(1)
        self._stderr: collections.deque[str] = collections.deque(maxlen=40)
        self._session_tag = ""

    # ---------------------------------------------------------------- config

    def _write_config(self, system_prompt: str, environ: dict[str, str]) -> None:
        o = self._options
        if o.api_key_env not in environ:
            raise RuntimeError(f"model key variable {o.api_key_env} is not set in the worker environment")
        provider: dict[str, Any] = {
            "baseUrl": o.base_url,
            "api": o.api,
            "apiKey": f"${o.api_key_env}",
            "models": [
                {
                    "id": self._model,
                    "name": self._model,
                    "reasoning": o.reasoning,
                    "input": ["text"],
                    "contextWindow": o.context_window,
                    "maxTokens": o.max_tokens,
                }
            ],
        }
        if o.headers:
            provider["headers"] = o.headers
        settings = {"retry": {"enabled": True, "maxRetries": o.max_retries}, "quietStartup": True}
        self._agent_dir.mkdir(parents=True, exist_ok=True)
        for name, content in (
            ("models.json", {"providers": {o.provider: provider}}),
            ("settings.json", settings),
        ):
            path = self._agent_dir / name
            fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
            with os.fdopen(fd, "w") as f:
                json.dump(content, f, indent=2)
        (self._agent_dir / "system-prompt.md").write_text(system_prompt)

    # ------------------------------------------------------------------ rpc

    async def _read_stdout(self, stream: asyncio.StreamReader) -> None:
        try:
            while line := await stream.readline():  # splits on LF only, as the protocol requires
                try:
                    record = json.loads(line)
                except json.JSONDecodeError:
                    log.warning("pi wrote a non-JSON line: %.200r", line)
                    continue
                kind = record.get("type")
                if kind == "message_update":
                    continue  # streaming deltas: nothing here needs them
                if kind == "response" and record.get("id") in self._pending:
                    future = self._pending[record["id"]]
                    if not future.done():
                        future.set_result(record)
                else:
                    self._events.put_nowait(record)
        finally:
            self._events.put_nowait(None)
            for future in self._pending.values():
                if not future.done():
                    future.set_exception(PiExited(self._exit_message()))

    async def _read_stderr(self, stream: asyncio.StreamReader) -> None:
        while line := await stream.readline():
            text = line.decode(errors="replace").rstrip()
            self._stderr.append(text)
            log.info("pi: %s", text)

    def _exit_message(self) -> str:
        code = self._proc.returncode if self._proc else None
        return f"pi exited (code {code}): {' | '.join(list(self._stderr)[-5:])}"

    def _write(self, command: dict[str, Any]) -> None:
        assert self._proc is not None and self._proc.stdin is not None
        if self._proc.stdin.is_closing():
            raise PiExited(self._exit_message())
        self._proc.stdin.write(json.dumps(command).encode() + b"\n")

    async def _command(self, command: dict[str, Any], timeout: float = COMMAND_TIMEOUT_S) -> dict[str, Any]:
        cmd_id = str(next(self._ids))
        future: asyncio.Future[dict[str, Any]] = asyncio.get_running_loop().create_future()
        self._pending[cmd_id] = future
        try:
            self._write({**command, "id": cmd_id})
            assert self._proc is not None and self._proc.stdin is not None
            await self._proc.stdin.drain()
            record = await asyncio.wait_for(future, timeout)
        finally:
            self._pending.pop(cmd_id, None)
        if not record.get("success"):
            raise RuntimeError(f"pi {command['type']} failed: {record.get('error')}")
        return record.get("data") or {}

    async def _next_event(self) -> dict[str, Any]:
        event = await self._events.get()
        if event is None:
            self._events.put_nowait(None)
            raise PiExited(self._exit_message())
        return event

    # ------------------------------------------------------------- lifecycle

    async def start(self, system_prompt: str, resume: str | None = None) -> None:
        o = self._options
        environ = {**os.environ, **self._env}
        self._write_config(system_prompt, environ)
        if resume and not any(self._sessions.rglob(f"*_{resume}.jsonl")):
            raise RuntimeError(f"pi session {resume} has no transcript in {self._sessions}")
        ready = self._agent_dir / "org-tools.json"
        ready.unlink(missing_ok=True)
        environ.update(
            {
                "PI_CODING_AGENT_DIR": str(self._agent_dir),
                "PI_OFFLINE": "1",  # no catalog refresh; model requests still go out
                "PI_SKIP_VERSION_CHECK": "1",
                "PI_TELEMETRY": "0",
                "SA_ORG_COORD_URL": self._coord_url,
                "SA_ORG_TOKEN": self._token,
                "SA_ORG_MAIN_BRANCH": self._main,
                "SA_ORG_PUSH_DENIED_REASON": push_denied_reason(self._main),
                "SA_ORG_READY_FILE": str(ready),
                "SA_ORG_BASH_TIMEOUT_S": str(o.bash_timeout_s),
            }
        )
        session_id = resume or str(uuid.uuid4())
        self._session_tag = uuid.uuid4().hex
        environ[SESSION_TAG_VAR] = self._session_tag
        cmd = [
            o.binary, "--mode", "rpc",
            "--session-dir", str(self._sessions), "--session-id", session_id,
            "--provider", o.provider, "--model", self._model,
            "--no-extensions", "--extension", str(EXTENSION),  # only ours, no discovered ones
            "--no-skills", "--no-prompt-templates", "--no-context-files",  # peers write into the repo
            "--no-themes",
            "--append-system-prompt", str(self._agent_dir / "system-prompt.md"),
        ]  # fmt: skip
        if o.thinking:
            cmd += ["--thinking", o.thinking]
        self._events = asyncio.Queue()
        self._stderr.clear()
        self._proc = await asyncio.create_subprocess_exec(
            *cmd,
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            cwd=self._workdir,
            env=environ,
            limit=STREAM_LIMIT,
            start_new_session=True,  # so close() can take the shell commands pi started down with it
        )
        assert self._proc.stdout is not None and self._proc.stderr is not None
        self._tasks = [
            asyncio.create_task(self._read_stdout(self._proc.stdout)),
            asyncio.create_task(self._read_stderr(self._proc.stderr)),
        ]
        try:
            state = await self._command({"type": "get_state"}, timeout=START_TIMEOUT_S)
            model = (state.get("model") or {}).get("id")
            if model != self._model:
                raise RuntimeError(f"pi selected model {model!r}, wanted {self._model!r}")
            try:
                loaded = json.loads(ready.read_text())["tools"]
            except (OSError, ValueError, KeyError):
                loaded = []
            if not loaded:  # pi keeps running when an extension fails to load; that would be a worker without org tools
                raise RuntimeError(f"organization tools did not load: {' | '.join(list(self._stderr)[-5:])}")
        except BaseException:
            await self.close()
            raise
        self.session_id = state.get("sessionId") or session_id

    async def run_turn(self, prompt: str) -> TurnOutcome:
        if self._proc is None:
            raise RuntimeError("adapter not started")
        while not self._events.empty():
            self._events.get_nowait()  # nothing from before this turn counts
        outcome = TurnOutcome()
        last_assistant: dict[str, Any] | None = None
        retry_error: str | None = None
        try:
            await self._command({"type": "prompt", "message": prompt})
            while (event := await self._next_event()).get("type") != "agent_settled":
                kind = event.get("type")
                if kind == "tool_execution_start":
                    name = str(event.get("toolName"))
                    outcome.tool_calls += 1
                    outcome.tools[name] = outcome.tools.get(name, 0) + 1
                elif kind == "message_end" and (event.get("message") or {}).get("role") == "assistant":
                    last_assistant = event["message"]
                elif kind == "auto_retry_end":
                    retry_error = None if event.get("success") else str(event.get("finalError") or "retries exhausted")
        except asyncio.CancelledError:
            with contextlib.suppress(Exception):
                self._write({"type": "abort"})
            raise
        stop = (last_assistant or {}).get("stopReason")
        if stop in ("error", "aborted") or retry_error:
            outcome.ok = False
            outcome.error = retry_error or str((last_assistant or {}).get("errorMessage") or stop)
        try:
            stats = await self._command({"type": "get_session_stats"})
        except Exception:
            log.warning("pi session stats unavailable", exc_info=True)
            return outcome
        outcome.usage = stats.get("tokens")
        outcome.cost_usd = stats.get("cost")
        outcome.session_id = stats.get("sessionId") or self.session_id
        self.session_id = outcome.session_id
        percent = (stats.get("contextUsage") or {}).get("percent")
        outcome.context_pct = float(percent) if percent is not None else None
        return outcome

    async def close(self) -> None:
        proc, self._proc = self._proc, None
        if proc is None:
            return
        with contextlib.suppress(Exception):
            assert proc.stdin is not None
            proc.stdin.close()  # pi shuts down in order when stdin closes
        try:
            await asyncio.wait_for(proc.wait(), SHUTDOWN_GRACE_S)
        except (TimeoutError, asyncio.CancelledError):
            pass
        finally:
            with contextlib.suppress(ProcessLookupError):
                os.killpg(proc.pid, signal.SIGKILL)
            _kill_tagged(self._session_tag)  # pi starts each command in its own session: killpg misses them
            await proc.wait()
            for task in self._tasks:
                task.cancel()
            await asyncio.gather(*self._tasks, return_exceptions=True)
            self._tasks = []
