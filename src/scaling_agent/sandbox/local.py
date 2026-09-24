"""Local provider: each worker runtime is a subprocess with its own working directory.

For development and small smoke runs. There is no isolation between workers beyond separate
directories, so only use it with trusted tasks.
"""

from __future__ import annotations

import asyncio
import os
import sys
from pathlib import Path

from ..config import ProviderSettings
from .base import SandboxHandle, SandboxProvider


class LocalProvider(SandboxProvider):
    def __init__(self, settings: ProviderSettings, run_name: str) -> None:
        self.root = Path(settings.local_root).resolve() / run_name
        self._procs: dict[str, asyncio.subprocess.Process] = {}

    async def prepare(self) -> None:
        self.root.mkdir(parents=True, exist_ok=True)

    async def _spawn(self, worker_id: str, env: dict[str, str]) -> asyncio.subprocess.Process:
        home = self.root / worker_id
        workdir = home / "workspace"
        workdir.mkdir(parents=True, exist_ok=True)
        # Only an allowlist of this machine's environment: the launcher's own secrets (coordination
        # admin token, Gitea admin password, cloud keys) must never reach a worker. Model credentials
        # arrive through `env` (the run's forward_env). Each worker gets its own HOME.
        allow = ("PATH", "LANG", "LC_ALL", "LC_CTYPE", "TERM", "TMPDIR", "TZ", "VIRTUAL_ENV", "PYTHONPATH")
        inherited = {k: v for k, v in os.environ.items() if k in allow}
        (home / "home").mkdir(parents=True, exist_ok=True)
        full_env = {
            **inherited,
            **env,
            "HOME": str(home / "home"),
            "SA_WORKER_WORKDIR": str(workdir),
            "SA_WORKER_STATE_DIR": str(home / "state"),
        }
        log = open(home / "runtime.log", "ab")  # noqa: SIM115 - handed to the child process
        return await asyncio.create_subprocess_exec(
            sys.executable, "-m", "scaling_agent.cli", "worker", "run",
            cwd=str(workdir), env=full_env, stdout=log, stderr=log,
        )

    async def start(self, worker_id: str, env: dict[str, str]) -> SandboxHandle:
        self._procs[worker_id] = await self._spawn(worker_id, env)
        return SandboxHandle(worker_id=worker_id, sandbox_id=f"local-{worker_id}", meta={"pid": str(self._procs[worker_id].pid)})

    async def runtime_alive(self, handle: SandboxHandle) -> bool | None:
        proc = self._procs.get(handle.worker_id)
        return proc is not None and proc.returncode is None

    async def relaunch(self, handle: SandboxHandle, env: dict[str, str]) -> None:
        self._procs[handle.worker_id] = await self._spawn(handle.worker_id, env)

    async def stop(self, handle: SandboxHandle) -> None:
        proc = self._procs.pop(handle.worker_id, None)
        if proc and proc.returncode is None:
            proc.terminate()
            try:
                await asyncio.wait_for(proc.wait(), 20)
            except TimeoutError:
                proc.kill()
