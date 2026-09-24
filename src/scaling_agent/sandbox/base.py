"""Sandbox provider interface: where worker runtimes live.

The launcher owns provisioning; credentials for the cloud provider never enter a sandbox.
A worker sandbox gets only: its worker id, the coordination server URL, its own coordination
token, its own Gitea token, and the model API credentials listed in `forward_env`.
"""

from __future__ import annotations

import abc
from dataclasses import dataclass, field


@dataclass
class SandboxHandle:
    worker_id: str
    sandbox_id: str
    meta: dict[str, str] = field(default_factory=dict)


class SandboxProvider(abc.ABC):
    @abc.abstractmethod
    async def prepare(self) -> None:
        """One-time setup per run (e.g. ensure the AGS sandbox tool/template exists)."""

    @abc.abstractmethod
    async def start(self, worker_id: str, env: dict[str, str]) -> SandboxHandle:
        """Start (or re-attach to) the sandbox for `worker_id` and launch the worker runtime in it."""

    @abc.abstractmethod
    async def runtime_alive(self, handle: SandboxHandle) -> bool:
        """Is the worker runtime process still running inside the sandbox?"""

    @abc.abstractmethod
    async def relaunch(self, handle: SandboxHandle, env: dict[str, str]) -> None:
        """Start the worker runtime again inside an existing sandbox (after a crash)."""

    async def keepalive(self, handle: SandboxHandle) -> None:
        """Extend the sandbox lifetime if the provider reclaims idle or expired sandboxes."""

    @abc.abstractmethod
    async def stop(self, handle: SandboxHandle) -> None:
        """Stop the sandbox."""

    async def close(self) -> None:
        """Release provider resources."""
