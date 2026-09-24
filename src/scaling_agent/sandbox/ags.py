"""Tencent Cloud AGS provider: one persistent sandbox per worker, runtime launched through envd.

Control plane: `AgsControlPlane` (Cloud API, AK/SK held by the launcher only).
Data plane: E2B-compatible envd at `https://49983-{instanceId}.{region}.tencentags.com`,
authenticated with the per-instance token from `AcquireSandboxInstanceToken` in `X-Access-Token`.
No AGS API key is needed.

The worker runtime is started as a background envd process with its environment passed per
command (envd does not hand the image's ENV to child processes). The launcher's supervisor
checks it with `commands.list()` and relaunches it if it died.
"""

from __future__ import annotations

import asyncio
import logging
import os
import shlex
import time
from datetime import datetime

from ..config import ProviderSettings
from .ags_control import ENVD_PORT, AgsControlPlane, ToolSpec
from .base import SandboxHandle, SandboxProvider

log = logging.getLogger(__name__)


def _parse_expiry(value: str | None) -> float | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00")).timestamp()
    except ValueError:
        return None


class AgsProvider(SandboxProvider):
    def __init__(self, settings: ProviderSettings, run_name: str) -> None:
        if not settings.image:
            raise ValueError("provider.image is required for AGS (a linux/amd64 image containing /usr/bin/envd)")
        secret_id = os.environ.get("TENCENTCLOUD_SECRET_ID")
        secret_key = os.environ.get("TENCENTCLOUD_SECRET_KEY")
        if not secret_id or not secret_key:
            raise ValueError("set TENCENTCLOUD_SECRET_ID and TENCENTCLOUD_SECRET_KEY in the launcher environment")
        self.s = settings
        self.run_name = run_name
        self.cp = AgsControlPlane(secret_id, secret_key, settings.region, settings.endpoint)
        self.domain = settings.data_plane_domain or f"{settings.region}.tencentags.com"
        self.tool_id: str | None = None
        self._tokens: dict[str, tuple[str, float | None]] = {}

    async def prepare(self) -> None:
        spec = ToolSpec(
            name=self.s.tool_name,
            image=self.s.image or "",
            image_registry_type=self.s.image_registry_type,
            role_arn=self.s.role_arn,
            cpu=self.s.cpu,
            memory=self.s.memory,
            disk=self.s.disk,
            network_mode=self.s.network_mode,
            subnet_ids=self.s.subnet_ids,
            security_group_ids=self.s.security_group_ids,
            default_timeout=None if self.s.persistent else (self.s.instance_timeout or "24h"),
            persistent=self.s.persistent,
        )
        self.tool_id = await asyncio.to_thread(self.cp.ensure_tool, spec)
        quota = await asyncio.to_thread(self.cp.quota)
        log.info("AGS tool %s ready; quota usage/limit: %s", self.tool_id, quota)

    async def _token(self, instance_id: str) -> str:
        cached = self._tokens.get(instance_id)
        if cached and (cached[1] is None or cached[1] - time.time() > 60):
            return cached[0]
        token, expires_at = await asyncio.to_thread(self.cp.acquire_token, instance_id)
        self._tokens[instance_id] = (token, _parse_expiry(expires_at))
        return token

    async def _sandbox(self, instance_id: str):
        from e2b import AsyncSandbox
        from e2b.connection_config import ConnectionConfig
        from packaging.version import Version

        token = await self._token(instance_id)
        cfg = ConnectionConfig(
            domain=self.domain,
            request_timeout=60,
            extra_sandbox_headers={"X-Access-Token": token, "E2b-Sandbox-Id": instance_id, "E2b-Sandbox-Port": str(ENVD_PORT)},
        )
        return AsyncSandbox(
            sandbox_id=instance_id,
            sandbox_domain=self.domain,
            envd_version=Version(self.s.envd_version),
            envd_access_token=token,
            connection_config=cfg,
        )

    async def _launch_runtime(self, handle: SandboxHandle, env: dict[str, str]) -> None:
        sbx = await self._sandbox(handle.sandbox_id)
        workdir = env.get("SA_WORKER_WORKDIR", "/workspace/repo")
        state_dir = env.get("SA_WORKER_STATE_DIR", "/workspace/state")
        cmd = (
            f"mkdir -p /workspace {shlex.quote(state_dir)} && "
            f"exec {self.s.runtime_command} >> {shlex.quote(state_dir)}/runtime.log 2>&1"
        )
        proc = await sbx.commands.run(
            f"bash -lc {shlex.quote(cmd)}", background=True, envs=env, cwd="/workspace", user="root", timeout=0
        )
        handle.meta["pid"] = str(proc.pid)

    async def start(self, worker_id: str, env: dict[str, str], replaces: str | None = None) -> SandboxHandle:
        if self.tool_id is None:
            raise RuntimeError("call prepare() first")
        inst = await asyncio.to_thread(
            self.cp.start_instance, self.tool_id, worker_id, self.run_name, self.s.instance_timeout, replaces
        )
        handle = SandboxHandle(
            worker_id=worker_id,
            sandbox_id=inst.InstanceId,
            meta={"timeout_s": str(inst.TimeoutSeconds or ""), "expires_at": inst.ExpiresAt or ""},
        )
        try:
            await self._launch_runtime(handle, env)
        except Exception:
            # Keep the instance: supervision sees no runtime pid and relaunches it in place.
            log.exception("started %s but could not launch its runtime yet", inst.InstanceId)
        return handle

    async def runtime_alive(self, handle: SandboxHandle) -> bool | None:
        pid = handle.meta.get("pid")
        if not pid:
            return False
        try:
            sbx = await self._sandbox(handle.sandbox_id)
            return any(str(p.pid) == pid for p in await sbx.commands.list())
        except Exception:
            # Unknown, not dead: relaunching now could start a second runtime next to a live one.
            log.warning("could not list processes in %s", handle.sandbox_id, exc_info=True)
            self._tokens.pop(handle.sandbox_id, None)  # maybe an expired token; refresh next time
            return None

    async def relaunch(self, handle: SandboxHandle, env: dict[str, str]) -> None:
        inst = await asyncio.to_thread(self.cp.get_instance, handle.sandbox_id)
        if inst is None or inst.Status not in ("RUNNING", "PAUSED"):
            # A replacement instance needs a new idempotency token, or AGS hands back the dead one.
            new = await self.start(handle.worker_id, env, replaces=handle.sandbox_id)
            handle.sandbox_id, handle.meta = new.sandbox_id, new.meta
            return
        if inst.Status == "PAUSED":
            await asyncio.to_thread(self.cp.resume, handle.sandbox_id, self.s.instance_timeout or self.s.keepalive_timeout)
            await asyncio.to_thread(self.cp.wait_running, handle.sandbox_id)
        await self._launch_runtime(handle, env)

    async def keepalive(self, handle: SandboxHandle) -> None:
        """Time-limited instances get their reclaim clock reset; persistent ones report no timeout."""
        if not handle.meta.get("timeout_s"):
            return
        await asyncio.to_thread(self.cp.extend, handle.sandbox_id, self.s.keepalive_timeout)

    async def stop(self, handle: SandboxHandle) -> None:
        await asyncio.to_thread(self.cp.stop, handle.sandbox_id)
        self._tokens.pop(handle.sandbox_id, None)
