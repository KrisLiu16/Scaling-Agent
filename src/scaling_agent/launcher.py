"""Launcher: provisions workers on a schedule and supervises the run. It is not an orchestrator:
it never assigns work, it only decides *how many* equal workers exist and keeps them alive.

Schedule (improving on Agensh's fixed 30s/3s stagger):
* Founding phase: the first `founders` workers start alone and get `founding_s` to set up the
  skeleton, build scripts, and module interfaces. Later workers join an organization that
  already has a structure, instead of hundreds of workers racing to create the same files.
* Staggered ramp-up per `stagger` phases (default: one per 30s for the first hour, then one per 3s).
* Backpressure: ramp-up pauses while the merge queue is deeper than `max_queue_per_worker` per
  active worker. Adding workers when integration is the bottleneck only adds stale PRs.
* Supervision: dead runtimes are relaunched, time-limited sandboxes kept alive.
* Reminders at T-45min / T-5min, then a shutdown event and sandbox teardown at T.
"""

from __future__ import annotations

import asyncio
import logging
import os
import secrets
import time
from collections.abc import Awaitable, Callable
from pathlib import Path
from typing import Any

import httpx

from .config import RunConfig
from .prompts import template_text
from .sandbox.base import SandboxHandle, SandboxProvider
from .workspace.gitea import GiteaClient

log = logging.getLogger(__name__)


def schedule_offsets(run: RunConfig) -> list[float]:
    """Start offset (seconds from launch) for each worker, in worker order."""
    offsets: list[float] = []
    t = 0.0
    phases = run.stagger or []
    for i in range(run.workers):
        if i == run.founders and run.founders > 0:
            t = max(t, run.founding_s)
        offsets.append(t)
        interval = phases[-1].interval_s if phases else 0.0
        for phase in phases:
            if phase.until_s is None or t < phase.until_s:
                interval = phase.interval_s
                break
        t += interval
    return offsets


class Launcher:
    def __init__(
        self,
        run: RunConfig,
        provider: SandboxProvider,
        admin_token: str,
        gitea_admin: GiteaClient | None = None,
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self.run = run
        self.provider = provider
        self.gitea = gitea_admin
        self._http = httpx.AsyncClient(
            base_url=run.coord_url.rstrip("/"), headers={"Authorization": f"Bearer {admin_token}"}, timeout=60
        )
        self._sleep = sleep
        self._clock = clock
        self.handles: dict[str, SandboxHandle] = {}
        self.envs: dict[str, dict[str, str]] = {}
        self.restarts: dict[str, int] = {}
        self.unstarted: set[str] = set()
        self.launch_id = f"{run.name}-{int(time.time())}"
        self._t0 = 0.0

    # ------------------------------------------------------------------ setup

    async def _admin(self, method: str, path: str, **kw: Any) -> Any:
        resp = await self._http.request(method, path, **kw)
        resp.raise_for_status()
        return resp.json()

    async def setup(self) -> None:
        ids = self.run.worker_ids()
        coord_tokens = (await self._admin("POST", "/api/admin/workers", json={"ids": ids}))["tokens"]
        task = Path(self.run.task_file).read_text()
        g = self.run.gitea
        repo_url = f"{g.worker_url.rstrip('/')}/{g.owner}/{g.repo}.git"
        gitea_tokens: dict[str, str] = {}
        if self.gitea is not None:
            await self._wait_for_repo()
            await self.gitea.ensure_task_issue(g.owner, g.repo, f"Task: {self.run.name}", task)
            for wid in ids:
                await self.gitea.ensure_user(wid, secrets.token_urlsafe(24))
                await self.gitea.add_collaborator(g.owner, g.repo, wid, "write")
                # Each worker owns the `<wid>/*` branch namespace: peers cannot push to, force-push,
                # or delete its branches (the merge bot can, to land them).
                await self.gitea.protect_branch(g.owner, g.repo, f"{wid}/*", g.bot_user, pushers=[wid, g.bot_user])
                gitea_tokens[wid] = await self.gitea.create_token(
                    wid, f"{self.run.name}-{secrets.token_hex(4)}", ["write:repository", "write:issue", "read:user"]
                )
        verification = "run the checks described in the task, and compare against the reference behaviour"
        await self._admin(
            "PUT",
            "/api/admin/prompt",
            json={
                "system_template": template_text("worker_prompt.md.j2"),
                "card_template": template_text("protocol_card.md.j2"),
                "values": {
                    "n_workers": self.run.workers,
                    "repo_url": repo_url,
                    "main_branch": g.main_branch,
                    "verification": verification,
                    "task": task,
                },
            },
        )
        forwarded = {name: os.environ[name] for name in self.run.forward_env if os.environ.get(name)}
        coord_public = self.run.coord_public_url or self.run.coord_url
        for wid in ids:
            env = {
                "SA_WORKER_WORKER_ID": wid,
                "SA_WORKER_COORD_URL": coord_public,
                "SA_WORKER_TOKEN": coord_tokens[wid],
                "SA_WORKER_HARNESS": self.run.harness,
                "GITEA_URL": g.worker_url,
                "GITEA_REPO_URL": repo_url,
                "SA_MAIN_BRANCH": g.main_branch,
                **self.run.worker_env,
                **forwarded,
            }
            if self.run.model:
                env["SA_WORKER_MODEL"] = self.run.model
            if wid in gitea_tokens:
                env["GITEA_TOKEN"] = gitea_tokens[wid]
            self.envs[wid] = env
        await self.provider.prepare()

    async def _wait_for_repo(self, timeout_s: float = 300.0) -> None:
        """The coordination server bootstraps the repository; wait for it instead of racing it."""
        from .workspace.gitea import GiteaError

        g = self.run.gitea
        deadline = self._clock() + timeout_s
        while True:
            try:
                await self.gitea._request("GET", f"/repos/{g.owner}/{g.repo}")
                return
            except (GiteaError, httpx.HTTPError) as e:
                if self._clock() > deadline:
                    raise RuntimeError(f"repository {g.owner}/{g.repo} did not appear: {e}") from e
                log.info("waiting for %s/%s to be bootstrapped by the coordination server", g.owner, g.repo)
                await self._sleep(5)

    # -------------------------------------------------------------------- run

    def elapsed(self) -> float:
        return self._clock() - self._t0

    async def _wait_until(self, offset: float) -> None:
        while (delay := offset - self.elapsed()) > 0:
            await self._sleep(min(delay, 30.0))

    async def _backpressure(self) -> bool:
        try:
            status = await self._admin("GET", "/api/admin/status")
        except httpx.HTTPError:
            return False
        depth = status.get("merge_queue", {}).get("depth", 0)
        return depth > self.run.max_queue_per_worker * max(1, len(self.handles))

    async def _start(self, wid: str) -> bool:
        try:
            self.handles[wid] = await self.provider.start(wid, self.envs[wid])
        except Exception:
            log.exception("failed to start %s; supervision will retry", wid)
            self.unstarted.add(wid)
            return False
        self.unstarted.discard(wid)
        log.info("started %s (%d/%d) at T+%.0fs", wid, len(self.handles), self.run.workers, self.elapsed())
        return True

    async def ramp(self) -> None:
        for wid, offset in zip(self.run.worker_ids(), schedule_offsets(self.run), strict=True):
            await self._wait_until(offset)
            while self.elapsed() < self.run.duration_s and await self._backpressure():
                log.info("ramp-up paused: merge queue backlog (%d workers active)", len(self.handles))
                await self._sleep(30)
            if self.elapsed() >= self.run.duration_s:
                return
            await self._start(wid)

    async def supervise(self, interval_s: float = 60.0, keepalive_every_s: float = 3600.0, max_restarts: int = 5) -> None:
        """Keep the organization alive: retry failed starts, relaunch dead runtimes, extend sandboxes.

        A runtime seen healthy for a full interval gets its restart budget back, so a short outage
        of the coordination server does not use up the budget of the whole fleet. An unknown state
        (the provider could not check) is never treated as dead, to avoid double runtimes.
        """
        last_keepalive = self.elapsed()
        while self.elapsed() < self.run.duration_s:
            await self._sleep(interval_s)
            for wid in sorted(self.unstarted):
                await self._start(wid)
            do_keepalive = self.elapsed() - last_keepalive >= keepalive_every_s
            for wid, handle in list(self.handles.items()):
                try:
                    if do_keepalive:
                        await self.provider.keepalive(handle)
                    alive = await self.provider.runtime_alive(handle)
                    if alive is None:
                        continue
                    if alive:
                        self.restarts.pop(wid, None)
                        continue
                    if self.restarts.get(wid, 0) >= max_restarts:
                        log.error("%s keeps dying; giving up after %d relaunches", wid, max_restarts)
                        continue
                    self.restarts[wid] = self.restarts.get(wid, 0) + 1
                    log.warning("%s runtime is down; relaunch #%d", wid, self.restarts[wid])
                    await self.provider.relaunch(handle, self.envs[wid])
                except Exception:
                    log.exception("supervision failed for %s", wid)
            if do_keepalive:
                last_keepalive = self.elapsed()

    async def _broadcast(self, text: str, key: str, kind: str = "announcement") -> None:
        try:
            await self._admin(
                "POST",
                "/api/admin/broadcast",
                # Keys are unique per launch: event ids are deduplicated forever per worker, so a
                # fixed key would silently drop the reminders of a second run on the same server.
                json={"text": text, "urgent": True, "kind": kind, "key": f"{self.launch_id}-{key}"},
            )
        except Exception:
            log.exception("broadcast %s failed", key)

    async def reminders(self) -> None:
        for reminder in sorted(self.run.reminders, key=lambda r: -r.before_end_s):
            await self._wait_until(self.run.duration_s - reminder.before_end_s)
            await self._broadcast(reminder.text, f"reminder-{reminder.before_end_s:.0f}")

    async def finish(self, grace_s: float = 60.0) -> None:
        await self._broadcast("Time is up. Stop now.", "shutdown", kind="shutdown")
        await self._sleep(grace_s)
        for handle in self.handles.values():
            try:
                await self.provider.stop(handle)
            except Exception:
                log.exception("failed to stop %s", handle.worker_id)

    async def launch(self) -> None:
        await self.setup()
        self._t0 = self._clock()
        tasks = [asyncio.create_task(c) for c in (self.ramp(), self.supervise(), self.reminders())]
        try:
            await self._wait_until(self.run.duration_s)
        finally:
            for t in tasks:
                t.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            try:
                await self.finish()
            finally:
                try:
                    await self._http.aclose()
                finally:
                    await self.provider.close()
