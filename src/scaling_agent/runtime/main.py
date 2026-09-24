"""Worker process entry point (`scaling-agent worker run`), executed inside the sandbox."""

from __future__ import annotations

import asyncio
import logging
import os
import signal
import subprocess
from pathlib import Path
from urllib.parse import urlsplit

import httpx

from ..config import RuntimeSettings
from .adapters import build_adapter
from .client import CoordClient
from .worker import WorkerRuntime

log = logging.getLogger("scaling_agent.worker")


def _git(*args: str, cwd: str | None = None) -> None:
    subprocess.run(["git", *args], cwd=cwd, check=True, capture_output=True, text=True)


def bootstrap_git(settings: RuntimeSettings) -> None:
    """Configure identity and credentials, then clone the shared repository if needed."""
    repo_url = os.environ.get("GITEA_REPO_URL")
    token = os.environ.get("GITEA_TOKEN")
    _git("config", "--global", "user.name", settings.worker_id)
    _git("config", "--global", "user.email", f"{settings.worker_id}@agents.invalid")
    _git("config", "--global", "pull.rebase", "false")
    if not repo_url:
        log.warning("GITEA_REPO_URL not set; skipping clone")
        return
    if token:
        parts = urlsplit(repo_url)
        cred_file = Path.home() / ".git-credentials"
        cred_file.write_text(f"{parts.scheme}://{settings.worker_id}:{token}@{parts.netloc}\n")
        cred_file.chmod(0o600)
        _git("config", "--global", "credential.helper", "store")
    workdir = Path(settings.workdir)
    if not (workdir / ".git").exists():
        workdir.mkdir(parents=True, exist_ok=True)
        if any(p for p in workdir.iterdir() if p.name != ".scaling-agent"):
            log.warning("%s is not empty and not a git repo; cloning into it anyway may fail", workdir)
        _git("clone", repo_url, ".", cwd=str(workdir))


async def fetch_prompts(settings: RuntimeSettings) -> tuple[str, str]:
    async with httpx.AsyncClient(base_url=settings.coord_url.rstrip("/"), timeout=30) as http:
        resp = await http.get("/api/prompt", headers={"Authorization": f"Bearer {settings.token}"})
        resp.raise_for_status()
        data = resp.json()
    return data["system_prompt"], data["protocol_card"]


async def amain() -> None:
    settings = RuntimeSettings()  # from SA_WORKER_* environment variables
    Path(settings.state_dir).mkdir(parents=True, exist_ok=True)
    await asyncio.to_thread(bootstrap_git, settings)
    system_prompt, protocol_card = await fetch_prompts(settings)
    coord = CoordClient(settings.coord_url, settings.token)
    forwarded = {k: v for k, v in os.environ.items() if k.startswith(("ANTHROPIC_", "CLAUDE_", "GITEA_"))}
    harness = build_adapter(
        settings.harness,
        coord_url=settings.coord_url,
        token=settings.token,
        workdir=settings.workdir,
        drain=coord.drain,
        model=settings.model,
        env=forwarded,
    )
    runtime = WorkerRuntime(
        worker_id=settings.worker_id,
        coord=coord,
        harness=harness,
        system_prompt=system_prompt,
        protocol_card=protocol_card,
        state_dir=settings.state_dir,
        continue_after_s=settings.continue_after_s,
        board_reminder=settings.board_reminder,
        rotate_after_turns=settings.rotate_after_turns,
        rotate_at_context_pct=settings.rotate_at_context_pct,
        max_turn_s=settings.max_turn_s,
    )
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, runtime.stop)
    try:
        await runtime.run()
    finally:
        await coord.close()


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    asyncio.run(amain())
