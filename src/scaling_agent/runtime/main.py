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
    """Clone the shared repository and configure identity and credentials *for this checkout only*.

    Nothing global is touched: several local workers can share one HOME (and a developer's own
    ~/.gitconfig) without overwriting each other's identity or tokens.
    """
    repo_url = os.environ.get("GITEA_REPO_URL")
    token = os.environ.get("GITEA_TOKEN")
    if not repo_url:
        log.warning("GITEA_REPO_URL not set; skipping clone")
        return
    state = Path(settings.state_dir)
    state.mkdir(parents=True, exist_ok=True)
    helper: list[str] = []
    if token:
        parts = urlsplit(repo_url)
        cred_file = state / "git-credentials"
        cred_file.write_text(f"{parts.scheme}://{settings.worker_id}:{token}@{parts.netloc}\n")
        cred_file.chmod(0o600)
        helper = [f"store --file={cred_file}"]
    workdir = Path(settings.workdir)
    if not (workdir / ".git").exists():
        workdir.mkdir(parents=True, exist_ok=True)
        if any(workdir.iterdir()):
            raise RuntimeError(f"{workdir} is not empty and not a git checkout; keep state_dir outside workdir")
        clone = ["-c", f"credential.helper={helper[0]}"] if helper else []
        _git(*clone, "clone", repo_url, ".", cwd=str(workdir))
    local = str(workdir)
    _git("config", "user.name", settings.worker_id, cwd=local)
    _git("config", "user.email", f"{settings.worker_id}@agents.invalid", cwd=local)
    _git("config", "pull.rebase", "false", cwd=local)
    if helper:
        _git("config", "credential.helper", helper[0], cwd=local)


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
    forwarded = {k: v for k, v in os.environ.items() if k.startswith(("ANTHROPIC_", "GITEA_"))}
    # Keep the harness's transcripts next to the runtime state, so a restarted runtime can resume them.
    forwarded["CLAUDE_CONFIG_DIR"] = str(Path(settings.state_dir) / "claude")
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
        max_continue_after_s=settings.max_continue_after_s,
        board_reminder=settings.board_reminder,
        rotate_after_turns=settings.rotate_after_turns,
        rotate_at_context_pct=settings.rotate_at_context_pct,
        max_turn_s=settings.max_turn_s,
    )
    loop = asyncio.get_running_loop()
    main_task = asyncio.current_task()

    def on_signal() -> None:
        # Stop between turns, and also interrupt an in-flight turn or long-poll so the harness
        # subprocess is shut down before the sandbox or pod is killed.
        runtime.stop()
        if main_task is not None:
            main_task.cancel()

    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, on_signal)
    try:
        await runtime.run()
    except asyncio.CancelledError:
        log.info("%s: stopped by signal", settings.worker_id)
    finally:
        await coord.close()


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    asyncio.run(amain())
