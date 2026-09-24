"""Scripted harness: a deterministic stand-in for an LLM worker, for infrastructure tests.

It drives the real infrastructure exactly like a model would (MCP tools on the coordination
server, git against Gitea, the Gitea REST API for PRs), one step per turn:

    claim -> build on a branch -> push + open PR -> merge_request
          -> (bounced? merge main, resolve, push, resubmit) -> merged -> PATCH_SUMMARY -> release

Each feature adds its own module (no conflict) and appends one line to a shared REGISTRY.md
(a deliberate hotspot), so concurrent workers hit real merge conflicts and exercise the
merge-queue bounce path. Claims include REGISTRY.md, so overlaps and DMs happen too.
"""

from __future__ import annotations

import logging
import os
import re
import subprocess
from pathlib import Path
from urllib.parse import urlsplit

import httpx
import httpx2
from mcp import Client
from mcp.client.streamable_http import streamable_http_client

from .base import HarnessAdapter, TurnOutcome

log = logging.getLogger(__name__)
REGISTRY = "REGISTRY.md"


class ScriptedAdapter(HarnessAdapter):
    def __init__(self, coord_url: str, token: str, workdir: str, features: int = 3, **_: object) -> None:
        self.coord_url = coord_url.rstrip("/")
        self.token = token
        self.workdir = Path(workdir)
        self.worker = os.environ.get("SA_WORKER_WORKER_ID", "worker")
        self.features = int(os.environ.get("SA_SCRIPTED_FEATURES", features))
        self.repo_url = os.environ.get("GITEA_REPO_URL", "")
        self.gitea_token = os.environ.get("GITEA_TOKEN", "")
        self.main = os.environ.get("SA_MAIN_BRANCH", "main")
        self.n = 0
        self.state = "idle"
        self.claim_id: int | None = None
        self.branch = ""
        self.pr: int | None = None
        self.calls = 0
        self.tools: dict[str, int] = {}

    # ------------------------------------------------------------------ helpers

    async def _mcp(self, tool: str, args: dict) -> str:
        self.calls += 1
        self.tools[tool] = self.tools.get(tool, 0) + 1
        http = httpx2.AsyncClient(headers={"Authorization": f"Bearer {self.token}"}, timeout=60)
        async with Client(streamable_http_client(f"{self.coord_url}/mcp", http_client=http)) as client:
            result = await client.call_tool(tool, args)
        return "\n".join(getattr(c, "text", "") for c in result.content)

    def _git(self, *args: str, check: bool = True) -> subprocess.CompletedProcess:
        self.calls += 1
        self.tools["git"] = self.tools.get("git", 0) + 1
        return subprocess.run(["git", *args], cwd=self.workdir, check=check, capture_output=True, text=True)

    def _repo_api(self) -> tuple[str, str]:
        parts = urlsplit(self.repo_url)
        owner, repo = parts.path.strip("/").removesuffix(".git").split("/")[-2:]
        return f"{parts.scheme}://{parts.netloc}/api/v1/repos/{owner}/{repo}", owner

    async def _open_pr(self) -> int:
        base, _ = self._repo_api()
        async with httpx.AsyncClient(headers={"Authorization": f"token {self.gitea_token}"}, timeout=30) as http:
            resp = await http.post(
                f"{base}/pulls",
                json={"head": self.branch, "base": self.main, "title": f"{self.worker}: feature {self.n}",
                      "body": f"Adds features/{self.worker}_{self.n}.py and registers it."},
            )
            resp.raise_for_status()
            self.calls += 1
            return int(resp.json()["number"])

    def _resolve_registry_conflict(self) -> None:
        """Union-resolve REGISTRY.md: keep every line from both sides (it is append-only)."""
        path = self.workdir / REGISTRY
        text = path.read_text()
        kept = [ln for ln in text.splitlines() if not re.match(r"^(<<<<<<<|=======|>>>>>>>)", ln)]
        seen: dict[str, None] = {}
        for ln in kept:
            seen.setdefault(ln, None)
        path.write_text("\n".join(seen) + "\n")
        self._git("add", REGISTRY)
        self._git("commit", "--no-edit", "-m", f"{self.worker}: merge {self.main}, union {REGISTRY}")

    # ------------------------------------------------------------------- steps

    async def start(self, system_prompt: str, resume: str | None = None) -> None:
        self.session_id = resume or f"scripted-{self.worker}"

    async def close(self) -> None:
        return None

    async def run_turn(self, prompt: str) -> TurnOutcome:
        self.calls, self.tools = 0, {}
        try:
            await self._step(prompt)
            return TurnOutcome(ok=True, tool_calls=self.calls, tools=dict(self.tools), session_id=self.session_id)
        except Exception as e:
            log.exception("scripted step failed in state %s", self.state)
            return TurnOutcome(ok=False, tool_calls=self.calls, tools=dict(self.tools), error=repr(e), session_id=self.session_id)

    async def _step(self, prompt: str) -> None:
        if "kind=shutdown" in prompt:
            return
        if self.state == "idle":
            if self.n >= self.features:
                await self._mcp("board_read", {"limit": 5})  # done: stay responsive to peers
                return
            self.n += 1
            scope = [f"features/{self.worker}_{self.n}.py", REGISTRY]
            out = await self._mcp("claim", {"scope": scope, "intent": f"feature {self.n} of {self.worker}"})
            m = re.search(r"claimed claim#(\d+)", out)
            self.claim_id = int(m.group(1)) if m else None
            for holder in sorted(set(re.findall(r"claim#\d+ (\S+) \{", out.split("WARNING", 1)[-1]))):
                if holder != self.worker and "WARNING" in out:
                    await self._mcp("send_dm", {"to": holder, "text": f"I only append one line to {REGISTRY}; union-merge it."})
                    break
            self.state = "building"
            return
        if self.state == "building":
            self._git("fetch", "origin", self.main)
            self.branch = f"{self.worker}/feature-{self.n}"
            self._git("checkout", "-B", self.branch, f"origin/{self.main}")
            feat = self.workdir / "features" / f"{self.worker}_{self.n}.py"
            feat.parent.mkdir(exist_ok=True)
            feat.write_text(f'NAME = "{self.worker}-{self.n}"\n\n\ndef run() -> str:\n    return NAME\n')
            with (self.workdir / REGISTRY).open("a") as fh:
                fh.write(f"- {self.worker}_{self.n}: features/{self.worker}_{self.n}.py\n")
            self._git("add", "-A")
            self._git("commit", "-m", f"{self.worker}: feature {self.n}")
            self._git("push", "-u", "origin", self.branch, "--force-with-lease")  # own branch only
            await self._mcp("board_write", {"type": "OBSERVED", "text": f"feature {self.n} of {self.worker} builds; PR next"})
            self.pr = await self._open_pr()
            await self._mcp("merge_request", {"pr_number": self.pr})
            self.state = "queued"
            return
        if self.state == "queued":
            status = await self._mcp("merge_status", {})
            mine = re.search(rf"PR #{self.pr}: (\w+)", status)
            st = mine.group(1) if mine else ""
            if st == "merged":
                await self._mcp(
                    "board_write",
                    {"type": "PATCH_SUMMARY", "text": f"files=features/{self.worker}_{self.n}.py,{REGISTRY} | idea=feature {self.n} | evidence=merged PR #{self.pr}"},
                )
                if self.claim_id:
                    await self._mcp("release_claim", {"claim_id": self.claim_id, "outcome": "done"})
                self.state = "idle"
            elif st in ("conflict", "failed"):
                self._git("fetch", "origin", self.main)
                merged = self._git("merge", "--no-edit", f"origin/{self.main}", check=False)
                if merged.returncode != 0:
                    self._resolve_registry_conflict()
                self._git("push", "origin", self.branch)
                await self._mcp("board_write", {"type": "FACT", "text": f"{REGISTRY} conflicts are append-only: union-merge them"})
                await self._mcp("merge_request", {"pr_number": self.pr})
            # queued/testing: nothing to do this turn
