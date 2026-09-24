"""Fast, exact conflict detection for the merge queue.

Gitea computes a PR's `mergeable` flag asynchronously and re-checks every open PR after each push
to main, so right after a merge the flag says "not mergeable" for PRs that are fine. Waiting it out
stalls a serial queue; trusting it bounces clean PRs. Instead the queue asks git directly: a bare
mirror is fetched (main + the PR head) and `git merge-tree --write-tree` reports, in ~100ms and
without a worktree, whether the merge is clean and which files conflict. No PR code runs here.
"""

from __future__ import annotations

import asyncio
import os
from dataclasses import dataclass, field
from urllib.parse import urlsplit, urlunsplit


@dataclass
class ConflictReport:
    clean: bool
    files: list[str] = field(default_factory=list)
    head_found: bool = True


class GitConflictChecker:
    def __init__(self, mirror_dir: str, clone_url: str, token: str, main_branch: str = "main") -> None:
        parts = urlsplit(clone_url)
        self._url = urlunsplit(parts._replace(netloc=f"merge-bot:{token}@{parts.netloc}"))
        self._dir = mirror_dir
        self._main = main_branch
        self._lock = asyncio.Lock()
        self._env = {"PATH": os.environ.get("PATH", "/usr/bin:/bin"), "HOME": mirror_dir, "GIT_TERMINAL_PROMPT": "0"}

    async def _git(self, *args: str) -> tuple[int, str]:
        proc = await asyncio.create_subprocess_exec(
            "git", "-C", self._dir, *args, env=self._env,
            stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.STDOUT,
        )
        out, _ = await asyncio.wait_for(proc.communicate(), 300)
        return proc.returncode or 0, out.decode(errors="replace")

    async def _ensure_mirror(self) -> None:
        if not os.path.exists(os.path.join(self._dir, "HEAD")):
            os.makedirs(self._dir, exist_ok=True)
            os.chmod(self._dir, 0o700)
            code, out = await self._git("init", "--quiet", "--bare")
            if code:
                raise RuntimeError(f"git init failed: {out}")

    async def check(self, pr_number: int, head_sha: str) -> ConflictReport:
        async with self._lock:
            await self._ensure_mirror()
            ref = f"refs/pr/{pr_number}"
            # refs/pull/N/head can trail a push by ~100ms: re-fetch briefly until the SHA shows up.
            for attempt in range(6):
                code, out = await self._git(
                    "fetch", "--quiet", "--force", self._url,
                    f"+refs/heads/{self._main}:refs/heads/{self._main}", f"+refs/pull/{pr_number}/head:{ref}",
                )
                if code:
                    raise RuntimeError(f"git fetch failed: {out.replace(self._url, '<remote>')[-500:]}")
                _, got = await self._git("rev-parse", ref)
                if got.strip() == head_sha:
                    break
                await asyncio.sleep(0.3 * (attempt + 1))
            else:
                return ConflictReport(clean=False, head_found=False)
            code, out = await self._git(
                "merge-tree", "--write-tree", "--name-only", "--no-messages", f"refs/heads/{self._main}", head_sha
            )
            if code == 0:
                return ConflictReport(clean=True)
            if code == 1:  # conflicts: first line is the tree id, then the conflicting paths
                return ConflictReport(clean=False, files=[ln for ln in out.splitlines()[1:] if ln.strip()])
            raise RuntimeError(f"git merge-tree failed ({code}): {out[-500:]}")
