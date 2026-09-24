"""Merge queue: the only path into main.

Why not let every worker self-merge (Agensh)? With hundreds of workers racing on main, most
PRs go stale before they land (about 75% never merged in the Agensh 1,024-agent replay) and
main can break between two "locally verified" merges. The queue fixes both:

* PRs land one at a time, in order; each lands only if it merges cleanly into the current main
  and, when a verify command is configured, the *merged result* passes it.
* A PR that conflicts or fails is bounced to its author with the exact reason, as a
  HIGH-priority event, instead of silently rotting.
* Branch protection makes the queue bot the only identity that can write main, so the rule is
  enforced by infrastructure, not by the prompt.
* Queue depth is exported for the launcher's backpressure.
"""

from __future__ import annotations

import asyncio
import logging
import os
import tempfile
import time
from dataclasses import dataclass
from urllib.parse import urlsplit, urlunsplit

from ..coord.store import Store
from ..protocol import Event, MergeRequest, MergeStatus, Priority
from .gitea import GiteaClient, GiteaError, PullRequest

log = logging.getLogger(__name__)


@dataclass
class VerifyResult:
    ok: bool
    output: str


class Verifier:
    async def verify(self, pr: PullRequest) -> VerifyResult:  # pragma: no cover - interface
        raise NotImplementedError


class CommandVerifier(Verifier):
    """Check out main, merge the PR head, run `command`. Runs wherever the coordination server runs.

    For heavy toolchains, point this at a dedicated integration sandbox instead (see DESIGN.md).
    """

    def __init__(self, clone_url: str, token: str, main_branch: str, command: str, timeout_s: float) -> None:
        parts = urlsplit(clone_url)
        self._url = urlunsplit(parts._replace(netloc=f"merge-bot:{token}@{parts.netloc}"))
        self._main = main_branch
        self._command = command
        self._timeout = timeout_s

    async def _run(self, *args: str, cwd: str, timeout: float = 300) -> tuple[int, str]:
        proc = await asyncio.create_subprocess_exec(
            *args, cwd=cwd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.STDOUT
        )
        try:
            out, _ = await asyncio.wait_for(proc.communicate(), timeout)
        except TimeoutError:
            proc.kill()
            return 124, f"timed out after {timeout:.0f}s: {' '.join(args[:3])}"
        return proc.returncode or 0, out.decode(errors="replace")

    async def verify(self, pr: PullRequest) -> VerifyResult:
        with tempfile.TemporaryDirectory(prefix=f"verify-pr{pr.number}-") as tmp:
            code, out = await self._run("git", "clone", "--quiet", "--branch", self._main, self._url, "repo", cwd=tmp)
            if code:
                return VerifyResult(False, f"clone failed: {out[-2000:]}")
            repo = os.path.join(tmp, "repo")
            for args in (
                ("git", "fetch", "--quiet", "origin", f"refs/pull/{pr.number}/head"),
                ("git", "-c", "user.name=merge-bot", "-c", "user.email=merge-bot@agents.invalid",
                 "merge", "--no-edit", "--quiet", "FETCH_HEAD"),
            ):
                code, out = await self._run(*args, cwd=repo)
                if code:
                    return VerifyResult(False, f"{' '.join(args[:2])} failed: {out[-2000:]}")
            code, out = await self._run("bash", "-lc", self._command, cwd=repo, timeout=self._timeout)
            return VerifyResult(code == 0, out[-4000:])


class MergeQueue:
    def __init__(
        self,
        store: Store,
        gitea: GiteaClient,
        owner: str,
        repo: str,
        main_branch: str = "main",
        verifier: Verifier | None = None,
        poll_s: float = 2.0,
        mergeable_retries: int = 3,
        retry_delay_s: float = 3.0,
    ) -> None:
        self.store = store
        self.gitea = gitea
        self.owner = owner
        self.repo = repo
        self.main = main_branch
        self.verifier = verifier
        self.poll_s = poll_s
        self.mergeable_retries = mergeable_retries
        self.retry_delay_s = retry_delay_s
        self._wake = asyncio.Event()

    def poke(self) -> None:
        self._wake.set()

    async def run(self, stop: asyncio.Event) -> None:
        while not stop.is_set():
            req = await self.store.merge_next()
            if req is None:
                self._wake.clear()
                try:
                    await asyncio.wait_for(self._wake.wait(), self.poll_s)
                except TimeoutError:
                    pass
                continue
            try:
                await self.process(req)
            except Exception as exc:  # keep the queue moving; the author gets the reason
                log.exception("merge queue failed on PR #%s", req.pr_number)
                await self._bounce(req, MergeStatus.FAILED, f"merge queue error: {exc}")

    async def process(self, req: MergeRequest) -> MergeRequest:
        started = time.time()
        pr = await self.gitea.get_pr(self.owner, self.repo, req.pr_number)
        if pr.merged:
            return await self.store.merge_update(req.id, MergeStatus.MERGED, "already merged", pr.head_sha)
        if pr.state != "open":
            return await self.store.merge_update(req.id, MergeStatus.CANCELLED, "PR is closed", pr.head_sha)
        if pr.base_ref != self.main:
            return await self._bounce(req, MergeStatus.FAILED, f"PR must target {self.main}, not {pr.base_ref}")
        files = await self.gitea.pr_files(self.owner, self.repo, pr.number)
        # Gitea checks mergeability asynchronously after a push and reports `mergeable=false`
        # while the check is running, so re-read before calling it a conflict.
        for _ in range(self.mergeable_retries):
            if pr.mergeable:
                break
            await asyncio.sleep(self.retry_delay_s)
            pr = await self.gitea.get_pr(self.owner, self.repo, req.pr_number)
        if not pr.mergeable:
            await self.store.record_file_heat(files, conflicted=True)
            return await self._bounce(
                req,
                MergeStatus.CONFLICT,
                f"PR #{pr.number} conflicts with {self.main}. Merge the latest {self.main} into "
                f"{pr.head_ref}, re-run your checks, push, then call merge_request again. "
                f"Files in this PR: {', '.join(files[:15])}",
                pr.head_sha,
            )
        if self.verifier is not None:
            result = await self.verifier.verify(pr)
            await self.store.trace(req.worker, "merge_verify", {"pr": pr.number, "ok": result.ok})
            if not result.ok:
                return await self._bounce(
                    req,
                    MergeStatus.FAILED,
                    f"PR #{pr.number} merged with current {self.main} fails verification:\n{result.output[-1500:]}",
                    pr.head_sha,
                )
        # 405 ("try again later" while Gitea re-checks mergeability after main moved) and
        # 409 ("cannot lock ref" or a moved head) are often transient: re-read and retry first.
        for attempt in range(self.mergeable_retries + 1):
            try:
                await self.gitea.merge_pr(self.owner, self.repo, pr.number, pr.head_sha, title=pr.title)
                break
            except GiteaError as e:
                if e.status not in (405, 409):
                    raise
                fresh = await self.gitea.get_pr(self.owner, self.repo, pr.number)
                if attempt == self.mergeable_retries or fresh.head_sha != pr.head_sha or (
                    e.status == 405 and not fresh.mergeable and attempt > 0
                ):
                    return await self._bounce(
                        req,
                        MergeStatus.CONFLICT,
                        f"PR #{pr.number} could not land ({e}). Merge the latest {self.main} into "
                        f"{pr.head_ref}, re-verify, push, and resubmit.",
                        pr.head_sha,
                    )
                await asyncio.sleep(self.retry_delay_s)
        await self.store.record_file_heat(files, merged=True)
        done = await self.store.merge_update(req.id, MergeStatus.MERGED, f"landed in {time.time() - started:.0f}s", pr.head_sha)
        await self.store.enqueue(
            req.worker,
            Event(
                event_id=f"merge-{req.id}-merged",
                source="merge_queue",
                kind="merged",
                priority=Priority.LOW,
                observed_at=time.time(),
                summary=f"PR #{pr.number} landed on {self.main}. Write a PATCH_SUMMARY and release its claim.",
                payload={"pr": pr.number},
            ),
        )
        return done

    async def _bounce(
        self, req: MergeRequest, status: MergeStatus, reason: str, head_sha: str | None = None
    ) -> MergeRequest:
        updated = await self.store.merge_update(req.id, status, reason, head_sha)
        await self.store.enqueue(
            req.worker,
            Event(
                event_id=f"merge-{req.id}-{status.value}",
                source="merge_queue",
                kind=f"merge_{status.value}",
                priority=Priority.HIGH,
                observed_at=time.time(),
                summary=reason,
                payload={"pr": req.pr_number},
            ),
        )
        try:
            await self.gitea.comment(self.owner, self.repo, req.pr_number, f"merge queue: {status.value}\n\n{reason}")
        except GiteaError:
            log.warning("could not comment on PR #%s", req.pr_number)
        return updated
