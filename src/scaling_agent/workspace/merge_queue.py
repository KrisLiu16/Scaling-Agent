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
import contextlib
import logging
import os
import re
import shutil
import tempfile
import time
from dataclasses import dataclass
from urllib.parse import urlsplit, urlunsplit

import httpx

from ..coord.store import Store
from ..protocol import Event, MergeRequest, MergeStatus, Priority
from .conflicts import GitConflictChecker
from .gitea import GiteaClient, GiteaError, PullRequest

log = logging.getLogger(__name__)
WIP = re.compile(r"^\s*(\[wip\]|wip:)", re.IGNORECASE)  # Gitea's default WORK_IN_PROGRESS_PREFIXES


@dataclass
class VerifyResult:
    ok: bool
    output: str


class Verifier:
    async def verify(self, pr: PullRequest) -> VerifyResult:  # pragma: no cover - interface
        raise NotImplementedError


class CommandVerifier(Verifier):
    """Check out main, merge the PR head, run `command`.

    SECURITY: this runs PR code with the coordination server's uid. It is only acceptable with
    trusted workers; anything the process can read (its database, tokens) the PR can read. The
    mitigations here (scrubbed environment, credentials removed from the checkout before the
    command runs, output returned only to the PR's author) do not make it a sandbox. For untrusted
    code, run verification in an isolated sandbox instead (see docs/DESIGN.md).
    """

    def __init__(self, clone_url: str, token: str, main_branch: str, command: str, timeout_s: float) -> None:
        parts = urlsplit(clone_url)
        self._url = urlunsplit(parts._replace(netloc=f"merge-bot:{token}@{parts.netloc}"))
        self._main = main_branch
        self._command = command
        self._timeout = timeout_s

    async def _run(self, *args: str, cwd: str, env: dict[str, str], timeout: float = 300) -> tuple[int, str]:
        proc = await asyncio.create_subprocess_exec(
            *args, cwd=cwd, env=env, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.STDOUT
        )
        try:
            out, _ = await asyncio.wait_for(proc.communicate(), timeout)
        except TimeoutError:
            proc.kill()
            return 124, f"timed out after {timeout:.0f}s: {' '.join(args[:3])}"
        return proc.returncode or 0, out.decode(errors="replace")

    async def verify(self, pr: PullRequest) -> VerifyResult:
        with tempfile.TemporaryDirectory(prefix=f"verify-pr{pr.number}-") as tmp:
            # Minimal environment: none of this process's secrets are inherited by git or the command.
            env = {"PATH": os.environ.get("PATH", "/usr/bin:/bin"), "HOME": tmp, "LANG": "C.UTF-8",
                   "GIT_TERMINAL_PROMPT": "0"}
            code, out = await self._run(
                "git", "clone", "--quiet", "--branch", self._main, self._url, "repo", cwd=tmp, env=env
            )
            if code:
                return VerifyResult(False, "clone failed")
            repo = os.path.join(tmp, "repo")
            # Verify exactly the commit that will be merged. refs/pull/N/head trails a push by ~100ms,
            # so re-fetch briefly before concluding that the head really moved.
            fetched = ""
            for attempt in range(6):
                code, _ = await self._run("git", "fetch", "--quiet", "origin", f"refs/pull/{pr.number}/head", cwd=repo, env=env)
                if code:
                    return VerifyResult(False, "fetching the PR head failed")
                _, fetched = await self._run("git", "rev-parse", "FETCH_HEAD", cwd=repo, env=env)
                if fetched.strip() == pr.head_sha:
                    break
                await asyncio.sleep(0.5 * (attempt + 1))
            else:
                return VerifyResult(False, f"PR head moved during verification ({fetched.strip()[:10]} != {pr.head_sha[:10]}); resubmit")
            # Drop the tokenized remote before any PR code runs.
            await self._run("git", "remote", "remove", "origin", cwd=repo, env=env)
            code, out = await self._run(
                "git", "-c", "user.name=merge-bot", "-c", "user.email=merge-bot@agents.invalid",
                "merge", "--no-edit", "--quiet", pr.head_sha, cwd=repo, env=env,
            )
            if code:
                return VerifyResult(False, f"merging into {self._main} failed: {out[-2000:]}")
            command = ["bash", "-c", self._command]
            setpriv = shutil.which("setpriv")
            if os.geteuid() == 0 and setpriv:
                # Run PR code as `nobody`: it cannot read this process's environment (/proc/<pid>/environ)
                # or the 0600 database. Still not a sandbox (network, CPU): prefer an isolated verifier.
                for root, dirs, files in os.walk(tmp):
                    for name in (*dirs, *files):
                        with contextlib.suppress(OSError):
                            os.chown(os.path.join(root, name), 65534, 65534, follow_symlinks=False)
                os.chown(tmp, 65534, 65534)
                command = [setpriv, "--reuid=65534", "--regid=65534", "--clear-groups", "--no-new-privs", *command]
            code, out = await self._run(*command, cwd=repo, env=env, timeout=self._timeout)
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
        mergeable_retries: int = 5,
        retry_delay_s: float = 2.0,
        allow_foreign_submissions: bool = False,
        conflicts: GitConflictChecker | None = None,
        defer_s: float = 30.0,
        max_deferrals: int = 3,
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
        self.allow_foreign_submissions = allow_foreign_submissions
        self.conflicts = conflicts
        self._backoff = 2.0
        self._wake = asyncio.Event()
        # A clean PR that Gitea still refuses to merge steps aside (request id -> (not before, count))
        # so it cannot hold up the queue, and fails after `max_deferrals` rounds instead of forever.
        self.defer_s = defer_s
        self.max_deferrals = max_deferrals
        self._deferred: dict[int, tuple[float, int]] = {}

    def poke(self) -> None:
        self._wake.set()

    async def run(self, stop: asyncio.Event) -> None:
        while not stop.is_set():
            now = time.time()
            waiting = {rid: t for rid, (t, _) in self._deferred.items() if t > now}
            req = await self.store.merge_next(skip=waiting)
            if req is None:
                self._wake.clear()
                try:
                    await asyncio.wait_for(self._wake.wait(), min([self.poll_s, *(t - now for t in waiting.values())]))
                except TimeoutError:
                    pass
                continue
            try:
                done = await self.process(req)
                if done.status is not MergeStatus.QUEUED:
                    self._deferred.pop(req.id, None)
            except (httpx.TransportError, GiteaError) as exc:
                if isinstance(exc, GiteaError) and exc.status < 500:
                    await self._fail_safely(req, exc)
                    continue
                # Gitea is down or overloaded: not the author's fault. Put the request back and wait.
                log.warning("gitea unavailable while landing PR #%s (%s); re-queued", req.pr_number, exc)
                with contextlib.suppress(Exception):
                    await self.store.merge_update(req.id, MergeStatus.QUEUED, f"retrying: {exc}")
                with contextlib.suppress(TimeoutError):
                    await asyncio.wait_for(stop.wait(), self._backoff)
                self._backoff = min(self._backoff * 2, 120.0)
                continue
            except Exception as exc:  # keep the queue moving; the author gets the reason
                await self._fail_safely(req, exc)
                continue
            self._backoff = 2.0

    async def _fail_safely(self, req: MergeRequest, exc: Exception) -> None:
        log.error("merge queue failed on PR #%s", req.pr_number, exc_info=exc)
        self._deferred.pop(req.id, None)
        try:
            await self._bounce(req, MergeStatus.FAILED, f"merge queue error: {exc}")
        except Exception:  # e.g. the store itself failed: the request is re-queued on restart
            log.exception("could not record the failure of PR #%s", req.pr_number)

    async def process(self, req: MergeRequest) -> MergeRequest:
        started = time.time()
        pr = await self.gitea.get_pr(self.owner, self.repo, req.pr_number)
        if pr.merged:
            return await self.store.merge_update(req.id, MergeStatus.MERGED, "already merged", pr.head_sha)
        if pr.state != "open":
            return await self.store.merge_update(req.id, MergeStatus.CANCELLED, "PR is closed", pr.head_sha)
        if pr.base_ref != self.main:
            return await self._bounce(req, MergeStatus.FAILED, f"PR must target {self.main}, not {pr.base_ref}")
        if pr.author != req.worker and not self.allow_foreign_submissions:
            return await self._bounce(
                req, MergeStatus.FAILED,
                f"PR #{pr.number} belongs to {pr.author}; only its author can submit it (ask them by DM)",
            )
        if WIP.match(pr.title):
            return await self._bounce(
                req, MergeStatus.FAILED,
                f"PR #{pr.number} is marked work-in-progress ({pr.title!r}); Gitea will not merge it. "
                "Remove the WIP prefix from the title and resubmit.",
            )
        files = await self.gitea.pr_files(self.owner, self.repo, pr.number)
        if self.conflicts is not None:
            # Exact and immediate: git says whether the merge is clean and which files conflict.
            report = await self.conflicts.check(pr.number, pr.head_sha)
            if not report.head_found:
                return await self._bounce(
                    req, MergeStatus.CONFLICT, f"PR #{pr.number} head moved while queued; push and resubmit.", pr.head_sha
                )
            if report.already_on_main:
                return await self._nothing_to_land(req, pr)
            if not report.clean:
                await self.store.record_file_heat(report.files or files, conflicted=True)
                return await self._bounce(
                    req,
                    MergeStatus.CONFLICT,
                    f"PR #{pr.number} conflicts with {self.main} in: {', '.join(report.files[:15]) or 'unknown files'}. "
                    f"Merge the latest {self.main} into {pr.head_ref}, resolve, re-run your checks, push, "
                    "then call merge_request again.",
                    pr.head_sha,
                )
        elif not files:
            return await self._nothing_to_land(req, pr)
        else:
            # Without a local mirror, fall back to Gitea's asynchronous `mergeable` flag. It reads
            # false while Gitea re-checks PRs after every push to main, so back off before believing it.
            for attempt in range(self.mergeable_retries):
                if pr.mergeable:
                    break
                await asyncio.sleep(self.retry_delay_s * (2 ** attempt))
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
        # When git already said the merge is clean, a 405 can only mean "still checking".
        known_clean = self.conflicts is not None
        for attempt in range(self.mergeable_retries + 1):
            try:
                await self.gitea.merge_pr(self.owner, self.repo, pr.number, pr.head_sha, title=pr.title)
                break
            except GiteaError as e:
                if e.status not in (405, 409):
                    raise
                fresh = await self.gitea.get_pr(self.owner, self.repo, pr.number)
                head_moved = fresh.head_sha != pr.head_sha
                if not head_moved and attempt < self.mergeable_retries and (
                    known_clean or e.status == 409 or fresh.mergeable or attempt == 0
                ):
                    await asyncio.sleep(self.retry_delay_s * (2 ** attempt))
                    continue
                if known_clean and not head_moved:
                    # Clean per git, but Gitea still refuses (usually: still re-checking after main
                    # moved). Step aside and retry later; a refusal that persists is reported.
                    _, count = self._deferred.get(req.id, (0.0, 0))
                    if count < self.max_deferrals:
                        self._deferred[req.id] = (time.time() + self.defer_s * 2**count, count + 1)
                        return await self.store.merge_update(
                            req.id, MergeStatus.QUEUED, f"gitea refused the merge ({e}); retrying later", pr.head_sha
                        )
                    self._deferred.pop(req.id, None)
                    return await self._bounce(
                        req,
                        MergeStatus.FAILED,
                        f"PR #{pr.number} merges cleanly with {self.main}, but Gitea keeps refusing to merge it ({e}). "
                        "Check the PR on Gitea (title, branch, permissions); push a fix and resubmit, or ask on the channel.",
                        pr.head_sha,
                    )
                return await self._bounce(
                    req,
                    MergeStatus.CONFLICT,
                    f"PR #{pr.number} could not land ({e}). Merge the latest {self.main} into "
                    f"{pr.head_ref}, re-verify, push, and resubmit.",
                    pr.head_sha,
                )
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

    async def _nothing_to_land(self, req: MergeRequest, pr: PullRequest) -> MergeRequest:
        return await self._bounce(
            req,
            MergeStatus.CANCELLED,
            f"PR #{pr.number} has nothing to land: {pr.head_ref} is already contained in {self.main} "
            "(it landed through another PR). Close the PR; if that work is done, release its claim.",
            pr.head_sha,
        )

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
        except Exception:  # Gitea errors or outages must not take the queue down
            log.warning("could not comment on PR #%s", req.pr_number)
        return updated
