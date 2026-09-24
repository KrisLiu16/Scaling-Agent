from __future__ import annotations

import json

from scaling_agent.protocol import MergeStatus
from scaling_agent.workspace.gitea import GiteaError, PullRequest
from scaling_agent.workspace.merge_queue import MergeQueue, VerifyResult


class FakeGitea:
    def __init__(self, prs: dict[int, PullRequest], merge_error: int | None = None) -> None:
        self.prs = prs
        self.merge_error = merge_error
        self.merged: list[tuple[int, str]] = []
        self.comments: list[tuple[int, str]] = []

    async def get_pr(self, owner, repo, number):
        return self.prs[number]

    async def pr_files(self, owner, repo, number):
        return ["src/main.py", f"src/mod{number}.py"]

    async def merge_pr(self, owner, repo, number, head_sha, title=None):
        if self.merge_error:
            raise GiteaError(self.merge_error, "head moved")
        self.merged.append((number, head_sha))

    async def comment(self, owner, repo, number, body):
        self.comments.append((number, body))


def pr(number: int, mergeable: bool = True, base: str = "main") -> PullRequest:
    return PullRequest(number=number, state="open", merged=False, mergeable=mergeable, head_ref=f"w1/t{number}",
                       head_sha=f"sha{number}", base_ref=base, author="w1", title=f"PR {number}")


class FailingVerifier:
    async def verify(self, p):
        return VerifyResult(False, "3 tests failed")


async def test_lands_clean_pr_and_notifies_author(store):
    gitea = FakeGitea({1: pr(1)})
    queue = MergeQueue(store, gitea, "org", "repo", retry_delay_s=0)
    await store.merge_submit("w1", 1)
    done = await queue.process(await store.merge_next())
    assert done.status is MergeStatus.MERGED and gitea.merged == [(1, "sha1")]
    batch = await store.next_batch("w1")
    assert [e.kind for e in batch.events] == ["merged"]


async def test_conflict_bounces_with_high_priority(store):
    gitea = FakeGitea({2: pr(2, mergeable=False)})
    queue = MergeQueue(store, gitea, "org", "repo", retry_delay_s=0)
    await store.merge_submit("w1", 2)
    done = await queue.process(await store.merge_next())
    assert done.status is MergeStatus.CONFLICT and not gitea.merged and gitea.comments
    events, _, _ = await store.drain_high("w1")
    assert events[0].kind == "merge_conflict" and "Merge the latest main" in events[0].summary
    hot = {h["path"]: h for h in await store.hotspots()}
    assert hot["src/main.py"]["conflicts"] == 1


async def test_failed_verification_and_head_moved(store):
    gitea = FakeGitea({3: pr(3)})
    await store.merge_submit("w1", 3)
    done = await MergeQueue(store, gitea, "org", "repo", verifier=FailingVerifier(), retry_delay_s=0).process(await store.merge_next())
    assert done.status is MergeStatus.FAILED and "3 tests failed" in done.detail

    gitea = FakeGitea({4: pr(4)}, merge_error=409)
    await store.merge_submit("w1", 4)
    done = await MergeQueue(store, gitea, "org", "repo", retry_delay_s=0).process(await store.merge_next())
    assert done.status is MergeStatus.CONFLICT


async def test_wrong_base_is_rejected(store):
    gitea = FakeGitea({5: pr(5, base="dev")})
    await store.merge_submit("w1", 5)
    done = await MergeQueue(store, gitea, "org", "repo", retry_delay_s=0).process(await store.merge_next())
    assert done.status is MergeStatus.FAILED and "must target main" in done.detail


# --------------------------------------------------------------------------- AGS


def test_ags_tool_request_shape():
    from scaling_agent.sandbox.ags_control import AgsControlPlane, ToolSpec

    cp = AgsControlPlane.__new__(AgsControlPlane)  # no network: only build the request
    req = cp._create_tool_request(
        ToolSpec(name="sa-worker", image="sgccr.ccs.tencentyun.com/ns/worker:v1", role_arn="qcs::cam::uin/1:roleName/x", disk="20Gi")
    )
    body = json.loads(req.to_json_string())
    assert body["ToolType"] == "custom" and body["Persistent"] is True
    sent = req._serialize()  # what the SDK actually sends: None fields are dropped
    assert "DefaultTimeout" not in sent  # persistent tools: no reclaim deadline requested
    cc = body["CustomConfiguration"]
    assert cc["Command"] == ["/bin/sh", "-c"] and "envd -port 49983" in cc["Args"][0]
    assert cc["Probe"]["HttpGet"] == {"Path": "/health", "Port": 49983, "Scheme": "HTTP"}
    assert cc["Probe"]["ReadyTimeoutMs"] <= 30000
    assert cc["Resources"] == {"CPU": "2", "Memory": "4Gi", "Storage": "20Gi"}
    assert len(body["ClientToken"]) <= 64


def test_ags_start_is_idempotent_and_reattaches(monkeypatch):
    from types import SimpleNamespace

    from scaling_agent.sandbox.ags_control import AgsControlPlane

    calls: list[str] = []
    running = SimpleNamespace(InstanceId="sbi-1", Status="RUNNING", Metadata=[], TimeoutSeconds=None, ExpiresAt=None)

    class FakeClient:
        def StartSandboxInstance(self, req):
            calls.append(json.loads(req.to_json_string())["ClientToken"])
            return SimpleNamespace(Instance=running)

    cp = AgsControlPlane.__new__(AgsControlPlane)
    cp.c = FakeClient()
    cp.region = "ap-singapore"
    found = {"value": None}
    monkeypatch.setattr(cp, "find_worker_instance", lambda *a: found["value"])
    monkeypatch.setattr(cp, "wait_running", lambda iid, wait_s=600: running)
    cp.start_instance("tool-1", "w0001", "run", timeout=None)
    cp.start_instance("tool-1", "w0001", "run", timeout=None)
    assert len(calls) == 2 and calls[0] == calls[1]  # same ClientToken for the same logical start
    found["value"] = running
    cp.start_instance("tool-1", "w0001", "run", timeout=None)
    assert len(calls) == 2  # re-attached to the live instance instead of starting another


async def test_mergeability_check_in_progress_is_not_a_conflict(store):
    class SlowCheckGitea(FakeGitea):
        def __init__(self):
            super().__init__({6: pr(6, mergeable=False)})
            self.reads = 0

        async def get_pr(self, owner, repo, number):
            self.reads += 1
            return pr(6, mergeable=self.reads >= 2)  # false while Gitea is still checking

    gitea = SlowCheckGitea()
    await store.merge_submit("w1", 6)
    done = await MergeQueue(store, gitea, "org", "repo", retry_delay_s=0).process(await store.merge_next())
    assert done.status is MergeStatus.MERGED


def _git(cwd, *args):
    import subprocess

    return subprocess.run(["git", *args], cwd=cwd, check=True, capture_output=True, text=True).stdout.strip()


async def test_git_conflict_checker_finds_exact_conflicts(tmp_path):
    from scaling_agent.workspace.conflicts import GitConflictChecker

    origin = tmp_path / "origin.git"
    work = tmp_path / "work"
    _git(tmp_path, "init", "-q", "--bare", "-b", "main", str(origin))
    _git(tmp_path, "clone", "-q", str(origin), str(work))
    _git(work, "config", "user.email", "t@t")
    _git(work, "config", "user.name", "t")
    (work / "REGISTRY.md").write_text("base\n")
    (work / "other.txt").write_text("x\n")
    _git(work, "add", "-A")
    _git(work, "commit", "-qm", "base")
    _git(work, "push", "-q", "origin", "HEAD:main")
    # PR 1 edits REGISTRY.md, PR 2 edits another file; then main also edits REGISTRY.md.
    _git(work, "checkout", "-qb", "a")
    (work / "REGISTRY.md").write_text("base\nfrom a\n")
    _git(work, "commit", "-qam", "a")
    sha_a = _git(work, "rev-parse", "HEAD")
    _git(work, "push", "-q", "origin", "HEAD:refs/pull/1/head")
    _git(work, "checkout", "-q", "main")
    _git(work, "checkout", "-qb", "b")
    (work / "other.txt").write_text("y\n")
    _git(work, "commit", "-qam", "b")
    sha_b = _git(work, "rev-parse", "HEAD")
    _git(work, "push", "-q", "origin", "HEAD:refs/pull/2/head")
    _git(work, "checkout", "-q", "main")
    (work / "REGISTRY.md").write_text("base\nfrom main\n")
    _git(work, "commit", "-qam", "main moves")
    _git(work, "push", "-q", "origin", "HEAD:main")

    checker = GitConflictChecker(str(tmp_path / "mirror.git"), f"file://{origin}", "unused", "main")
    checker._url = f"file://{origin}"  # no credentials for a local remote
    conflict = await checker.check(1, sha_a)
    assert not conflict.clean and conflict.files == ["REGISTRY.md"]
    clean = await checker.check(2, sha_b)
    assert clean.clean
    moved = await checker.check(2, "0" * 40)
    assert not moved.head_found
    # A PR whose head is already in main (it landed through another PR): nothing to merge.
    base = _git(work, "rev-parse", "HEAD~1")
    _git(work, "push", "-q", "origin", f"{base}:refs/pull/3/head")
    landed = await checker.check(3, base)
    assert landed.clean and landed.already_on_main and not clean.already_on_main


class FakeChecker:
    def __init__(self, **report) -> None:
        self.report = report

    async def check(self, pr_number, head_sha):
        from scaling_agent.workspace.conflicts import ConflictReport

        return ConflictReport(**{"clean": True, **self.report})


async def test_pr_already_on_main_is_cancelled_not_retried(store):
    gitea = FakeGitea({9: pr(9)}, merge_error=405)  # Gitea refuses empty merges with 405
    await store.merge_submit("w1", 9)
    queue = MergeQueue(store, gitea, "org", "repo", retry_delay_s=0, conflicts=FakeChecker(already_on_main=True))
    done = await queue.process(await store.merge_next())
    assert done.status is MergeStatus.CANCELLED and "already contained in main" in done.detail
    events, _, _ = await store.drain_high("w1")
    assert events[0].kind == "merge_cancelled"

    class EmptyPrGitea(FakeGitea):  # without a mirror: an empty diff means the same
        async def pr_files(self, owner, repo, number):
            return []

    await store.merge_submit("w1", 10)
    done = await MergeQueue(store, EmptyPrGitea({10: pr(10)}), "org", "repo", retry_delay_s=0).process(await store.merge_next())
    assert done.status is MergeStatus.CANCELLED


async def test_refused_clean_merge_steps_aside_then_fails(store):
    import asyncio

    class PickyGitea(FakeGitea):
        async def merge_pr(self, owner, repo, number, head_sha, title=None):
            if number == 11:
                raise GiteaError(405, "User not allowed to merge PR")
            self.merged.append((number, head_sha))

    gitea = PickyGitea({11: pr(11), 12: pr(12)})
    queue = MergeQueue(store, gitea, "org", "repo", retry_delay_s=0, mergeable_retries=1,
                       conflicts=FakeChecker(), defer_s=60, poll_s=0.05)
    await store.merge_submit("w1", 11)
    await store.merge_submit("w1", 12)
    stop = asyncio.Event()
    task = asyncio.create_task(queue.run(stop))
    for _ in range(100):
        if gitea.merged:
            break
        await asyncio.sleep(0.02)
    stop.set()
    queue.poke()
    await task
    assert gitea.merged == [(12, "sha12")]  # PR 11 stepped aside instead of blocking the queue
    first = next(r for r in await store.merge_requests("w1") if r.pr_number == 11)
    assert first.status is MergeStatus.QUEUED and "refused" in first.detail

    # Deferred rounds are capped: the author gets Gitea's reason instead of an endless retry.
    queue.defer_s = 0
    for _ in range(queue.max_deferrals):
        req = await store.merge_next()
        done = await queue.process(req)
    assert done.status is MergeStatus.FAILED and "User not allowed" in done.detail
