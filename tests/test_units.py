from __future__ import annotations

import time

import pytest

from scaling_agent.config import RunConfig, StaggerPhase
from scaling_agent.coord import scope
from scaling_agent.coord.grep import matches, parse_query
from scaling_agent.coord.render import render_turn_prompt
from scaling_agent.coord.store import ValidationError
from scaling_agent.launcher import schedule_offsets
from scaling_agent.protocol import BoardType, Event, MergeStatus, Priority
from scaling_agent.runtime.adapters.claude_code import pushes_protected
from scaling_agent.workspace.routing import route_webhook


# ------------------------------------------------------------------- grep / scope


def test_grep_dnf():
    clauses = parse_query("latex&table, Math ,&")
    assert clauses == [["latex", "table"], ["math"]]
    assert matches("LaTeX writer drops table captions", clauses)
    assert matches("math is broken", clauses)
    assert not matches("latex figures", clauses)


@pytest.mark.parametrize(
    ("a", "b", "expected"),
    [
        ("src/a.py", "src/a.py", True),
        ("src/", "src/a.py", True),
        ("src/a", "src/ab.py", False),
        ("src/*_writer.py", "src/latex_writer.py", True),
        ("src/*_writer.py", "src/main.py", False),
        ("src/writers/*", "src/writers/", True),
        ("tests/test_latex*", "src/latex.py", False),
        ("area:parser", "area:Parser", True),
        ("area:parser", "src/parser.py", False),
    ],
)
def test_scope_overlap(a, b, expected):
    na, nb = scope.normalize([a])[0], scope.normalize([b])[0]
    assert scope.items_overlap(na, nb) is expected
    assert scope.items_overlap(nb, na) is expected


def test_scope_normalize_dedupes_and_cleans():
    assert scope.normalize(["./src//a.py", "src/a.py", "AREA: Parser", "  "]) == ["src/a.py", "area:parser"]


# ------------------------------------------------------------------------- store


async def test_claim_conflict_notifies_and_expires(store):
    first = await store.claim("w1", ["src/x/"], "x module", ttl_s=0.5)
    second = await store.claim("w2", ["src/x/y.py"], "y file")
    assert [c.worker for c in second.conflicts] == ["w1"]
    events, _, _ = await store.drain_high("w1")
    assert [e.kind for e in events] == ["claim_overlap"]
    time.sleep(0.6)
    third = await store.claim("w3", ["src/x/"], "x again")
    assert [c.worker for c in third.conflicts] == ["w2"]  # w1's claim expired
    assert first.claim is not None


async def test_touch_renews_claims(store):
    await store.claim("w1", ["src/a"], "a", ttl_s=0.3)
    await store.touch("w1")  # renews to now + claim_ttl_s (600)
    time.sleep(0.4)
    assert len(await store.active_claims("w1")) == 1


async def test_release_requires_owner(store):
    result = await store.claim("w1", ["src/a"], "a")
    with pytest.raises(ValidationError):
        await store.release_claim("w2", result.claim.id, "done")
    released = await store.release_claim("w1", result.claim.id, "handoff", "to w2")
    assert released.outcome == "handoff: to w2"
    assert await store.active_claims("w1") == []


async def test_board_modes(store):
    await store.claim("w1", ["src/a/"], "a")
    await store.board_write("w2", BoardType.FACT, "a/ uses utf-8", scope=["src/a/b.py"])
    await store.board_write("w2", BoardType.FACT, "unrelated fact", scope=["src/z.py"])
    _, relevant, _ = await store.drain_high("w1", board_mode="relevant")
    assert [e.text for e in relevant] == ["a/ uses utf-8"]
    batch = await store.next_batch("w1", board_mode="relevant")
    # The relevant entry was forwarded mid-turn; only the unrelated one remains for the digest.
    assert [e.text for e in batch.new_board_entries] == ["unrelated fact"]
    _, all_mode, _ = await store.drain_high("w3", board_mode="all")
    assert {e.text for e in all_mode} >= {"a/ uses utf-8", "unrelated fact"}


async def test_board_entry_inherits_claim_scope(store):
    await store.claim("w2", ["src/q/"], "q")
    entry = await store.board_write("w2", BoardType.OBSERVED, "q prints trailing newline")
    assert entry.scope == ["src/q/"]


async def test_event_dedupe(store):
    ev = Event(event_id="gitea-1", source="gitea", kind="pr_opened", observed_at=time.time(), summary="x")
    assert await store.enqueue("w1", ev) is True
    assert await store.enqueue("w1", ev) is False


async def test_has_pending_ignores_board_noise(store):
    await store.board_write("w2", BoardType.FACT, "noise")
    assert await store.has_pending("w1") is False
    await store.post_channel("w2", "interface change")
    assert await store.has_pending("w1") is True


async def test_merge_queue_store_and_hotspots(store):
    req, pos = await store.merge_submit("w1", 7)
    again, pos2 = await store.merge_submit("w1", 7)
    assert req.id == again.id and pos == pos2 == 1
    nxt = await store.merge_next()
    assert nxt.status is MergeStatus.TESTING
    await store.merge_update(nxt.id, MergeStatus.MERGED)
    assert await store.queue_depth() == 0
    await store.record_file_heat(["src/main.py"], conflicted=True)
    result = await store.claim("w2", ["src/"], "everything")
    assert "src/main.py" in result.hotspots
    hot = await store.hotspots()
    assert hot[0]["path"] in ("src/main.py", "src/")


# ------------------------------------------------------------------------ render


def test_turn_prompt_order():
    from scaling_agent.protocol import TurnBatch

    batch = TurnBatch(
        lease_id="l",
        worker="w1",
        events=[Event(event_id="e", source="messages", kind="direct_message", priority=Priority.HIGH, observed_at=0, summary="DM from w2: hi")],
    )
    text = render_turn_prompt(batch, "[card]", [], extra=["reminder"])
    assert text.index("[card]") < text.index("You hold no live claim") < text.index("reminder") < text.index("DM from w2")


# ----------------------------------------------------------------------- routing


def test_route_pr_to_involved_and_push_to_claim_holders():
    from scaling_agent.protocol import Claim

    workers = {"w1", "w2", "w3"}
    payload = {
        "action": "opened",
        "sender": {"login": "w1"},
        "pull_request": {
            "number": 5, "title": "t", "user": {"login": "w1"},
            "assignees": [{"login": "w2"}], "requested_reviewers": [], "body": "cc @w3 @nobody",
        },
    }
    routed = route_webhook("pull_request", "d1", payload, workers, [])
    assert sorted(w for w, _ in routed) == ["w2", "w3"]  # author/sender excluded, unknown mention ignored

    claims = [Claim(id=1, worker="w2", scope=["src/a/"], intent="a", created_at=0, expires_at=1e12)]
    push = {"ref": "refs/heads/main", "after": "abc123", "sender": {"login": "merge-bot"},
            "commits": [{"added": [], "modified": ["src/a/x.py"], "removed": []}]}
    routed = route_webhook("push", "d2", push, workers, claims)
    assert [(w, e.kind) for w, e in routed] == [("w2", "main_advanced_in_your_scope")]


# ---------------------------------------------------------------- guard / sched


@pytest.mark.parametrize(
    ("cmd", "blocked"),
    [
        ("git push origin feature/main-fix", False),
        ("git push -u origin w0001/latex", False),
        ("git push origin main", True),
        ("git push origin HEAD:main", True),
        ("git push --force origin w1", True),
        ("git push origin +w1", True),
        ("make test && git push origin refs/heads/main", True),
    ],
)
def test_push_guard(cmd, blocked):
    assert pushes_protected(cmd) is blocked


def test_schedule_offsets_founders_and_phases():
    run = RunConfig(
        task_file="t.md", workers=7, founders=2, founding_s=100,
        stagger=[StaggerPhase(until_s=120, interval_s=30), StaggerPhase(interval_s=3)],
    )
    assert schedule_offsets(run) == [0, 30, 100, 130, 133, 136, 139]


def test_all_adapters_instantiate(tmp_path):
    from scaling_agent.runtime.adapters import build_adapter

    common = {"coord_url": "http://coord:8700", "token": "t", "workdir": str(tmp_path), "drain": None, "model": None, "env": {}}
    for kind in ("claude_code", "scripted", "fake"):
        assert build_adapter(kind, **common) is not None
