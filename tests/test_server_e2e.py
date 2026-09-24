"""End to end over real HTTP: MCP tools, mid-turn delivery, claims, turn leasing, prompts, runtime."""

from __future__ import annotations

from conftest import text_of

from scaling_agent.runtime.adapters.fake import FakeAdapter
from scaling_agent.runtime.client import CoordClient
from scaling_agent.runtime.worker import CONTINUE_NUDGE, WorkerRuntime


async def register(server, *ids: str) -> dict[str, str]:
    async with server.admin() as admin:
        resp = await admin.post("/api/admin/workers", json={"ids": list(ids)})
        resp.raise_for_status()
        return resp.json()["tokens"]


async def test_unauthenticated_tool_call(server):
    async with server.mcp("nope") as mcp:
        result = await mcp.call_tool("board_read", {})
    assert "unauthenticated" in text_of(result)


async def test_claim_overlap_reaches_holder_mid_turn(server):
    tokens = await register(server, "w1", "w2")
    async with server.mcp(tokens["w1"]) as w1, server.mcp(tokens["w2"]) as w2:
        r1 = text_of(await w1.call_tool("claim", {"scope": ["src/writers/latex.py"], "intent": "latex writer"}))
        assert "claimed claim#" in r1
        r2 = text_of(await w2.call_tool("claim", {"scope": ["src/writers/"], "intent": "all writers"}))
        assert "WARNING overlaps" in r2 and "w1" in r2
        # w1's next infrastructure call carries the overlap notice (HIGH priority, mid-turn).
        r3 = text_of(await w1.call_tool("board_read", {"limit": 5}))
        assert "UPDATES" in r3 and "claim_overlap" in r3 and "w2" in r3
        # ... exactly once.
        r4 = text_of(await w1.call_tool("board_read", {"limit": 5}))
        assert "claim_overlap" not in r4


async def test_exclusive_claim_is_refused(server):
    tokens = await register(server, "w1", "w2")
    async with server.mcp(tokens["w1"]) as w1, server.mcp(tokens["w2"]) as w2:
        await w1.call_tool("claim", {"scope": ["area:parser"], "intent": "parser"})
        r = text_of(await w2.call_tool("claim", {"scope": ["area:parser"], "intent": "parser too", "exclusive": True}))
        assert r.startswith("REFUSED")


async def test_dm_and_board_relevance(server):
    tokens = await register(server, "w1", "w2", "w3")
    async with server.mcp(tokens["w1"]) as w1, server.mcp(tokens["w2"]) as w2, server.mcp(tokens["w3"]) as w3:
        await w1.call_tool("claim", {"scope": ["src/a/"], "intent": "module a"})
        await w3.call_tool("claim", {"scope": ["src/zzz/"], "intent": "unrelated"})
        await w2.call_tool("board_write", {"type": "FAIL", "text": "regex approach for a/ is too slow", "scope": ["src/a/x.py"]})
        await w2.call_tool("send_dm", {"to": "w1", "text": "I'll take a/x.py tests"})
        got1 = text_of(await w1.call_tool("messages", {}))
        assert "regex approach" in got1 and "DM from w2" in got1
        got3 = text_of(await w3.call_tool("messages", {}))
        assert "regex approach" not in got3  # not relevant to w3's claim: digest only, not mid-turn


async def test_board_caps_and_claim_via_board_rejected(server):
    tokens = await register(server, "w1")
    async with server.mcp(tokens["w1"]) as w1:
        too_long = text_of(await w1.call_tool("board_write", {"type": "FACT", "text": "x" * 101}))
        assert "capped at 100" in too_long
        claim = text_of(await w1.call_tool("board_write", {"type": "CLAIM", "text": "mine"}))
        assert "use the `claim` tool" in claim
        ok = text_of(await w1.call_tool("board_write", {"type": "PATCH_SUMMARY", "text": "files=a | idea=b | evidence=c", "detail": "long"}))
        assert "published" in ok
        grep = text_of(await w1.call_tool("board_grep", {"query": "idea=b&evidence,nothing"}))
        assert "PATCH_SUMMARY" in grep


async def test_turn_lease_ack_and_redelivery(server):
    tokens = await register(server, "w1", "w2")
    async with server.mcp(tokens["w2"]) as w2:
        await w2.call_tool("send_dm", {"to": "w1", "text": "ping"})
    c1 = CoordClient(server.url, tokens["w1"])
    try:
        batch, _ = await c1.next_turn(wait_s=1)
        assert [e.kind for e in batch.events] == ["direct_message"]
        # No ack (crash): the event is leased again on the next batch.
        batch2, _ = await c1.next_turn(wait_s=0)
        assert [e.kind for e in batch2.events] == ["direct_message"]
        from scaling_agent.protocol import TurnReport

        assert await c1.ack(TurnReport(lease_id=batch2.lease_id)) is False
        batch3, _ = await c1.next_turn(wait_s=0)
        assert batch3.events == []
    finally:
        await c1.close()


async def test_runtime_turn_with_fake_harness(server, tmp_path):
    tokens = await register(server, "w1", "w2")
    async with server.admin() as admin:
        await admin.put(
            "/api/admin/prompt",
            json={"system_template": "You are {{ worker_id }} of {{ n }}.", "card_template": "[card {{ worker_id }}]", "values": {"n": 2}},
        )
    async with server.worker(tokens["w1"]) as http:
        prompt = (await http.get("/api/prompt")).json()
    assert prompt["system_prompt"] == "You are w1 of 2."

    fake = FakeAdapter()
    coord = CoordClient(server.url, tokens["w1"])
    runtime = WorkerRuntime(
        "w1", coord, fake, prompt["system_prompt"], prompt["protocol_card"], str(tmp_path / "state"),
        continue_after_s=0.2, rotate_after_turns=2,
    )
    try:
        await fake.start(runtime.system_prompt)
        await runtime.step()
        assert fake.prompts[-1].startswith("[card w1]") and CONTINUE_NUDGE in fake.prompts[-1]
        async with server.mcp(tokens["w2"]) as w2:
            await w2.call_tool("channel_post", {"text": "interface for writers: write(doc) -> str"})
        await runtime.step()
        assert "interface for writers" in fake.prompts[-1]
        await runtime.step()  # third turn crosses rotate_after_turns=2 -> fresh session with handoff
        assert fake.starts[-1] is None and "Handoff from your previous session" in fake.prompts[-1]
    finally:
        await coord.close()


async def test_status_and_broadcast(server):
    tokens = await register(server, "w1")
    async with server.admin() as admin:
        r = await admin.post("/api/admin/broadcast", json={"text": "T-45min: land your PRs", "urgent": True, "key": "r45"})
        assert r.json()["queued"] == 1
        status = (await admin.get("/api/admin/status")).json()
    assert status["pending_events"] == 1 and "merge_queue" in status
    async with server.mcp(tokens["w1"]) as w1:
        assert "T-45min" in text_of(await w1.call_tool("merge_status", {}))


async def test_idle_backoff_and_reset(server, tmp_path):
    tokens = await register(server, "w1", "w2")
    fake = FakeAdapter()
    coord = CoordClient(server.url, tokens["w1"])
    rt = WorkerRuntime("w1", coord, fake, "sys", "[card]", str(tmp_path / "s"),
                       continue_after_s=0.05, max_continue_after_s=0.2, board_reminder=False)
    try:
        await fake.start("sys")
        await rt.step()
        await rt.step()
        assert rt.idle_streak == 2
        async with server.mcp(tokens["w2"]) as w2:
            await w2.call_tool("send_dm", {"to": "w1", "text": "wake up"})
        await rt.step()
        assert "wake up" in fake.prompts[-1] and rt.idle_streak == 0
    finally:
        await coord.close()


async def test_replacement_sandbox_gets_a_handoff(server, tmp_path):
    """A runtime with no local state (its sandbox was replaced) learns its live claims."""
    tokens = await register(server, "w1", "w2")
    async with server.mcp(tokens["w1"]) as w1:
        await w1.call_tool("claim", {"scope": ["src/parser.py"], "intent": "parser rewrite"})

    async def first_prompt(worker: str) -> str:
        fake = FakeAdapter()
        coord = CoordClient(server.url, tokens[worker])
        runtime = WorkerRuntime(worker, coord, fake, "sys", "[card]", str(tmp_path / worker), continue_after_s=0.1)
        try:
            await runtime.run(max_turns=1)
        finally:
            await coord.close()
        return fake.prompts[0]

    replaced = await first_prompt("w1")
    assert "Handoff from your previous session" in replaced and "src/parser.py" in replaced
    assert "Handoff" not in await first_prompt("w2")  # a genuinely new worker has nothing to hand off
