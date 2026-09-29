"""The pi harness's extension (adapters/pi_extension), run under node against a live coordination server."""

from __future__ import annotations

import asyncio
import json
import os
import shutil
from pathlib import Path

import pytest

from scaling_agent.runtime.adapters.guards import push_denied_reason

PROBE = Path(__file__).parent / "pi_extension_probe.mjs"
GUARD_CASES = Path(__file__).parent / "fixtures" / "push_guard_cases.json"
GUARD = Path(__file__).parent.parent / "src/scaling_agent/runtime/adapters/pi_extension/guard.ts"

pytestmark = pytest.mark.skipif(shutil.which("node") is None, reason="node is not installed")


async def node(*args: str, env: dict[str, str] | None = None) -> str:
    proc = await asyncio.create_subprocess_exec(
        "node", *args, env=env, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
    )
    stdout, stderr = await asyncio.wait_for(proc.communicate(), 60)
    assert proc.returncode == 0, stderr.decode()
    return stdout.decode()


async def test_guard_matches_python_on_shared_cases():
    script = (
        f"import {{ pushesProtected }} from {json.dumps(GUARD.as_uri())};"
        f"import {{ readFileSync }} from 'node:fs';"
        f"const cases = JSON.parse(readFileSync({json.dumps(str(GUARD_CASES))}, 'utf8'));"
        "console.log(JSON.stringify(cases.map(([cmd]) => pushesProtected(cmd))));"
    )
    got = json.loads(await node("--input-type=module", "-e", script))
    expected = [blocked for _, blocked in json.loads(GUARD_CASES.read_text())]
    assert got == expected


async def test_extension_registers_org_tools_and_applies_hooks(server, tmp_path):
    async with server.admin() as admin:
        tokens = (await admin.post("/api/admin/workers", json={"ids": ["w1", "w2"]})).json()["tokens"]
    ready = tmp_path / "ready.json"

    env = {
        **os.environ,
        "SA_ORG_COORD_URL": server.url,
        "SA_ORG_TOKEN": tokens["w1"],
        "SA_ORG_PUSH_DENIED_REASON": push_denied_reason("main"),
        "SA_ORG_READY_FILE": str(ready),
        "PROBE_PEER_TOKEN": tokens["w2"],
        "SA_ORG_BASH_TIMEOUT_S": "456",
    }
    out = json.loads(await node(str(PROBE), env=env))
    assert "error" not in out, out

    async with server.mcp(tokens["w1"]) as client:
        served = sorted(t.name for t in (await client.list_tools()).tools)
    assert out["tools"] == served  # names come from tools/list, none hard-coded
    assert sorted(json.loads(ready.read_text())["tools"]) == served
    assert out["snippet"]
    assert "type" in out["schemaRequired"] and "text" in out["schemaRequired"]
    assert "published #1 [FACT] w1: from node" in out["boardWrite"]
    # a peer's DM rides on the result of a tool that is not an org tool
    assert out["bashResult"][0] == "ls output"
    assert "urgent from peer" in out["bashResult"][1]
    assert out["orgResult"] is None  # org tool results already carry updates
    assert out["blocked"] == {"block": True, "reason": push_denied_reason("main")}
    assert out["allowed"] is None
    assert out["otherTool"] is None
    # pi's bash tool has no default timeout: the extension gives commands without one a bound, and keeps a chosen one
    assert out["timeouts"] == {"plain": 456, "explicit": 1800, "notBash": None}


async def test_extension_fails_loudly_without_configuration(tmp_path):

    env = {k: v for k, v in os.environ.items() if not k.startswith("SA_ORG_")}
    out = json.loads(await node(str(PROBE), env=env))
    assert "SA_ORG_COORD_URL is not set" in out["error"]


async def test_extension_fails_when_the_coordination_server_is_unreachable(tmp_path):

    env = {**os.environ, "SA_ORG_COORD_URL": "http://127.0.0.1:1", "SA_ORG_TOKEN": "x"}
    out = json.loads(await node(str(PROBE), env=env))
    assert "error" in out and "tools" not in out


