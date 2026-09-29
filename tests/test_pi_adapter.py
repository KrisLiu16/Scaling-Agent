"""PiAdapter against a stand-in `pi` executable that speaks the RPC protocol (no model, no network)."""

from __future__ import annotations

import asyncio
import json
import os
import stat
import sys
import textwrap
from pathlib import Path

import pytest

from scaling_agent.runtime.adapters import build_adapter
from scaling_agent.runtime.adapters.pi import PiAdapter, PiExited

FAKE_PI = textwrap.dedent(
    '''\
    #!{python}
    import json, os, subprocess, sys

    args = sys.argv[1:]
    session = args[args.index("--session-id") + 1]
    model = args[args.index("--model") + 1]
    mode = os.environ.get("FAKE_PI_MODE", "ok")
    log = os.environ["FAKE_PI_LOG"]

    def note(entry):
        with open(log, "a") as f:
            f.write(json.dumps(entry) + "\\n")

    def out(record):
        sys.stdout.write(json.dumps(record) + "\\n")
        sys.stdout.flush()

    note({{"argv": args, "env": {{k: v for k, v in os.environ.items() if k.startswith(("PI_", "SA_ORG_", "SA_PI_"))}}}})
    if os.environ.get("SA_ORG_READY_FILE") and mode != "no_tools":
        open(os.environ["SA_ORG_READY_FILE"], "w").write(json.dumps({{"tools": ["claim", "board_write"]}}))
    if mode in ("child", "detached"):
        child = subprocess.Popen(["sleep", "300"], start_new_session=(mode == "detached"))
        open(os.environ["FAKE_PI_CHILD"], "w").write(str(child.pid))

    def assistant(reason, error=None):
        message = {{"role": "assistant", "content": [], "stopReason": reason}}
        if error:
            message["errorMessage"] = error
        return message

    for line in sys.stdin:
        cmd = json.loads(line)
        note({{"cmd": cmd["type"]}})
        reply = lambda data=None, ok=True, **kw: out(
            {{"id": cmd["id"], "type": "response", "command": cmd["type"], "success": ok, "data": data, **kw}}
        ) if "id" in cmd else None
        kind = cmd["type"]
        if kind == "get_state":
            reply({{"model": {{"id": "wrong-model" if mode == "wrong_model" else model}}, "sessionId": session}})
        elif kind == "get_session_stats":
            reply({{
                "sessionId": session, "tokens": {{"input": 10, "output": 5, "total": 15}}, "cost": 0.5,
                "contextUsage": {{"tokens": 1, "contextWindow": 8, "percent": None if mode == "no_percent" else 12.5}},
            }})
        elif kind == "abort":
            reply()
        elif kind == "prompt":
            reply()
            if mode == "hang":
                continue
            if mode == "crash":
                sys.exit(3)
            out({{"type": "agent_start"}})
            for name in ("bash", "claim", "bash"):
                out({{"type": "tool_execution_start", "toolName": name}})
            out({{"type": "message_update", "assistantMessageEvent": {{"type": "text_delta"}}}})
            if mode == "error":
                out({{"type": "message_end", "message": assistant("error", "Our servers are currently overloaded.")}})
                out({{"type": "auto_retry_start", "attempt": 1}})
                out({{"type": "agent_end", "willRetry": True}})
                out({{"type": "message_end", "message": assistant("stop")}})
                out({{"type": "auto_retry_end", "success": True, "attempt": 2}})
            elif mode == "retries_exhausted":
                out({{"type": "message_end", "message": assistant("error", "overloaded")}})
                out({{"type": "auto_retry_end", "success": False, "attempt": 8, "finalError": "overloaded after 8 tries"}})
            else:
                out({{"type": "message_end", "message": assistant("stop")}})
            out({{"type": "agent_end", "willRetry": False}})
            out({{"type": "agent_settled"}})
    '''
)


@pytest.fixture
def fake(tmp_path, monkeypatch):
    binary = tmp_path / "fake-pi"
    binary.write_text(FAKE_PI.format(python=sys.executable))
    binary.chmod(binary.stat().st_mode | stat.S_IXUSR)
    log = tmp_path / "pi.log"
    monkeypatch.setenv("FAKE_PI_LOG", str(log))
    monkeypatch.setenv("FAKE_PI_CHILD", str(tmp_path / "child.pid"))
    monkeypatch.setenv("OPENAI_API_KEY", "sk-secret-value")
    (tmp_path / "work").mkdir()
    return binary


def make(tmp_path, binary, **options) -> PiAdapter:
    return PiAdapter(
        coord_url="http://coord:8700/",
        token="tok-w1",
        workdir=str(tmp_path / "work"),
        model="gpt-x",
        env={"PI_CODING_AGENT_DIR": str(tmp_path / "agent")},
        main_branch="trunk",
        options={"base_url": "http://gw/v1", "binary": str(binary), **options},
    )


def log_entries(tmp_path) -> list[dict]:
    return [json.loads(line) for line in (tmp_path / "pi.log").read_text().splitlines()]


async def test_turn_counts_tools_and_reports_usage(tmp_path, fake):
    adapter = make(tmp_path, fake)
    await adapter.start("SYSTEM PROMPT")
    try:
        outcome = await adapter.run_turn("hello")
    finally:
        await adapter.close()
    assert outcome.ok and outcome.error is None
    assert outcome.tool_calls == 3 and outcome.tools == {"bash": 2, "claim": 1}
    assert outcome.usage == {"input": 10, "output": 5, "total": 15}
    assert outcome.cost_usd == 0.5 and outcome.context_pct == 12.5
    assert outcome.session_id == adapter.session_id
    start = log_entries(tmp_path)[0]
    assert start["env"]["SA_ORG_COORD_URL"] == "http://coord:8700"
    assert start["env"]["SA_ORG_TOKEN"] == "tok-w1" and start["env"]["SA_ORG_MAIN_BRANCH"] == "trunk"
    assert "trunk" in start["env"]["SA_ORG_PUSH_DENIED_REASON"]
    assert start["env"]["PI_OFFLINE"] == "1" and start["env"]["PI_TELEMETRY"] == "0"
    argv = start["argv"]
    assert argv[argv.index("--session-id") + 1] == adapter.session_id
    for flag in ("--no-extensions", "--no-skills", "--no-prompt-templates", "--no-context-files"):
        assert flag in argv  # only our extension, nothing peers can drop into the repo
    assert "--thinking" not in argv


async def test_model_key_is_referenced_not_written(tmp_path, fake):
    adapter = make(tmp_path, fake, thinking="high", headers={"X-Team": "a"}, max_retries=5)
    await adapter.start("SYSTEM PROMPT")
    await adapter.close()
    agent = tmp_path / "agent"
    models = json.loads((agent / "models.json").read_text())
    provider = models["providers"]["gateway"]
    assert provider["apiKey"] == "$OPENAI_API_KEY" and provider["api"] == "openai-responses"
    assert provider["baseUrl"] == "http://gw/v1" and provider["headers"] == {"X-Team": "a"}
    assert provider["models"][0]["id"] == "gpt-x"
    assert "sk-secret-value" not in "".join(p.read_text() for p in agent.iterdir() if p.is_file())
    assert stat.S_IMODE((agent / "models.json").stat().st_mode) == 0o600
    assert json.loads((agent / "settings.json").read_text())["retry"]["maxRetries"] == 5
    assert (agent / "system-prompt.md").read_text() == "SYSTEM PROMPT"
    assert "high" in log_entries(tmp_path)[0]["argv"]


async def test_missing_model_key_is_reported_before_pi_starts(tmp_path, fake, monkeypatch):
    monkeypatch.delenv("OPENAI_API_KEY")
    adapter = make(tmp_path, fake)
    with pytest.raises(RuntimeError, match="OPENAI_API_KEY"):
        await adapter.start("p")
    assert not (tmp_path / "pi.log").exists()


async def test_provider_error_that_pi_retried_away_is_not_a_failure(tmp_path, fake, monkeypatch):
    monkeypatch.setenv("FAKE_PI_MODE", "error")
    adapter = make(tmp_path, fake)
    await adapter.start("p")
    try:
        outcome = await adapter.run_turn("x")
    finally:
        await adapter.close()
    assert outcome.ok  # the intermediate error was retried; the settled run ended normally


async def test_exhausted_retries_fail_the_turn_with_the_reason(tmp_path, fake, monkeypatch):
    monkeypatch.setenv("FAKE_PI_MODE", "retries_exhausted")
    adapter = make(tmp_path, fake)
    await adapter.start("p")
    try:
        outcome = await adapter.run_turn("x")
    finally:
        await adapter.close()
    assert not outcome.ok and outcome.error == "overloaded after 8 tries"


async def test_context_percent_is_optional(tmp_path, fake, monkeypatch):
    monkeypatch.setenv("FAKE_PI_MODE", "no_percent")
    adapter = make(tmp_path, fake)
    await adapter.start("p")
    try:
        assert (await adapter.run_turn("x")).context_pct is None
    finally:
        await adapter.close()


async def test_pi_dying_mid_turn_raises_with_its_stderr_context(tmp_path, fake, monkeypatch):
    monkeypatch.setenv("FAKE_PI_MODE", "crash")
    adapter = make(tmp_path, fake)
    await adapter.start("p")
    try:
        with pytest.raises(PiExited, match="code 3"):
            await adapter.run_turn("x")
    finally:
        await adapter.close()


async def test_start_fails_when_org_tools_did_not_load(tmp_path, fake, monkeypatch):
    monkeypatch.setenv("FAKE_PI_MODE", "no_tools")
    adapter = make(tmp_path, fake)
    with pytest.raises(RuntimeError, match="organization tools did not load"):
        await adapter.start("p")
    assert adapter._proc is None  # the half-started process is gone


async def test_start_fails_when_pi_picked_another_model(tmp_path, fake, monkeypatch):
    monkeypatch.setenv("FAKE_PI_MODE", "wrong_model")
    adapter = make(tmp_path, fake)
    with pytest.raises(RuntimeError, match="wrong-model"):
        await adapter.start("p")


async def test_resume_needs_the_transcript(tmp_path, fake):
    adapter = make(tmp_path, fake)
    with pytest.raises(RuntimeError, match="no transcript"):
        await adapter.start("p", resume="abc")
    transcripts = tmp_path / "agent" / "sessions" / "--work--"
    transcripts.mkdir(parents=True)
    (transcripts / "2026-09-29T00-00-00-000Z_abc.jsonl").write_text("{}\n")
    await adapter.start("p", resume="abc")
    try:
        assert adapter.session_id == "abc"
        argv = log_entries(tmp_path)[0]["argv"]
        assert argv[argv.index("--session-id") + 1] == "abc"
    finally:
        await adapter.close()


async def test_cancelled_turn_sends_abort_and_close_takes_children_down(tmp_path, fake, monkeypatch):
    monkeypatch.setenv("FAKE_PI_MODE", "hang")
    adapter = make(tmp_path, fake)
    await adapter.start("p")
    turn = asyncio.create_task(adapter.run_turn("x"))
    await asyncio.sleep(0.5)
    turn.cancel()
    with pytest.raises(asyncio.CancelledError):
        await turn
    await asyncio.sleep(0.3)
    assert {"cmd": "abort"} in log_entries(tmp_path)
    await adapter.close()


async def test_close_kills_processes_pi_started(tmp_path, fake, monkeypatch):
    monkeypatch.setenv("FAKE_PI_MODE", "child")
    adapter = make(tmp_path, fake)
    await adapter.start("p")
    child = int((tmp_path / "child.pid").read_text())
    os.kill(child, 0)  # alive
    await adapter.close()
    await asyncio.sleep(0.2)
    with pytest.raises(ProcessLookupError):
        os.kill(child, 0)


async def test_close_kills_commands_pi_started_in_their_own_session(tmp_path, fake, monkeypatch):
    monkeypatch.setenv("FAKE_PI_MODE", "detached")  # pi runs each shell command in a new session
    adapter = make(tmp_path, fake)
    await adapter.start("p")
    child = int((tmp_path / "child.pid").read_text())
    os.kill(child, 0)
    await adapter.close()
    await asyncio.sleep(0.3)
    with pytest.raises(ProcessLookupError):
        os.kill(child, 0)


async def test_sessions_do_not_kill_each_others_commands(tmp_path, fake, monkeypatch):
    monkeypatch.setenv("FAKE_PI_MODE", "detached")
    first = make(tmp_path, fake)
    await first.start("p")
    survivor = int((tmp_path / "child.pid").read_text())
    second = make(tmp_path, fake)
    assert first._session_tag != ""
    await second.start("p")
    assert second._session_tag != first._session_tag
    await second.close()
    os.kill(survivor, 0)  # the first session's command is untouched by the second one's cleanup
    await first.close()


async def test_bash_timeout_default_is_handed_to_the_extension(tmp_path, fake):
    adapter = make(tmp_path, fake, bash_timeout_s=123)
    await adapter.start("p")
    await adapter.close()
    env = log_entries(tmp_path)[0]["env"]
    assert env["SA_ORG_BASH_TIMEOUT_S"] == "123" and env["SA_PI_SESSION_TAG"]


async def test_adapter_reusable_after_close(tmp_path, fake):
    adapter = make(tmp_path, fake)
    for _ in range(2):
        await adapter.start("p")
        assert (await adapter.run_turn("x")).ok
        await adapter.close()


def test_options_are_validated_and_only_pi_takes_them(tmp_path):
    common = {"coord_url": "http://c", "token": "t", "workdir": str(tmp_path), "drain": None, "model": "m", "env": {}}
    with pytest.raises(ValueError, match="harness_options are not supported"):
        build_adapter("scripted", options={"base_url": "x"}, **common)
    with pytest.raises(ValueError):
        build_adapter("pi", options={"base_url": "x", "typo": 1}, **common)
    with pytest.raises(ValueError, match="needs a model"):
        build_adapter("pi", options={"base_url": "x"}, **{**common, "model": None})
    assert build_adapter("pi", options={"base_url": "http://x/v1"}, **common) is not None
