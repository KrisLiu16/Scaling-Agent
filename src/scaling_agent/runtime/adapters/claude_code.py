"""Claude Code adapter (via the Claude Agent SDK, which bundles the CLI).

* One persistent `ClaudeSDKClient` session per worker; each turn is one `query()` and ends at the
  `ResultMessage`. The session id is persisted by the runtime so a restart resumes it.
* The organization's MCP server (coordination server, streamable HTTP) is attached with the
  worker's bearer token; its tools show up as `mcp__org__*`.
* A PostToolUse hook asks the coordination server for pending updates after every *other* tool
  (Bash, Edit, ...) and injects them as additional context, so urgent DMs reach a worker within
  one tool call even when it is not touching the infrastructure.
* A PreToolUse guard denies pushing to the main branch from the shell (defense in depth; branch
  protection already enforces it server-side).
"""

from __future__ import annotations

import logging
import re
import shlex
from typing import Any

from claude_agent_sdk import (
    AssistantMessage,
    ClaudeAgentOptions,
    ClaudeSDKClient,
    HookMatcher,
    ResultMessage,
    ToolUseBlock,
)

from .base import DrainFn, HarnessAdapter, TurnOutcome

log = logging.getLogger(__name__)

MCP_NAME = "org"
FORCE_FLAGS = {"-f", "--force", "--force-with-lease", "--mirror", "--delete", "-d"}


def pushes_protected(command: str, protected: str = "main") -> bool:
    """True if any `git push` in a shell command targets the protected branch or forces/deletes."""
    for segment in re.split(r"&&|\|\||;|\||\n", command):
        try:
            tokens = shlex.split(segment)
        except ValueError:
            tokens = segment.split()
        if "git" not in tokens or "push" not in tokens:
            continue
        args = tokens[tokens.index("push") + 1 :]
        for arg in args:
            if arg in FORCE_FLAGS or arg.startswith("--force"):
                return True
            dest = arg.lstrip("+").split(":")[-1]
            if dest in (protected, f"refs/heads/{protected}") or arg.startswith("+"):
                return True
    return False


class ClaudeCodeAdapter(HarnessAdapter):
    def __init__(
        self,
        coord_url: str,
        token: str,
        workdir: str,
        drain: DrainFn | None = None,
        model: str | None = None,
        env: dict[str, str] | None = None,
        main_branch: str = "main",
    ) -> None:
        self._coord_url = coord_url.rstrip("/")
        self._token = token
        self._workdir = workdir
        self._drain = drain
        self._model = model
        self._env = env or {}
        self._main = main_branch
        self._client: ClaudeSDKClient | None = None

    async def _post_tool_use(self, hook_input: dict[str, Any], tool_use_id: str | None, context: Any) -> dict[str, Any]:
        if self._drain is None or str(hook_input.get("tool_name", "")).startswith(f"mcp__{MCP_NAME}__"):
            return {}  # our own MCP tools already carry the updates in their results
        try:
            text = await self._drain()
        except Exception:  # never break the agent loop over a missed update
            log.warning("drain failed", exc_info=True)
            return {}
        if not text:
            return {}
        return {"hookSpecificOutput": {"hookEventName": "PostToolUse", "additionalContext": text}}

    async def _guard_bash(self, hook_input: dict[str, Any], tool_use_id: str | None, context: Any) -> dict[str, Any]:
        command = str((hook_input.get("tool_input") or {}).get("command", ""))
        if pushes_protected(command, self._main):
            return {
                "hookSpecificOutput": {
                    "hookEventName": "PreToolUse",
                    "permissionDecision": "deny",
                    "permissionDecisionReason": (
                        f"Do not push to {self._main} or force-push. Push your own branch, open a PR, and "
                        "submit it with the merge_request tool; the merge queue lands it."
                    ),
                }
            }
        return {}

    async def start(self, system_prompt: str, resume: str | None = None) -> None:
        options = ClaudeAgentOptions(
            system_prompt={"type": "preset", "preset": "claude_code", "append": system_prompt},
            mcp_servers={
                MCP_NAME: {
                    "type": "http",
                    "url": f"{self._coord_url}/mcp",
                    "headers": {"Authorization": f"Bearer {self._token}"},
                }
            },
            permission_mode="bypassPermissions",  # the sandbox is the security boundary
            cwd=self._workdir,
            model=self._model,
            resume=resume,
            # AGS runs the runtime as root; Claude Code only allows bypassPermissions as root when it
            # is told it is inside a sandbox.
            env={
                "IS_SANDBOX": "1",
                "ENABLE_TOOL_SEARCH": "false",  # load the org tools up front instead of deferring them
                "CLAUDE_CODE_DISABLE_AUTO_MEMORY": "1",  # shared state lives in the board, not private memory
                **self._env,
            },
            # Prompts carry peer-written text (DMs, board, PR titles): no @path expansion or slash commands.
            verbatim_prompts=True,
            hooks={
                "PostToolUse": [HookMatcher(matcher=None, hooks=[self._post_tool_use])],
                "PreToolUse": [HookMatcher(matcher="Bash", hooks=[self._guard_bash])],
            },
        )
        self._client = ClaudeSDKClient(options=options)
        await self._client.connect()
        self.session_id = resume

    async def run_turn(self, prompt: str) -> TurnOutcome:
        if self._client is None:
            raise RuntimeError("adapter not started")
        outcome = TurnOutcome()
        await self._client.query(prompt)
        # Our turn ends at the ResultMessage that answers our prompt. Background-task completions
        # produce extra results with a non-human origin; keep reading past those.
        async for message in self._client.receive_messages():
            if isinstance(message, AssistantMessage):
                for block in message.content:
                    if isinstance(block, ToolUseBlock):
                        outcome.tool_calls += 1
                        outcome.tools[block.name] = outcome.tools.get(block.name, 0) + 1
            elif isinstance(message, ResultMessage):
                origin = getattr(message, "origin", None)
                if origin is not None and origin.get("kind") != "human":
                    continue
                outcome.ok = not message.is_error
                outcome.cost_usd = message.total_cost_usd
                outcome.usage = message.usage
                outcome.session_id = message.session_id
                self.session_id = message.session_id
                if message.is_error:
                    outcome.error = "; ".join(message.errors or []) or message.subtype
                break
        try:
            usage = await self._client.get_context_usage()
            outcome.context_pct = float(usage.get("percentage", 0.0))
        except Exception:
            outcome.context_pct = None
        return outcome

    async def close(self) -> None:
        if self._client is not None:
            try:
                await self._client.disconnect()
            finally:
                self._client = None
