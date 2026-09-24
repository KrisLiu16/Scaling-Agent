"""Harness adapter interface: how the organization layer drives one single-agent harness.

Agensh keeps this seam deliberately thin so any harness (Claude Code, Copilot, Codex, ...) can
plug in. An adapter owns one persistent session and runs one turn per prompt.
"""

from __future__ import annotations

import abc
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from typing import Any

# Returns rendered mid-turn updates ("" if none); adapters call it after every tool use.
DrainFn = Callable[[], Awaitable[str]]


@dataclass
class TurnOutcome:
    ok: bool = True
    tool_calls: int = 0
    tools: dict[str, int] = field(default_factory=dict)
    cost_usd: float | None = None
    usage: dict[str, Any] | None = None
    session_id: str | None = None
    context_pct: float | None = None  # context window used after the turn, if the harness reports it
    error: str | None = None


class HarnessAdapter(abc.ABC):
    session_id: str | None = None

    @abc.abstractmethod
    async def start(self, system_prompt: str, resume: str | None = None) -> None:
        """Open (or resume) the persistent session."""

    @abc.abstractmethod
    async def run_turn(self, prompt: str) -> TurnOutcome:
        """Send one user message and run until the harness ends its turn."""

    @abc.abstractmethod
    async def close(self) -> None:
        """Close the session (the transcript may be resumed later with `start(resume=...)`)."""
