"""Scripted adapter for tests and dry runs: records prompts, optionally runs a callback per turn."""

from __future__ import annotations

import itertools
from collections.abc import Awaitable, Callable

from .base import HarnessAdapter, TurnOutcome

_ids = itertools.count(1)


class FakeAdapter(HarnessAdapter):
    def __init__(self, on_turn: Callable[[str], Awaitable[TurnOutcome | None]] | None = None) -> None:
        self.prompts: list[str] = []
        self.system_prompt: str | None = None
        self.starts: list[str | None] = []
        self.on_turn = on_turn
        self.closed = False

    async def start(self, system_prompt: str, resume: str | None = None) -> None:
        self.system_prompt = system_prompt
        self.starts.append(resume)
        self.session_id = resume or f"fake-session-{next(_ids)}"
        self.closed = False

    async def run_turn(self, prompt: str) -> TurnOutcome:
        self.prompts.append(prompt)
        outcome = await self.on_turn(prompt) if self.on_turn else None
        outcome = outcome or TurnOutcome()
        outcome.session_id = self.session_id
        return outcome

    async def close(self) -> None:
        self.closed = True
