"""The per-worker event loop (Agensh appendix B, plus fixes for its weak spots).

    long-poll coordination server ─► turn batch ─► render prompt ─► harness turn ─► ack
          ▲                                                                     │
          └──────────── continue nudge / board reminder / session rotation ◄────┘

Differences from Agensh, on purpose:
* A worker with nothing new continues after `continue_after_s` (default 60s), not 10 minutes.
* The protocol card is re-sent at the top of every turn, so the rules stay near the end of the
  context instead of drifting out of it as the session grows.
* Sessions rotate after `rotate_after_turns` turns or when context usage passes
  `rotate_at_context_pct`: state lives in the infrastructure (claims, board, git, merge queue),
  so a fresh session plus a handoff note resumes the work without the accumulated drift.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import logging
import time
from dataclasses import asdict, dataclass
from pathlib import Path

from ..coord.render import render_turn_prompt
from ..protocol import TurnReport
from .adapters.base import HarnessAdapter, TurnOutcome
from .client import CoordClient

log = logging.getLogger(__name__)

CONTINUE_NUDGE = (
    "Nothing new arrived for you. There is always more work: if you hold a claim, keep going on it; "
    "otherwise look at what the target still does that the shared implementation doesn't, check the "
    "board for gaps and FAILs, and claim the next piece. Do not wait for instructions."
)
BOARD_REMINDER = (
    "Your last turn published nothing to the board. If you established anything a peer could reuse "
    "(a FACT, an OBSERVED behaviour, a FAIL you ruled out, a PATCH_SUMMARY for landed work), publish it now."
)


@dataclass
class RuntimeState:
    session_id: str | None = None
    turns_in_session: int = 0
    total_turns: int = 0
    sessions: int = 0

    @classmethod
    def load(cls, path: Path) -> RuntimeState:
        try:
            return cls(**json.loads(path.read_text()))
        except (FileNotFoundError, json.JSONDecodeError, TypeError):
            return cls()

    def save(self, path: Path) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp = path.with_suffix(".tmp")
        tmp.write_text(json.dumps(asdict(self)))
        tmp.replace(path)


class WorkerRuntime:
    def __init__(
        self,
        worker_id: str,
        coord: CoordClient,
        harness: HarnessAdapter,
        system_prompt: str,
        protocol_card: str,
        state_dir: str,
        continue_after_s: float = 60.0,
        max_continue_after_s: float = 600.0,
        board_reminder: bool = True,
        rotate_after_turns: int = 60,
        rotate_at_context_pct: float = 70.0,
        max_turn_s: float = 3600.0,
    ) -> None:
        self.worker_id = worker_id
        self.coord = coord
        self.harness = harness
        self.system_prompt = system_prompt
        self.protocol_card = protocol_card
        self.state_path = Path(state_dir) / "runtime_state.json"
        self.state = RuntimeState.load(self.state_path)
        self.continue_after_s = continue_after_s
        self.max_continue_after_s = max_continue_after_s
        self.idle_streak = 0
        self.board_reminder = board_reminder
        self.rotate_after_turns = rotate_after_turns
        self.rotate_at_context_pct = rotate_at_context_pct
        self.max_turn_s = max_turn_s
        self._stop = asyncio.Event()
        self._pending_extra: list[str] = []
        self._rotate_next = False
        self._handoff_pending = False
        self._reminded = False

    def stop(self) -> None:
        self._stop.set()

    async def _open_session(self, fresh: bool) -> None:
        """Open the harness session; if resuming fails (transcript gone, CLI error), start fresh."""
        resume = None if fresh else self.state.session_id
        try:
            await self.harness.start(self.system_prompt, resume=resume)
        except Exception:
            if resume is None:
                raise
            log.warning("%s: resuming session %s failed; starting a fresh one", self.worker_id, resume, exc_info=True)
            with contextlib.suppress(Exception):
                await self.harness.close()
            await self.harness.start(self.system_prompt, resume=None)
            resume = None
            await self._queue_handoff()
        if resume is None:
            self.state.session_id = self.harness.session_id
            self.state.turns_in_session = 0
            self.state.sessions += 1
            self.state.save(self.state_path)

    async def _queue_handoff(self, skip_if_empty: bool = False) -> None:
        try:
            text = await self.coord.handoff(skip_if_empty=skip_if_empty)
            if text:
                self._pending_extra.insert(0, text)
                self._handoff_pending = True
        except Exception:
            log.warning("handoff unavailable", exc_info=True)

    async def _rotate(self) -> None:
        log.info("%s: rotating session after %d turns", self.worker_id, self.state.turns_in_session)
        with contextlib.suppress(Exception):
            await self.harness.close()
        await self._open_session(fresh=True)
        await self._queue_handoff()
        await self.coord.trace([{"kind": "session_rotated", "data": {"sessions": self.state.sessions}}])

    async def run(self, max_turns: int | None = None) -> None:
        """Run turns until stopped. Transient failures (coordination server unreachable, harness
        failing to start) are retried with capped exponential backoff instead of killing the worker."""
        turns = 0
        delay = 1.0
        opened = False
        try:
            while not self._stop.is_set() and (max_turns is None or turns < max_turns):
                try:
                    if not opened:
                        fresh = self.state.session_id is None
                        await self._open_session(fresh=fresh)
                        if fresh:
                            # No local state: a first start, or a replacement sandbox (the old one
                            # died with its disk). The organization may still hold this worker's
                            # claims and merge requests; hand them to the new session.
                            await self._queue_handoff(skip_if_empty=True)
                        opened = True
                    await self.step()
                    turns += 1
                    delay = 1.0
                except asyncio.CancelledError:
                    raise
                except Exception:
                    log.exception("%s: step failed; retrying in %.0fs", self.worker_id, delay)
                    with contextlib.suppress(TimeoutError):
                        await asyncio.wait_for(self._stop.wait(), delay)
                    delay = min(delay * 2, 60.0)
        finally:
            with contextlib.suppress(Exception):
                await self.harness.close()

    async def _wait_for_work(self, wait_s: float) -> None:
        """Long-poll (in server-sized slices) until something arrives, the wait is over, or stop."""
        deadline = time.time() + wait_s
        while not self._stop.is_set():
            remaining = deadline - time.time()
            if remaining <= 0 or await self.coord.wait(min(remaining, 25.0)):
                return

    async def step(self) -> TurnOutcome:
        if self._rotate_next or (self.rotate_after_turns and self.state.turns_in_session >= self.rotate_after_turns):
            self._rotate_next = False
            await self._rotate()
        # Idle backoff: a worker with nothing to do should not burn a turn every minute forever.
        # Each consecutive idle turn doubles the wait (capped); any incoming event resets it.
        # Only a pending handoff (fresh session) skips the wait; reminders ride the next turn.
        if self._handoff_pending:
            self._handoff_pending = False
        else:
            backoff = min(self.continue_after_s * (2 ** min(self.idle_streak, 16)), self.max_continue_after_s)
            await self._wait_for_work(backoff)
        batch, claims = await self.coord.next_turn(wait_s=0)
        extra = list(self._pending_extra)
        self._pending_extra.clear()
        if not batch.events and not batch.channel_messages:
            extra.append(CONTINUE_NUDGE)
        if any(e.kind == "shutdown" for e in batch.events):
            self.stop()
        prompt = render_turn_prompt(batch, self.protocol_card, claims, extra)

        started = time.time()
        redeliver = False
        try:
            outcome = await asyncio.wait_for(self.harness.run_turn(prompt), self.max_turn_s)
        except TimeoutError:
            outcome = TurnOutcome(ok=False, error=f"turn exceeded {self.max_turn_s:.0f}s")
            self._rotate_next = redeliver = True
        except asyncio.CancelledError:
            raise
        except Exception as e:  # harness crash: rotate, and put the batch's events back
            log.exception("%s: turn failed", self.worker_id)
            outcome = TurnOutcome(ok=False, error=repr(e))
            self._rotate_next = redeliver = True
        duration = time.time() - started

        wrote_board = await self.coord.ack(
            TurnReport(
                lease_id=batch.lease_id,
                ok=outcome.ok,
                redeliver=redeliver,
                tool_calls=outcome.tool_calls,
                duration_s=duration,
                error=outcome.error,
            )
        )
        nothing_arrived = not batch.events and not batch.channel_messages
        idle = nothing_arrived and not wrote_board
        # Remind once per stretch of work that produced nothing for peers; never nag an idle worker.
        if self.board_reminder and outcome.ok and not wrote_board and outcome.tool_calls > 2 and not self._reminded:
            self._pending_extra.append(BOARD_REMINDER)
            self._reminded = True
        elif wrote_board:
            self._reminded = False
        self.idle_streak = self.idle_streak + 1 if idle else 0
        if outcome.context_pct is not None and outcome.context_pct >= self.rotate_at_context_pct:
            self._rotate_next = True

        self.state.turns_in_session += 1
        self.state.total_turns += 1
        if outcome.session_id:
            self.state.session_id = outcome.session_id
        self.state.save(self.state_path)
        await self.coord.trace(
            [
                {
                    "kind": "turn_detail",
                    "data": {
                        "lease": batch.lease_id,
                        "events": len(batch.events),
                        "channel": len(batch.channel_messages),
                        "new_board": len(batch.new_board_entries),
                        "tools": outcome.tools,
                        "cost_usd": outcome.cost_usd,
                        "usage": outcome.usage,
                        "context_pct": outcome.context_pct,
                        "turn_in_session": self.state.turns_in_session,
                        "idle_streak": self.idle_streak,
                    },
                }
            ]
        )
        return outcome
