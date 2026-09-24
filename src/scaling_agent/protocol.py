"""Wire types shared by the coordination server, the worker runtime, and the launcher."""

from __future__ import annotations

import enum
from typing import Any

from pydantic import BaseModel, Field


class Priority(enum.IntEnum):
    """Delivery priority.

    HIGH events (direct messages, new board entries) are appended to the result of the
    worker's next infrastructure tool call, i.e. they reach the worker mid-turn.
    LOW events (channel posts, workspace notifications) queue until the current turn ends
    and are folded into the next prompt.
    """

    LOW = 0
    HIGH = 1


class BoardType(str, enum.Enum):
    OBSERVED = "OBSERVED"
    FACT = "FACT"
    FAIL = "FAIL"
    CLAIM = "CLAIM"
    PATCH_SUMMARY = "PATCH_SUMMARY"


def one_line(text: str) -> str:
    """Collapse peer-written text onto one line.

    Peer text (DMs, channel posts, claim intents, scope items) is rendered into other workers'
    prompts inside line-oriented blocks such as `[event]`. Newlines would let a peer forge whole
    blocks (a fake system announcement, say), so they never survive into rendered text.
    """
    return " ".join(str(text).split())


BOARD_TEXT_CAP = 100
BOARD_PATCH_SUMMARY_CAP = 300
BOARD_READ_LIMIT = 2000


def board_text_cap(entry_type: BoardType) -> int:
    return BOARD_PATCH_SUMMARY_CAP if entry_type is BoardType.PATCH_SUMMARY else BOARD_TEXT_CAP


class BoardEntry(BaseModel):
    id: int
    ts: float
    agent: str
    type: BoardType
    text: str
    has_detail: bool = False
    scope: list[str] = Field(default_factory=list)

    def render(self) -> str:
        more = " (+detail)" if self.has_detail else ""
        where = f" {{{one_line(', '.join(self.scope))}}}" if self.scope else ""
        return f"#{self.id} [{self.type.value}] {self.agent}{where}: {one_line(self.text)}{more}"


class Claim(BaseModel):
    """A lease on a scope of work. Expires unless the holder stays active (any tool call renews it)."""

    id: int
    worker: str
    scope: list[str]
    intent: str
    created_at: float
    expires_at: float
    released_at: float | None = None
    outcome: str | None = None

    def render(self, now: float) -> str:
        ttl = max(0, int(self.expires_at - now))
        return (
            f"claim#{self.id} {self.worker} {{{one_line(', '.join(self.scope))}}} — {one_line(self.intent)} "
            f"(expires in {ttl}s)"
        )


class ClaimResult(BaseModel):
    claim: Claim | None
    conflicts: list[Claim] = Field(default_factory=list)
    hotspots: list[str] = Field(default_factory=list)
    refused: bool = False


class MergeStatus(str, enum.Enum):
    QUEUED = "queued"
    TESTING = "testing"
    MERGED = "merged"
    CONFLICT = "conflict"
    FAILED = "failed"
    CANCELLED = "cancelled"


class MergeRequest(BaseModel):
    id: int
    pr_number: int
    worker: str
    status: MergeStatus
    head_sha: str | None = None
    detail: str | None = None
    enqueued_at: float
    updated_at: float


class Message(BaseModel):
    id: int
    ts: float
    sender: str
    channel: str | None = None
    recipient: str | None = None
    text: str

    def render(self) -> str:
        where = f"#{self.channel}" if self.channel else "DM"
        return f"[{where}] {self.sender}: {one_line(self.text)}"


class Event(BaseModel):
    """One notification addressed to one worker (mirrors Agensh's `[event]` block)."""

    event_id: str
    source: str  # gitea | messages | board | system
    kind: str
    priority: Priority = Priority.LOW
    observed_at: float
    summary: str
    payload: dict[str, Any] = Field(default_factory=dict)

    def render(self) -> str:
        return (
            "[event]\n"
            f"event_id={self.event_id}\n"
            f"source={self.source}\n"
            f"kind={self.kind}\n"
            f"observed_at={self.observed_at:.0f}\n"
            f"summary: {one_line(self.summary)}"
        )


class TurnBatch(BaseModel):
    """Everything the dispatcher folds into one prompt. Acknowledged after the turn completes."""

    lease_id: str
    worker: str
    events: list[Event] = Field(default_factory=list)
    channel_messages: list[Message] = Field(default_factory=list)
    new_board_entries: list[BoardEntry] = Field(default_factory=list)
    recent_board: list[BoardEntry] = Field(default_factory=list)

    @property
    def empty(self) -> bool:
        return not (self.events or self.channel_messages or self.new_board_entries)


class TurnReport(BaseModel):
    """What the worker runtime reports back when acknowledging a turn."""

    lease_id: str
    ok: bool = True
    redeliver: bool = False  # the harness never consumed the prompt: put the lease's events back
    tool_calls: int = 0
    duration_s: float = 0.0
    error: str | None = None
