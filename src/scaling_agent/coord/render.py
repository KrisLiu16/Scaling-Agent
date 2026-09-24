"""Text rendering for what workers see: tool-result update blocks and turn prompts."""

from __future__ import annotations

import time

from ..protocol import BoardEntry, Claim, Event, TurnBatch

UPDATES_HEADER = "==== UPDATES (arrived during your turn) ===="
SHARED_CONTEXT_HEADER = "==== SHARED CONTEXT ===="


def render_updates(events: list[Event], entries: list[BoardEntry], omitted: int) -> str:
    if not events and not entries and not omitted:
        return ""
    lines = [UPDATES_HEADER]
    lines.extend(e.render() for e in events)
    for entry in entries:
        hint = {
            "FAIL": "stop if you are doing this",
            "FACT": "use it",
            "OBSERVED": "use it",
            "CLAIM": "if it overlaps what you are building, settle it by direct message",
            "PATCH_SUMMARY": "sync main before you build on this",
        }.get(entry.type.value, "")
        lines.append(f"[board] {entry.render()}" + (f"  <- {hint}" if hint else ""))
    if omitted:
        lines.append(f"(+{omitted} more relevant board entries; call board_read with your scope to see them)")
    return "\n".join(lines)


def render_turn_prompt(
    batch: TurnBatch,
    protocol_card: str,
    my_claims: list[Claim],
    extra: list[str] | None = None,
    max_digest: int = 40,
) -> str:
    """The message that opens a turn: protocol card, targeted events, digests, board snapshot."""
    now = time.time()
    parts: list[str] = [protocol_card.strip()]
    if my_claims:
        parts.append("Your live claims:\n" + "\n".join(f"- {c.render(now)}" for c in my_claims))
    else:
        parts.append("You hold no live claim. Pick the next piece of work and claim it before you build.")
    if extra:
        parts.extend(extra)
    if batch.events:
        parts.append("\n\n".join(e.render() for e in batch.events))
    if batch.channel_messages:
        parts.append("Channel since your last turn:\n" + "\n".join(m.render() for m in batch.channel_messages[-30:]))
    if batch.new_board_entries:
        shown = batch.new_board_entries[-max_digest:]
        more = len(batch.new_board_entries) - len(shown)
        digest = "\n".join(e.render() for e in shown)
        if more:
            digest += f"\n(+{more} older new entries; use board_grep to search)"
        parts.append("New board entries since your last turn:\n" + digest)
    if batch.recent_board:
        parts.append(SHARED_CONTEXT_HEADER + "\n" + "\n".join(e.render() for e in batch.recent_board))
    return "\n\n".join(parts)
