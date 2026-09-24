"""SQLite-backed state for the coordination server.

Delivery semantics (see docs/DESIGN.md, "事件投递"):

* Targeted events (DMs, claim conflicts, merge-queue results, workspace notifications, system
  reminders) live in `events` and are delivered at-least-once: a turn batch leases them and they
  are marked delivered only when the worker acknowledges the turn; a crash before the ack
  re-delivers them. HIGH-priority events drained mid-turn are marked delivered immediately.
* Broadcast streams (board entries, channel posts) go through per-worker cursors and are
  at-most-once. Fan-out stays O(1) per write instead of O(N); nothing is lost for good because
  every prompt carries a board snapshot and the full history is readable through tools.
* Board delivery has two modes. `all` forwards every new entry mid-turn (what Agensh does).
  `relevant` forwards mid-turn only the entries whose scope overlaps the recipient's active
  claims, and folds the rest into the next prompt as a capped digest.
"""

from __future__ import annotations

import asyncio
import contextlib
import hashlib
import json
import os
import secrets
import time
import uuid
from collections.abc import AsyncIterator, Callable, Iterable
from typing import Any, Literal

import aiosqlite

from ..protocol import (
    BOARD_READ_LIMIT,
    one_line,
    BoardEntry,
    BoardType,
    Claim,
    ClaimResult,
    Event,
    MergeRequest,
    MergeStatus,
    Message,
    Priority,
    TurnBatch,
    board_text_cap,
)
from . import scope as scopes
from .grep import matches, parse_query

BoardMode = Literal["all", "relevant"]

SCHEMA = """
PRAGMA journal_mode=WAL;
PRAGMA synchronous=NORMAL;
CREATE TABLE IF NOT EXISTS workers (
    id TEXT PRIMARY KEY,
    token_sha256 TEXT UNIQUE NOT NULL,
    created_at REAL NOT NULL,
    last_seen REAL,
    last_board_write REAL,
    board_cursor INTEGER NOT NULL DEFAULT 0,      -- next-turn digest cursor
    board_hot_cursor INTEGER NOT NULL DEFAULT 0,  -- mid-turn delivery cursor
    channel_cursor INTEGER NOT NULL DEFAULT 0,
    state TEXT NOT NULL DEFAULT 'registered'
);
CREATE TABLE IF NOT EXISTS board (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    ts REAL NOT NULL,
    agent TEXT NOT NULL,
    type TEXT NOT NULL,
    text TEXT NOT NULL,
    detail TEXT,
    scope TEXT NOT NULL DEFAULT '[]'
);
CREATE INDEX IF NOT EXISTS board_agent_ts ON board(agent, ts);
CREATE TABLE IF NOT EXISTS claims (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    worker TEXT NOT NULL,
    scope TEXT NOT NULL,
    intent TEXT NOT NULL,
    created_at REAL NOT NULL,
    expires_at REAL NOT NULL,
    released_at REAL,
    outcome TEXT
);
CREATE INDEX IF NOT EXISTS claims_active ON claims(released_at, expires_at);
CREATE TABLE IF NOT EXISTS messages (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    ts REAL NOT NULL,
    sender TEXT NOT NULL,
    channel TEXT,
    recipient TEXT,
    text TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS messages_channel ON messages(channel, id);
CREATE INDEX IF NOT EXISTS messages_dm ON messages(recipient, id);
CREATE TABLE IF NOT EXISTS events (
    seq INTEGER PRIMARY KEY AUTOINCREMENT,
    event_id TEXT NOT NULL,
    worker TEXT NOT NULL,
    ts REAL NOT NULL,
    source TEXT NOT NULL,
    kind TEXT NOT NULL,
    priority INTEGER NOT NULL,
    summary TEXT NOT NULL,
    payload TEXT NOT NULL DEFAULT '{}',
    lease_id TEXT,
    delivered_at REAL,
    UNIQUE(worker, event_id)
);
CREATE INDEX IF NOT EXISTS events_pending ON events(worker, delivered_at, seq);
CREATE TABLE IF NOT EXISTS leases (
    lease_id TEXT PRIMARY KEY,
    worker TEXT NOT NULL,
    created_at REAL NOT NULL,
    acked_at REAL
);
CREATE TABLE IF NOT EXISTS merge_queue (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    pr_number INTEGER NOT NULL,
    worker TEXT NOT NULL,
    status TEXT NOT NULL,
    head_sha TEXT,
    detail TEXT,
    enqueued_at REAL NOT NULL,
    updated_at REAL NOT NULL
);
CREATE INDEX IF NOT EXISTS merge_queue_status ON merge_queue(status, id);
CREATE TABLE IF NOT EXISTS file_heat (
    path TEXT PRIMARY KEY,
    merges INTEGER NOT NULL DEFAULT 0,
    conflicts INTEGER NOT NULL DEFAULT 0,
    last_touched REAL NOT NULL
);
CREATE TABLE IF NOT EXISTS kv (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS trace (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    ts REAL NOT NULL,
    worker TEXT,
    kind TEXT NOT NULL,
    data TEXT NOT NULL DEFAULT '{}'
);
CREATE INDEX IF NOT EXISTS trace_worker ON trace(worker, id);
CREATE INDEX IF NOT EXISTS trace_kind ON trace(kind, id);
"""

MAIN_CHANNEL = "main"
SYSTEM = "system"
CONTROL_KINDS = ("shutdown",)
ACTIVE_MERGE_STATES = (MergeStatus.QUEUED.value, MergeStatus.TESTING.value)


def hash_token(token: str) -> str:
    return hashlib.sha256(token.encode()).hexdigest()


class ValidationError(ValueError):
    """Raised for caller mistakes; the message is shown to the worker verbatim."""


class Store:
    def __init__(self, path: str, claim_ttl_s: float = 1800.0, hot_file_threshold: int = 5) -> None:
        self._path = path
        self._db: aiosqlite.Connection | None = None
        # aiosqlite runs statements on one thread, but multi-statement operations
        # (read cursor -> select -> advance cursor) must not interleave.
        self._lock = asyncio.Lock()
        self._wakes: set[str] = set()
        self.claim_ttl_s = claim_ttl_s
        self.hot_file_threshold = hot_file_threshold
        # Called with a worker id after a transaction that queued an event for it (long-poll wakeup).
        self.on_enqueue: Callable[[str], None] | None = None

    async def open(self) -> None:
        if self._path != ":memory:":
            # Tokens (bot token, webhook secret) live in this file: owner-only.
            os.close(os.open(self._path, os.O_CREAT | os.O_RDWR, 0o600))
            os.chmod(self._path, 0o600)
        self._db = await aiosqlite.connect(self._path)
        self._db.row_factory = aiosqlite.Row
        await self._db.executescript(SCHEMA)
        await self._db.commit()

    async def close(self) -> None:
        if self._db is not None:
            await self._db.close()
            self._db = None

    @property
    def db(self) -> aiosqlite.Connection:
        if self._db is None:
            raise RuntimeError("store is not open")
        return self._db

    @contextlib.asynccontextmanager
    async def _tx(self) -> AsyncIterator[None]:
        """Serialize a multi-statement write and make it atomic: commit on success, roll back on error.

        One shared connection means a failed request must not leave half-written rows for the next
        writer's commit to publish.
        """
        async with self._lock:
            self._wakes = set()
            try:
                yield
                await self.db.commit()
            except BaseException:
                await self.db.rollback()
                raise
            wakes, self._wakes = self._wakes, set()
        if self.on_enqueue is not None:
            for worker in wakes:
                self.on_enqueue(worker)

    async def _fetchall(self, sql: str, params: Iterable[Any] = ()) -> list[aiosqlite.Row]:
        async with self.db.execute(sql, tuple(params)) as cur:
            return list(await cur.fetchall())

    async def _fetchone(self, sql: str, params: Iterable[Any] = ()) -> aiosqlite.Row | None:
        async with self.db.execute(sql, tuple(params)) as cur:
            return await cur.fetchone()

    # ------------------------------------------------------------------ workers

    async def register_worker(self, worker: str, token: str | None = None) -> str:
        """Create a worker (or rotate its token). Returns the plaintext token."""
        token = token or secrets.token_urlsafe(32)
        async with self._tx():
            await self.db.execute(
                "INSERT INTO workers(id, token_sha256, created_at) VALUES (?, ?, ?) "
                "ON CONFLICT(id) DO UPDATE SET token_sha256=excluded.token_sha256",
                (worker, hash_token(token), time.time()),
            )
        return token

    async def authenticate(self, token: str) -> str | None:
        row = await self._fetchone("SELECT id FROM workers WHERE token_sha256=?", (hash_token(token),))
        return row["id"] if row else None

    async def worker_ids(self) -> list[str]:
        return [r["id"] for r in await self._fetchall("SELECT id FROM workers ORDER BY id")]

    async def touch(self, worker: str, state: str | None = None, renew_claims: bool = True) -> None:
        """Record liveness. Any activity renews the worker's claims, so dead workers' claims expire."""
        now = time.time()
        async with self._tx():
            if state:
                await self.db.execute("UPDATE workers SET last_seen=?, state=? WHERE id=?", (now, state, worker))
            else:
                await self.db.execute("UPDATE workers SET last_seen=? WHERE id=?", (now, worker))
            if renew_claims:
                await self.db.execute(
                    "UPDATE claims SET expires_at=max(expires_at, ?) "
                    "WHERE worker=? AND released_at IS NULL AND expires_at>?",
                    (now + self.claim_ttl_s, worker, now),
                )

    # ------------------------------------------------------------------- claims

    @staticmethod
    def _claim_row(row: aiosqlite.Row) -> Claim:
        return Claim(
            id=row["id"],
            worker=row["worker"],
            scope=json.loads(row["scope"]),
            intent=row["intent"],
            created_at=row["created_at"],
            expires_at=row["expires_at"],
            released_at=row["released_at"],
            outcome=row["outcome"],
        )

    async def active_claims(self, worker: str | None = None) -> list[Claim]:
        now = time.time()
        sql = "SELECT * FROM claims WHERE released_at IS NULL AND expires_at>?"
        params: list[Any] = [now]
        if worker:
            sql += " AND worker=?"
            params.append(worker)
        return [self._claim_row(r) for r in await self._fetchall(sql + " ORDER BY id", params)]

    async def interest_scope(self, worker: str) -> list[str]:
        items: list[str] = []
        for claim in await self.active_claims(worker):
            items.extend(claim.scope)
        return scopes.normalize(items)

    async def claim(
        self, worker: str, scope: list[str], intent: str, ttl_s: float | None = None, exclusive: bool = False
    ) -> ClaimResult:
        """Record a claim, detecting overlaps with other workers' live claims at write time.

        Advisory by default: the claim is recorded, the conflicts are returned to the claimant, and
        every conflicting holder gets a HIGH-priority event so both sides can settle it by DM.
        With `exclusive=True` an overlapping claim is refused instead.
        """
        norm = scopes.normalize(scope)
        if not norm:
            raise ValidationError("scope must name at least one path, glob, or area:<tag>")
        intent = " ".join(intent.split())
        if not intent:
            raise ValidationError("intent must not be empty")
        now = time.time()
        ttl = ttl_s if ttl_s is not None else self.claim_ttl_s
        async with self._tx():
            live = [
                self._claim_row(r)
                for r in await self._fetchall(
                    "SELECT * FROM claims WHERE released_at IS NULL AND expires_at>? AND worker!=?", (now, worker)
                )
            ]
            conflicts = [c for c in live if scopes.scopes_overlap(norm, c.scope)]
            hot = await self._hot_paths_locked(norm)
            if conflicts and exclusive:
                return ClaimResult(claim=None, conflicts=conflicts, hotspots=hot, refused=True)
            cur = await self.db.execute(
                "INSERT INTO claims(worker, scope, intent, created_at, expires_at) VALUES (?, ?, ?, ?, ?)",
                (worker, json.dumps(norm), intent, now, now + ttl),
            )
            claim = Claim(id=cur.lastrowid, worker=worker, scope=norm, intent=intent, created_at=now, expires_at=now + ttl)
            # Mirror onto the board so the claim is visible in every peer's context.
            board_text = intent[: board_text_cap(BoardType.CLAIM)]
            await self.db.execute(
                "INSERT INTO board(ts, agent, type, text, detail, scope) VALUES (?, ?, ?, ?, ?, ?)",
                (now, worker, BoardType.CLAIM.value, board_text, f"claim#{claim.id} scope={norm}", json.dumps(norm)),
            )
            await self.db.execute("UPDATE workers SET last_board_write=? WHERE id=?", (now, worker))
            for other in conflicts:
                await self._enqueue_locked(
                    other.worker,
                    Event(
                        event_id=f"claim-conflict-{claim.id}-{other.id}",
                        source="claims",
                        kind="claim_overlap",
                        priority=Priority.HIGH,
                        observed_at=now,
                        summary=(
                            f"{worker} claimed {norm} ({intent!r}), overlapping your claim#{other.id} "
                            f"{other.scope}. Settle it with {worker} by direct message."
                        ),
                        payload={"claim_id": claim.id, "your_claim_id": other.id, "claimant": worker},
                    ),
                )
        return ClaimResult(claim=claim, conflicts=conflicts, hotspots=hot)

    async def release_claim(self, worker: str, claim_id: int, outcome: str, note: str | None = None) -> Claim:
        now = time.time()
        async with self._tx():
            row = await self._fetchone("SELECT * FROM claims WHERE id=? AND worker=?", (claim_id, worker))
            if row is None:
                raise ValidationError(f"claim#{claim_id} is not yours")
            await self.db.execute(
                "UPDATE claims SET released_at=?, outcome=? WHERE id=? AND released_at IS NULL",
                (now, outcome if not note else f"{outcome}: {note}", claim_id),
            )
            row = await self._fetchone("SELECT * FROM claims WHERE id=?", (claim_id,))
        return self._claim_row(row)

    # -------------------------------------------------------------------- board

    async def board_write(
        self,
        agent: str,
        entry_type: BoardType,
        text: str,
        detail: str | None = None,
        scope: list[str] | None = None,
    ) -> BoardEntry:
        text = " ".join(text.split())
        if not text:
            raise ValidationError("text must not be empty")
        cap = board_text_cap(entry_type)
        if len(text) > cap:
            raise ValidationError(
                f"{entry_type.value} text is capped at {cap} chars (got {len(text)}); move the long version into `detail`"
            )
        # Entries inherit the author's current claim scope so relevance routing works without extra effort.
        norm = scopes.normalize(scope) if scope else await self.interest_scope(agent)
        now = time.time()
        async with self._tx():
            cur = await self.db.execute(
                "INSERT INTO board(ts, agent, type, text, detail, scope) VALUES (?, ?, ?, ?, ?, ?)",
                (now, agent, entry_type.value, text, detail or None, json.dumps(norm)),
            )
            await self.db.execute("UPDATE workers SET last_board_write=? WHERE id=?", (now, agent))
        return BoardEntry(
            id=cur.lastrowid, ts=now, agent=agent, type=entry_type, text=text, has_detail=bool(detail), scope=norm
        )

    @staticmethod
    def _board_row(row: aiosqlite.Row) -> BoardEntry:
        return BoardEntry(
            id=row["id"],
            ts=row["ts"],
            agent=row["agent"],
            type=BoardType(row["type"]),
            text=row["text"],
            has_detail=bool(row["detail"]),
            scope=json.loads(row["scope"] or "[]"),
        )

    async def board_read(
        self,
        limit: int = BOARD_READ_LIMIT,
        types: Iterable[BoardType] | None = None,
        scope: list[str] | None = None,
    ) -> list[BoardEntry]:
        limit = max(1, min(limit, BOARD_READ_LIMIT))
        sql = "SELECT * FROM board"
        params: list[Any] = []
        type_list = [t.value for t in types] if types else []
        if type_list:
            sql += f" WHERE type IN ({','.join('?' * len(type_list))})"
            params.extend(type_list)
        sql += " ORDER BY id DESC"
        norm = scopes.normalize(scope)
        out: list[BoardEntry] = []
        async with self.db.execute(sql, params) as cur:
            async for row in cur:
                entry = self._board_row(row)
                if norm and not scopes.scopes_overlap(norm, entry.scope):
                    continue
                out.append(entry)
                if len(out) >= limit:
                    break
        return list(reversed(out))

    async def board_grep(self, query: str, limit: int = 200) -> list[BoardEntry]:
        clauses = parse_query(query)
        if not clauses:
            return []
        # Prefilter in SQL on the first term of each clause, then apply exact DNF semantics.
        where = " OR ".join(
            "(lower(text) LIKE ? OR lower(coalesce(detail,'')) LIKE ? OR lower(scope) LIKE ? OR lower(agent) LIKE ?)"
            for _ in clauses
        )
        params: list[Any] = []
        for clause in clauses:
            params.extend([f"%{clause[0]}%"] * 4)
        out: list[BoardEntry] = []
        async with self.db.execute(f"SELECT * FROM board WHERE {where} ORDER BY id DESC", params) as cur:
            async for row in cur:
                haystack = f"{row['type']} {row['agent']} {row['text']} {row['detail'] or ''} {row['scope']}"
                if matches(haystack, clauses):
                    out.append(self._board_row(row))
                    if len(out) >= limit:
                        break
        return list(reversed(out))

    async def board_unfold(self, entry_id: int) -> tuple[BoardEntry, str | None] | None:
        row = await self._fetchone("SELECT * FROM board WHERE id=?", (entry_id,))
        if row is None:
            return None
        return self._board_row(row), row["detail"]

    async def wrote_board_since(self, worker: str, since: float) -> bool:
        return await self._fetchone("SELECT 1 FROM board WHERE agent=? AND ts>=? LIMIT 1", (worker, since)) is not None

    # ----------------------------------------------------------------- messages

    async def post_channel(self, sender: str, text: str, channel: str = MAIN_CHANNEL) -> Message:
        text = one_line(text)
        if not text:
            raise ValidationError("text must not be empty")
        now = time.time()
        async with self._tx():
            cur = await self.db.execute(
                "INSERT INTO messages(ts, sender, channel, text) VALUES (?, ?, ?, ?)", (now, sender, channel, text)
            )
        return Message(id=cur.lastrowid, ts=now, sender=sender, channel=channel, text=text)

    async def send_dm(self, sender: str, recipient: str, text: str) -> Message:
        text = one_line(text)
        if not text:
            raise ValidationError("text must not be empty")
        if recipient == sender:
            raise ValidationError("cannot send a direct message to yourself")
        now = time.time()
        async with self._tx():
            if await self._fetchone("SELECT 1 FROM workers WHERE id=?", (recipient,)) is None:
                raise ValidationError(f"unknown worker {recipient!r}")
            cur = await self.db.execute(
                "INSERT INTO messages(ts, sender, recipient, text) VALUES (?, ?, ?, ?)", (now, sender, recipient, text)
            )
            msg = Message(id=cur.lastrowid, ts=now, sender=sender, recipient=recipient, text=text)
            await self._enqueue_locked(
                recipient,
                Event(
                    event_id=f"dm-{msg.id}",
                    source="messages",
                    kind="direct_message",
                    priority=Priority.HIGH,
                    observed_at=now,
                    summary=f"DM from {sender}: {text}",
                    payload={"message_id": msg.id, "sender": sender},
                ),
            )
        return msg

    async def message_history(
        self, worker: str, channel: str | None = MAIN_CHANNEL, peer: str | None = None, limit: int = 50
    ) -> list[Message]:
        limit = max(1, min(limit, 500))
        if peer:
            rows = await self._fetchall(
                "SELECT * FROM messages WHERE (sender=? AND recipient=?) OR (sender=? AND recipient=?) "
                "ORDER BY id DESC LIMIT ?",
                (worker, peer, peer, worker, limit),
            )
        else:
            rows = await self._fetchall(
                "SELECT * FROM messages WHERE channel=? ORDER BY id DESC LIMIT ?", (channel or MAIN_CHANNEL, limit)
            )
        return [
            Message(id=r["id"], ts=r["ts"], sender=r["sender"], channel=r["channel"], recipient=r["recipient"], text=r["text"])
            for r in reversed(rows)
        ]

    # ------------------------------------------------------------------- events

    async def _enqueue_locked(self, worker: str, event: Event) -> bool:
        cur = await self.db.execute(
            "INSERT OR IGNORE INTO events(event_id, worker, ts, source, kind, priority, summary, payload) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            (
                event.event_id,
                worker,
                event.observed_at,
                event.source,
                event.kind,
                int(event.priority),
                event.summary,
                json.dumps(event.payload, ensure_ascii=False),
            ),
        )
        if cur.rowcount > 0:
            self._wakes.add(worker)
        return cur.rowcount > 0

    async def enqueue(self, worker: str, event: Event) -> bool:
        """Queue an event for one worker. Returns False for a duplicate `event_id` (dedupe)."""
        async with self._tx():
            inserted = await self._enqueue_locked(worker, event)
        return inserted

    async def enqueue_many(self, targets: Iterable[tuple[str, Event]]) -> int:
        n = 0
        async with self._tx():
            for worker, event in targets:
                n += await self._enqueue_locked(worker, event)
        return n

    @staticmethod
    def _event_row(row: aiosqlite.Row) -> Event:
        return Event(
            event_id=row["event_id"],
            source=row["source"],
            kind=row["kind"],
            priority=Priority(row["priority"]),
            observed_at=row["ts"],
            summary=row["summary"],
            payload=json.loads(row["payload"]),
        )

    async def has_pending(self, worker: str) -> bool:
        """Cheap check used by long-polling: should this worker start a turn now?

        Only targeted events and channel posts wake a worker. Board entries don't: at scale some
        peer writes the board every second, which would keep every worker permanently busy with
        digests. They reach the worker mid-turn (if relevant) or in the next turn's digest.
        """
        row = await self._fetchone("SELECT channel_cursor FROM workers WHERE id=?", (worker,))
        if row is None:
            return False
        if await self._fetchone("SELECT 1 FROM events WHERE worker=? AND delivered_at IS NULL LIMIT 1", (worker,)):
            return True
        return (
            await self._fetchone(
                "SELECT 1 FROM messages WHERE channel=? AND id>? AND sender!=? LIMIT 1",
                (MAIN_CHANNEL, row["channel_cursor"], worker),
            )
            is not None
        )

    async def next_batch(
        self,
        worker: str,
        board_mode: BoardMode = "relevant",
        recent_board: int = 30,
        max_items: int = 100,
    ) -> TurnBatch:
        """Lease everything pending for `worker` into one turn batch."""
        lease_id = uuid.uuid4().hex
        now = time.time()
        interest = await self.interest_scope(worker)
        async with self._tx():
            row = await self._fetchone(
                "SELECT board_cursor, board_hot_cursor, channel_cursor FROM workers WHERE id=?", (worker,)
            )
            if row is None:
                raise KeyError(f"unknown worker {worker!r}")
            board_cursor, hot_cursor, channel_cursor = row["board_cursor"], row["board_hot_cursor"], row["channel_cursor"]

            # Pending targeted events, including ones leased by a turn that never acked.
            event_rows = await self._fetchall(
                "SELECT * FROM events WHERE worker=? AND delivered_at IS NULL ORDER BY priority DESC, seq LIMIT ?",
                (worker, max_items),
            )
            if event_rows:
                await self.db.execute(
                    f"UPDATE events SET lease_id=? WHERE seq IN ({','.join('?' * len(event_rows))})",
                    (lease_id, *[r["seq"] for r in event_rows]),
                )

            channel_rows = await self._fetchall(
                "SELECT * FROM messages WHERE channel=? AND id>? ORDER BY id DESC LIMIT ?",
                (MAIN_CHANNEL, channel_cursor, max_items),
            )
            head_msg = await self._fetchone("SELECT max(id) AS m FROM messages WHERE channel=?", (MAIN_CHANNEL,))
            new_channel_cursor = max(channel_cursor, head_msg["m"] or 0)

            # New board entries since the digest cursor, minus the ones already forwarded mid-turn.
            board_rows = await self._fetchall(
                "SELECT * FROM board WHERE id>? AND agent!=? ORDER BY id DESC LIMIT ?",
                (board_cursor, worker, max_items * 4),
            )
            new_board: list[BoardEntry] = []
            for r in board_rows:
                entry = self._board_row(r)
                already_forwarded = entry.id <= hot_cursor and (
                    board_mode == "all" or scopes.scopes_overlap(interest, entry.scope)
                )
                if not already_forwarded:
                    new_board.append(entry)
                if len(new_board) >= max_items:
                    break
            head = await self._fetchone("SELECT max(id) AS m FROM board")
            head_id = head["m"] or 0
            await self.db.execute(
                "UPDATE workers SET board_cursor=?, board_hot_cursor=?, channel_cursor=?, last_seen=?, state='active' "
                "WHERE id=?",
                (max(board_cursor, head_id), max(hot_cursor, head_id), new_channel_cursor, now, worker),
            )
            await self.db.execute("INSERT INTO leases(lease_id, worker, created_at) VALUES (?, ?, ?)", (lease_id, worker, now))

        snapshot = await self.board_read(limit=recent_board) if recent_board > 0 else []
        if interest and recent_board > 0:
            relevant = await self.board_read(limit=recent_board, scope=interest)
            known = {e.id for e in snapshot}
            snapshot = sorted(snapshot + [e for e in relevant if e.id not in known], key=lambda e: e.id)
        return TurnBatch(
            lease_id=lease_id,
            worker=worker,
            events=[self._event_row(r) for r in event_rows],
            channel_messages=[
                Message(id=r["id"], ts=r["ts"], sender=r["sender"], channel=r["channel"], text=r["text"])
                for r in reversed(channel_rows)
                if r["sender"] != worker
            ],
            new_board_entries=list(reversed(new_board)),
            recent_board=snapshot,
        )

    async def ack(self, worker: str, lease_id: str, redeliver: bool = False) -> float | None:
        """Close a lease. Returns the lease start time (None if unknown or foreign).

        Normally the lease's events become delivered. With `redeliver=True` (the harness crashed or
        timed out before consuming the prompt) they are released and go into the next batch again.
        """
        now = time.time()
        async with self._tx():
            row = await self._fetchone(
                "SELECT created_at, acked_at FROM leases WHERE lease_id=? AND worker=?", (lease_id, worker)
            )
            if row is None:
                return None
            if row["acked_at"] is None:
                if redeliver:
                    await self.db.execute(
                        "UPDATE events SET lease_id=NULL WHERE lease_id=? AND worker=? AND delivered_at IS NULL",
                        (lease_id, worker),
                    )
                else:
                    await self.db.execute(
                        "UPDATE events SET delivered_at=? WHERE lease_id=? AND worker=? AND delivered_at IS NULL",
                        (now, lease_id, worker),
                    )
                await self.db.execute("UPDATE leases SET acked_at=? WHERE lease_id=?", (now, lease_id))
            return row["created_at"]

    async def drain_high(
        self, worker: str, board_mode: BoardMode = "relevant", max_board: int = 20
    ) -> tuple[list[Event], list[BoardEntry], int]:
        """Pull HIGH-priority items for mid-turn delivery.

        Returns (events, board entries, number of relevant entries not shown because of `max_board`).
        """
        now = time.time()
        interest = await self.interest_scope(worker)
        async with self._tx():
            # Skip events already leased into the current turn's prompt, and control events that the
            # runtime itself must see in a turn batch (shutdown).
            event_rows = await self._fetchall(
                "SELECT * FROM events WHERE worker=? AND delivered_at IS NULL AND lease_id IS NULL "
                f"AND priority>=? AND kind NOT IN ({','.join('?' * len(CONTROL_KINDS))}) ORDER BY seq",
                (worker, int(Priority.HIGH), *CONTROL_KINDS),
            )
            if event_rows:
                await self.db.execute(
                    f"UPDATE events SET delivered_at=? WHERE seq IN ({','.join('?' * len(event_rows))})",
                    (now, *[r["seq"] for r in event_rows]),
                )
            row = await self._fetchone("SELECT board_hot_cursor FROM workers WHERE id=?", (worker,))
            hot_cursor = row["board_hot_cursor"] if row else 0
            rows = await self._fetchall(
                "SELECT * FROM board WHERE id>? AND agent!=? ORDER BY id", (hot_cursor, worker)
            )
            candidates = [self._board_row(r) for r in rows]
            if board_mode == "relevant":
                candidates = [e for e in candidates if interest and scopes.scopes_overlap(interest, e.scope)]
            shown = candidates[-max_board:]  # newest are the most useful mid-turn
            head = await self._fetchone("SELECT max(id) AS m FROM board")
            await self.db.execute(
                "UPDATE workers SET board_hot_cursor=?, last_seen=? WHERE id=?",
                (max(hot_cursor, head["m"] or 0), now, worker),
            )
        return [self._event_row(r) for r in event_rows], shown, len(candidates) - len(shown)

    # -------------------------------------------------------------- merge queue

    @staticmethod
    def _merge_row(row: aiosqlite.Row) -> MergeRequest:
        return MergeRequest(
            id=row["id"],
            pr_number=row["pr_number"],
            worker=row["worker"],
            status=MergeStatus(row["status"]),
            head_sha=row["head_sha"],
            detail=row["detail"],
            enqueued_at=row["enqueued_at"],
            updated_at=row["updated_at"],
        )

    async def merge_submit(self, worker: str, pr_number: int) -> tuple[MergeRequest, int]:
        """Queue a PR for landing. Idempotent per PR while it is still queued. Returns (request, position)."""
        now = time.time()
        async with self._tx():
            row = await self._fetchone(
                f"SELECT * FROM merge_queue WHERE pr_number=? AND status IN ({','.join('?' * len(ACTIVE_MERGE_STATES))})",
                (pr_number, *ACTIVE_MERGE_STATES),
            )
            if row is None:
                cur = await self.db.execute(
                    "INSERT INTO merge_queue(pr_number, worker, status, enqueued_at, updated_at) VALUES (?, ?, ?, ?, ?)",
                    (pr_number, worker, MergeStatus.QUEUED.value, now, now),
                )
                row = await self._fetchone("SELECT * FROM merge_queue WHERE id=?", (cur.lastrowid,))
            pos = await self._fetchone(
                f"SELECT count(*) AS n FROM merge_queue WHERE id<=? AND status IN ({','.join('?' * len(ACTIVE_MERGE_STATES))})",
                (row["id"], *ACTIVE_MERGE_STATES),
            )
        return self._merge_row(row), pos["n"]

    async def reset_inflight_merges(self) -> int:
        """At startup: requests left in TESTING by a crash or restart go back to the queue."""
        async with self._tx():
            cur = await self.db.execute(
                "UPDATE merge_queue SET status=?, updated_at=? WHERE status=?",
                (MergeStatus.QUEUED.value, time.time(), MergeStatus.TESTING.value),
            )
        return cur.rowcount

    async def merge_next(self) -> MergeRequest | None:
        """Claim the oldest queued request for processing (single consumer)."""
        async with self._tx():
            row = await self._fetchone(
                "SELECT * FROM merge_queue WHERE status=? ORDER BY id LIMIT 1", (MergeStatus.QUEUED.value,)
            )
            if row is None:
                return None
            await self.db.execute(
                "UPDATE merge_queue SET status=?, updated_at=? WHERE id=?", (MergeStatus.TESTING.value, time.time(), row["id"])
            )
            row = await self._fetchone("SELECT * FROM merge_queue WHERE id=?", (row["id"],))
        return self._merge_row(row)

    async def merge_update(
        self, request_id: int, status: MergeStatus, detail: str | None = None, head_sha: str | None = None
    ) -> MergeRequest:
        async with self._tx():
            await self.db.execute(
                "UPDATE merge_queue SET status=?, detail=?, head_sha=coalesce(?, head_sha), updated_at=? WHERE id=?",
                (status.value, detail, head_sha, time.time(), request_id),
            )
            row = await self._fetchone("SELECT * FROM merge_queue WHERE id=?", (request_id,))
        return self._merge_row(row)

    async def merge_requests(self, worker: str | None = None, active_only: bool = False, limit: int = 50) -> list[MergeRequest]:
        sql = "SELECT * FROM merge_queue WHERE 1=1"
        params: list[Any] = []
        if worker:
            sql += " AND worker=?"
            params.append(worker)
        if active_only:
            sql += f" AND status IN ({','.join('?' * len(ACTIVE_MERGE_STATES))})"
            params.extend(ACTIVE_MERGE_STATES)
        rows = await self._fetchall(sql + " ORDER BY id DESC LIMIT ?", (*params, limit))
        return [self._merge_row(r) for r in rows]

    async def queue_depth(self) -> int:
        row = await self._fetchone(
            f"SELECT count(*) AS n FROM merge_queue WHERE status IN ({','.join('?' * len(ACTIVE_MERGE_STATES))})",
            ACTIVE_MERGE_STATES,
        )
        return row["n"]

    # ----------------------------------------------------------------- hotspots

    async def record_file_heat(self, paths: Iterable[str], merged: bool = False, conflicted: bool = False) -> None:
        now = time.time()
        async with self._tx():
            for path in {scopes.normalize_item(p) for p in paths}:
                await self.db.execute(
                    "INSERT INTO file_heat(path, merges, conflicts, last_touched) VALUES (?, ?, ?, ?) "
                    "ON CONFLICT(path) DO UPDATE SET merges=merges+excluded.merges, "
                    "conflicts=conflicts+excluded.conflicts, last_touched=excluded.last_touched",
                    (path, int(merged), int(conflicted), now),
                )

    async def hotspots(self, limit: int = 20) -> list[dict[str, Any]]:
        """Files that many live claims or recent merges/conflicts touch: the organization's conflict magnets."""
        heat = {
            r["path"]: {"path": r["path"], "merges": r["merges"], "conflicts": r["conflicts"], "claims": 0}
            for r in await self._fetchall("SELECT * FROM file_heat")
        }
        for claim in await self.active_claims():
            for item in claim.scope:
                if item.startswith(scopes.AREA_PREFIX):
                    continue
                heat.setdefault(item, {"path": item, "merges": 0, "conflicts": 0, "claims": 0})["claims"] += 1
        ranked = sorted(heat.values(), key=lambda h: (2 * h["conflicts"] + h["merges"] + 3 * h["claims"]), reverse=True)
        return [h for h in ranked if (2 * h["conflicts"] + h["merges"] + 3 * h["claims"]) > 0][:limit]

    async def _hot_paths_locked(self, scope: list[str]) -> list[str]:
        rows = await self._fetchall(
            "SELECT path FROM file_heat WHERE merges + 2 * conflicts >= ?", (self.hot_file_threshold,)
        )
        return [r["path"] for r in rows if scopes.path_in_scope(r["path"], scope)]

    # ----------------------------------------------------------------------- kv

    async def kv_set(self, key: str, value: Any) -> None:
        async with self._tx():
            await self.db.execute(
                "INSERT INTO kv(key, value) VALUES (?, ?) ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                (key, json.dumps(value, ensure_ascii=False)),
            )

    async def kv_get(self, key: str, default: Any = None) -> Any:
        row = await self._fetchone("SELECT value FROM kv WHERE key=?", (key,))
        return json.loads(row["value"]) if row else default

    # -------------------------------------------------------------------- trace

    async def trace(self, worker: str | None, kind: str, data: dict[str, Any] | None = None) -> None:
        async with self._tx():
            await self.db.execute(
                "INSERT INTO trace(ts, worker, kind, data) VALUES (?, ?, ?, ?)",
                (time.time(), worker, kind, json.dumps(data or {}, ensure_ascii=False, default=str)),
            )

    async def trace_many(self, worker: str | None, records: Iterable[dict[str, Any]]) -> int:
        n = 0
        async with self._tx():
            for rec in records:
                await self.db.execute(
                    "INSERT INTO trace(ts, worker, kind, data) VALUES (?, ?, ?, ?)",
                    (
                        float(rec.get("ts") or time.time()),
                        worker,
                        str(rec.get("kind") or "runtime"),
                        json.dumps(rec.get("data") or {}, ensure_ascii=False, default=str),
                    ),
                )
                n += 1
        return n

    async def status(self) -> dict[str, Any]:
        """Fleet KPIs for dashboards, the launcher's backpressure, and post-hoc analysis."""
        now = time.time()
        out: dict[str, Any] = {"ts": now}
        rows = await self._fetchall("SELECT state, count(*) AS n FROM workers GROUP BY state")
        out["workers"] = {r["state"]: r["n"] for r in rows}
        stale = await self._fetchone("SELECT count(*) AS n FROM workers WHERE last_seen < ?", (now - 600,))
        out["workers_silent_10m"] = stale["n"]
        out["board"] = {r["type"]: r["n"] for r in await self._fetchall("SELECT type, count(*) AS n FROM board GROUP BY type")}
        row = await self._fetchone(
            "SELECT sum(channel IS NOT NULL) AS channel, sum(recipient IS NOT NULL) AS dm FROM messages"
        )
        out["messages"] = {"channel": row["channel"] or 0, "dm": row["dm"] or 0}
        claims = await self.active_claims()
        conflicts = await self._fetchone("SELECT count(*) AS n FROM events WHERE kind='claim_overlap'")
        out["claims"] = {"active": len(claims), "overlap_events": conflicts["n"]}
        mq = {r["status"]: r["n"] for r in await self._fetchall("SELECT status, count(*) AS n FROM merge_queue GROUP BY status")}
        landed, bounced = mq.get(MergeStatus.MERGED.value, 0), mq.get(MergeStatus.CONFLICT.value, 0) + mq.get(MergeStatus.FAILED.value, 0)
        out["merge_queue"] = {**mq, "depth": await self.queue_depth(), "land_rate": landed / max(1, landed + bounced)}
        pending = await self._fetchone("SELECT count(*) AS n FROM events WHERE delivered_at IS NULL")
        out["pending_events"] = pending["n"]
        out["hotspots"] = await self.hotspots(limit=5)
        return out
