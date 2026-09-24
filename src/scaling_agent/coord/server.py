"""Coordination server: MCP tools for workers + HTTP API for the worker runtime and the launcher.

One process, one SQLite database. Workers call the MCP tools (board, claims, messages, merge
queue) with a per-worker bearer token; every tool result carries the HIGH-priority updates that
arrived since the worker's last infrastructure call (direct messages, overlapping claims,
merge-queue bounces, relevant board entries), which is how peers reach a worker mid-turn.
"""

from __future__ import annotations

import asyncio
import contextlib
import hmac
import json
import logging
import secrets
import time
from collections.abc import Awaitable, Callable
from typing import Any

from mcp.server.mcpserver import Context, MCPServer
from starlette.applications import Starlette
from starlette.requests import Request
from starlette.responses import JSONResponse, PlainTextResponse, Response
from starlette.routing import Mount, Route

from ..config import CoordSettings
from ..protocol import BoardType, Event, Priority, TurnReport
from ..workspace.gitea import GiteaClient, verify_signature
from ..workspace.conflicts import GitConflictChecker
from ..workspace.merge_queue import CommandVerifier, MergeQueue
from ..workspace.routing import route_webhook
from ..prompts import render
from .render import render_updates
from .store import SYSTEM, Store, ValidationError

log = logging.getLogger(__name__)

MAX_BODY = 5_000_000

INSTRUCTIONS = (
    "Organization infrastructure for self-organized workers: shared context board, work claims, "
    "channel and direct messages, and the merge queue (the only way into main). Every result may end "
    "with an UPDATES block: act on it (FAIL -> stop doing that; overlapping CLAIM -> settle by DM)."
)


class Coordinator:
    """Shared state and background tasks behind the HTTP app."""

    def __init__(self, settings: CoordSettings) -> None:
        self.settings = settings
        self.store = Store(settings.db_path, claim_ttl_s=settings.claim_ttl_s, hot_file_threshold=settings.hot_file_threshold)
        self.gitea: GiteaClient | None = None
        self.merge_queue: MergeQueue | None = None
        self._stop = asyncio.Event()
        self._tasks: list[asyncio.Task[Any]] = []
        self._wakeups: dict[str, asyncio.Event] = {}
        self.webhook_secret: str | None = None

    async def start(self) -> None:
        await self.store.open()
        self.store.on_enqueue = self.wake
        requeued = await self.store.reset_inflight_merges()
        if requeued:
            log.warning("re-queued %d merge requests left in flight by a previous run", requeued)
        g = self.settings.gitea
        self.webhook_secret = g.webhook_secret or await self.store.kv_get("gitea_webhook_secret")
        if g.admin_user and g.admin_password:
            self._tasks.append(asyncio.create_task(self._bootstrap_then_queue(), name="gitea-bootstrap"))
        elif g.bot_token:
            self._start_merge_queue(g.bot_token)

    async def _bootstrap_then_queue(self) -> None:
        """Idempotently prepare the shared workspace, retrying until Gitea is up."""
        delay = 2.0
        while not self._stop.is_set():
            try:
                bot_token = await self._bootstrap_gitea()
                break
            except Exception as e:  # Gitea not ready yet, or transient API failure
                log.warning("gitea bootstrap failed (%s); retrying in %.0fs", e, delay)
                await asyncio.sleep(delay)
                delay = min(delay * 2, 30)
        else:
            return
        self._start_merge_queue(bot_token)

    async def _bootstrap_gitea(self) -> str:
        g = self.settings.gitea
        admin = GiteaClient(g.url, username=g.admin_user, password=g.admin_password)
        try:
            await admin.ensure_org(g.owner)
            await admin.ensure_repo(g.owner, g.repo, g.main_branch)
            await admin.ensure_user(g.bot_user, secrets.token_urlsafe(24))
            await admin.add_collaborator(g.owner, g.repo, g.bot_user, "admin")
            bot_token = g.bot_token or await self.store.kv_get("gitea_bot_token")
            if not bot_token:
                bot_token = await admin.create_token(
                    g.bot_user, f"merge-queue-{secrets.token_hex(4)}", ["write:repository", "write:issue"]
                )
                await self.store.kv_set("gitea_bot_token", bot_token)
            if not self.webhook_secret:
                self.webhook_secret = secrets.token_urlsafe(32)
                await self.store.kv_set("gitea_webhook_secret", self.webhook_secret)
            await admin.protect_branch(g.owner, g.repo, g.main_branch, g.bot_user)
            await admin.ensure_webhook(
                g.owner, g.repo, self.settings.self_url.rstrip("/") + "/api/gitea/webhook", self.webhook_secret
            )
            log.info("gitea workspace %s/%s ready (main protected; only %s merges)", g.owner, g.repo, g.bot_user)
            return bot_token
        finally:
            await admin.close()

    def _start_merge_queue(self, bot_token: str) -> None:
        if not self.settings.merge_queue_enabled:
            return
        g = self.settings.gitea
        self.gitea = GiteaClient(g.url, bot_token)
        verifier = None
        if self.settings.verify_command:
            log.warning(
                "verify_command runs PR code on this host with this process's uid: anything it can read "
                "(the database, tokens) a malicious PR can read. Use it only with trusted workers, or run "
                "verification in an isolated sandbox (see docs/DESIGN.md)."
            )
            verifier = CommandVerifier(
                f"{g.url.rstrip('/')}/{g.owner}/{g.repo}.git",
                bot_token,
                g.main_branch,
                self.settings.verify_command,
                self.settings.verify_timeout_s,
            )
        conflicts = None
        if self.settings.conflict_mirror_dir:
            conflicts = GitConflictChecker(
                self.settings.conflict_mirror_dir, f"{g.url.rstrip('/')}/{g.owner}/{g.repo}.git", bot_token, g.main_branch
            )
        self.merge_queue = MergeQueue(
            self.store, self.gitea, g.owner, g.repo, g.main_branch, verifier,
            allow_foreign_submissions=self.settings.allow_foreign_merge_requests,
            conflicts=conflicts,
        )
        self._tasks.append(asyncio.create_task(self._keep_running(self.merge_queue.run, "merge-queue")))

    async def _keep_running(self, fn: Callable[[asyncio.Event], Awaitable[None]], name: str) -> None:
        """Restart a background loop if it ever dies, so the merge queue cannot silently stop."""
        delay = 1.0
        while not self._stop.is_set():
            try:
                await fn(self._stop)
                return
            except asyncio.CancelledError:
                raise
            except Exception:
                log.exception("%s crashed; restarting in %.0fs", name, delay)
                await asyncio.sleep(delay)
                delay = min(delay * 2, 60)

    async def stop(self) -> None:
        self._stop.set()
        for task in self._tasks:
            task.cancel()
        for task in self._tasks:
            with contextlib.suppress(asyncio.CancelledError, Exception):
                await task
        if self.gitea:
            await self.gitea.close()
        await self.store.close()

    def wake(self, worker: str | None = None) -> None:
        """Wake long-polling runtimes (one worker, or everyone)."""
        targets = [self._wakeups[worker]] if worker and worker in self._wakeups else (
            list(self._wakeups.values()) if worker is None else []
        )
        for ev in targets:
            ev.set()

    def wakeup_event(self, worker: str) -> asyncio.Event:
        return self._wakeups.setdefault(worker, asyncio.Event())


def _bearer(headers: Any) -> str | None:
    if not headers:
        return None
    value = headers.get("authorization") or headers.get("Authorization")
    if value and value.lower().startswith("bearer "):
        return value[7:].strip()
    return None


def build_mcp(coord: Coordinator) -> MCPServer:
    mcp = MCPServer(name="scaling-agent-coord", instructions=INSTRUCTIONS)
    store = coord.store
    s = coord.settings

    async def run_tool(ctx: Context, name: str, fn: Callable[[str], Awaitable[str]], args: dict[str, Any]) -> str:
        token = _bearer(ctx.headers)
        worker = await store.authenticate(token) if token else None
        if worker is None:
            return "ERROR: unauthenticated (missing or invalid bearer token)"
        started = time.time()
        await store.touch(worker)  # liveness also renews the caller's claims
        ok = True
        try:
            body = await fn(worker)
        except ValidationError as e:
            ok = False
            body = f"ERROR: {e}"
        events, entries, omitted = await store.drain_high(worker, board_mode=s.board_mode, max_board=s.max_board_mid_turn)
        await store.trace(
            worker,
            "tool",
            {"name": name, "ok": ok, "ms": int((time.time() - started) * 1000), "args": args,
             "delivered_events": len(events), "delivered_board": len(entries)},
        )
        updates = render_updates(events, entries, omitted)
        return f"{body}\n\n{updates}" if updates else body

    def parse_type(value: str) -> BoardType:
        try:
            return BoardType(value.strip().upper())
        except ValueError:
            raise ValidationError(f"type must be one of {[t.value for t in BoardType]}") from None

    @mcp.tool()
    async def board_write(type: str, text: str, ctx: Context, detail: str = "", scope: list[str] | None = None) -> str:
        """Publish a short verified note to the shared context board the moment you establish it.

        type: OBSERVED (noticeable behaviour), FACT (confirmed), FAIL (hypothesis you falsified: the
        highest-value entry, it stops peers spending budget on it), PATCH_SUMMARY as
        `files= | idea= | evidence=` after a change lands. Use the `claim` tool (not this) to claim work.
        text is capped at 100 chars (300 for PATCH_SUMMARY); put the long version in `detail`.
        scope defaults to your live claims' scope so the note reaches the peers working nearby.
        """

        async def do(worker: str) -> str:
            t = parse_type(type)
            if t is BoardType.CLAIM:
                raise ValidationError("use the `claim` tool to claim work (it detects overlaps)")
            e = await store.board_write(worker, t, text, detail or None, scope)
            return f"published {e.render()}"

        return await run_tool(ctx, "board_write", do, {"type": type})

    @mcp.tool()
    async def board_read(ctx: Context, limit: int = 200, types: list[str] | None = None, scope: list[str] | None = None) -> str:
        """Read the most recent board entries (max 2000), optionally filtered by type and scope."""

        async def do(worker: str) -> str:
            ts = [parse_type(t) for t in types] if types else None
            entries = await store.board_read(limit=limit, types=ts, scope=scope)
            return "\n".join(e.render() for e in entries) or "(board is empty for this filter)"

        return await run_tool(ctx, "board_read", do, {"limit": limit, "types": types, "scope": scope})

    @mcp.tool()
    async def board_grep(query: str, ctx: Context, limit: int = 100) -> str:
        """Search the complete board history. ',' is OR, '&' is AND (case-insensitive): `a&b,c` = (a AND b) OR c."""

        async def do(worker: str) -> str:
            entries = await store.board_grep(query, limit=limit)
            return "\n".join(e.render() for e in entries) or "(no match)"

        return await run_tool(ctx, "board_grep", do, {"query": query})

    @mcp.tool()
    async def board_unfold(entry_id: int, ctx: Context) -> str:
        """Show the full `detail` of a board entry."""

        async def do(worker: str) -> str:
            found = await store.board_unfold(entry_id)
            if found is None:
                raise ValidationError(f"no board entry #{entry_id}")
            entry, detail = found
            return f"{entry.render()}\n\n{detail or '(no detail)'}"

        return await run_tool(ctx, "board_unfold", do, {"entry_id": entry_id})

    @mcp.tool()
    async def claim(scope: list[str], intent: str, ctx: Context, exclusive: bool = False) -> str:
        """Claim a piece of work before you build it.

        scope: repository paths or globs you will change (e.g. `src/writers/latex.py`, `tests/test_latex*`)
        and/or area tags (`area:latex-writer`). Keep it as narrow as the work. intent: one line on what
        you will deliver. The server checks overlaps with live claims right now: overlapping holders are
        returned to you and notified; settle it by direct message (or pick different work). exclusive=true
        refuses the claim instead of recording it when it overlaps. Claims expire unless you stay active.
        """

        async def do(worker: str) -> str:
            result = await store.claim(worker, scope, intent, exclusive=exclusive)
            now = time.time()
            lines: list[str] = []
            if result.refused:
                lines.append("REFUSED: overlapping live claims (exclusive=true):")
            elif result.claim:
                lines.append(f"claimed {result.claim.render(now)}")
            if result.conflicts:
                if not result.refused:
                    lines.append("WARNING overlaps (holders were notified; settle by direct message):")
                lines.extend(f"- {c.render(now)}" for c in result.conflicts)
            if result.hotspots:
                lines.append(
                    "HOTSPOT files in your scope (many merges/conflicts): "
                    + ", ".join(result.hotspots)
                    + ". Prefer adding a new module that registers itself over editing these files; "
                    "keep any edit to them minimal and land it quickly."
                )
            return "\n".join(lines)

        return await run_tool(ctx, "claim", do, {"scope": scope, "exclusive": exclusive})

    @mcp.tool()
    async def claims(ctx: Context, scope: list[str] | None = None, worker: str = "") -> str:
        """List live claims, optionally only those overlapping `scope` or held by `worker`."""

        async def do(me: str) -> str:
            from . import scope as scopes

            live = await store.active_claims(worker or None)
            if scope:
                norm = scopes.normalize(scope)
                live = [c for c in live if scopes.scopes_overlap(norm, c.scope)]
            now = time.time()
            return "\n".join(c.render(now) for c in live) or "(no live claims match)"

        return await run_tool(ctx, "claims", do, {"scope": scope, "worker": worker})

    @mcp.tool()
    async def release_claim(claim_id: int, outcome: str, ctx: Context, note: str = "") -> str:
        """Release your claim: outcome is `done`, `abandoned`, or `handoff` (say to whom in `note`)."""

        async def do(worker: str) -> str:
            if outcome not in ("done", "abandoned", "handoff"):
                raise ValidationError("outcome must be done, abandoned, or handoff")
            c = await store.release_claim(worker, claim_id, outcome, note or None)
            return f"released claim#{c.id} ({c.outcome})"

        return await run_tool(ctx, "release_claim", do, {"claim_id": claim_id, "outcome": outcome})

    @mcp.tool()
    async def channel_post(text: str, ctx: Context) -> str:
        """Post to the team channel (everyone sees it at their next turn). Be brief and concrete.
        Use it for announcements that change others' work (interfaces, conventions); use DMs for collisions."""

        async def do(worker: str) -> str:
            m = await store.post_channel(worker, text)
            coord.wake()
            return f"posted #{m.id}"

        return await run_tool(ctx, "channel_post", do, {})

    @mcp.tool()
    async def send_dm(to: str, text: str, ctx: Context) -> str:
        """Direct message one peer. It reaches them mid-turn and interrupts what they are doing, so use it
        when two of you are about to edit the same thing or you block each other."""

        async def do(worker: str) -> str:
            m = await store.send_dm(worker, to, text)
            coord.wake(to)
            return f"sent DM #{m.id} to {to}"

        return await run_tool(ctx, "send_dm", do, {"to": to})

    @mcp.tool()
    async def messages(ctx: Context, peer: str = "", limit: int = 30) -> str:
        """Read message history: the team channel, or your DM thread with `peer`."""

        async def do(worker: str) -> str:
            msgs = await store.message_history(worker, peer=peer or None, limit=limit)
            return "\n".join(m.render() for m in msgs) or "(no messages)"

        return await run_tool(ctx, "messages", do, {"peer": peer})

    @mcp.tool()
    async def merge_request(pr_number: int, ctx: Context) -> str:
        """Submit your PR to the merge queue: the only way into main. The queue lands PRs one at a time
        after checking the PR merges cleanly into the current main (and passes verification, if
        configured). A conflict or failure comes back to you as an update with the exact reason."""

        async def do(worker: str) -> str:
            if coord.merge_queue is None:
                raise ValidationError("merge queue is not configured on this server")
            req, pos = await store.merge_submit(worker, pr_number)
            coord.merge_queue.poke()
            return f"PR #{pr_number} is {req.status.value} in the merge queue (position {pos})"

        return await run_tool(ctx, "merge_request", do, {"pr_number": pr_number})

    @mcp.tool()
    async def merge_status(ctx: Context) -> str:
        """Your recent merge-queue requests and the current queue depth."""

        async def do(worker: str) -> str:
            reqs = await store.merge_requests(worker=worker, limit=10)
            depth = await store.queue_depth()
            lines = [f"queue depth: {depth}"]
            lines += [f"PR #{r.pr_number}: {r.status.value} {('- ' + r.detail) if r.detail else ''}" for r in reqs]
            return "\n".join(lines)

        return await run_tool(ctx, "merge_status", do, {})

    @mcp.tool()
    async def hotspots(ctx: Context, limit: int = 15) -> str:
        """Files where claims, merges, and conflicts concentrate. Avoid growing them; extend by adding modules."""

        async def do(worker: str) -> str:
            rows = await store.hotspots(limit=limit)
            return "\n".join(
                f"{h['path']}: claims={h['claims']} merges={h['merges']} conflicts={h['conflicts']}" for h in rows
            ) or "(no hotspots yet)"

        return await run_tool(ctx, "hotspots", do, {})

    return mcp


def build_app(settings: CoordSettings) -> Starlette:
    coord = Coordinator(settings)
    mcp = build_mcp(coord)
    mcp_app = mcp.streamable_http_app(stateless_http=True, json_response=True, host=settings.host)
    store = coord.store

    async def worker_from(request: Request) -> str | None:
        token = _bearer(request.headers)
        return await store.authenticate(token) if token else None

    def is_admin(request: Request) -> bool:
        given = _bearer(request.headers) or ""
        return bool(settings.admin_token) and hmac.compare_digest(given.encode(), settings.admin_token.encode())

    async def healthz(request: Request) -> Response:
        return PlainTextResponse("ok")

    async def wait_pending(worker: str, wait_s: float) -> bool:
        deadline = time.time() + min(wait_s, settings.long_poll_s)
        wake = coord.wakeup_event(worker)
        while True:
            wake.clear()  # clear before checking, or a wakeup between check and wait is lost
            if await store.has_pending(worker):
                return True
            remaining = deadline - time.time()
            if remaining <= 0:
                return False
            with contextlib.suppress(TimeoutError):
                await asyncio.wait_for(wake.wait(), max(0.1, min(5.0, remaining)))

    async def wait_turn(request: Request) -> Response:
        """Long-poll without leasing: has anything arrived that should start a turn?"""
        worker = await worker_from(request)
        if worker is None:
            return JSONResponse({"error": "unauthenticated"}, status_code=401)
        body = await request.json() if await request.body() else {}
        await store.touch(worker)
        pending = await wait_pending(worker, float(body.get("wait_s", settings.long_poll_s)))
        return JSONResponse({"pending": pending})

    async def next_turn(request: Request) -> Response:
        """Lease the next turn batch, after waiting (bounded by long_poll_s) for something to arrive."""
        worker = await worker_from(request)
        if worker is None:
            return JSONResponse({"error": "unauthenticated"}, status_code=401)
        body = await request.json() if await request.body() else {}
        await store.touch(worker)
        await wait_pending(worker, float(body.get("wait_s", 0)))
        batch = await store.next_batch(worker, board_mode=settings.board_mode, recent_board=settings.recent_board_in_prompt)
        claims = await store.active_claims(worker)
        return JSONResponse({"batch": batch.model_dump(mode="json"), "claims": [c.model_dump(mode="json") for c in claims]})

    async def ack_turn(request: Request) -> Response:
        worker = await worker_from(request)
        if worker is None:
            return JSONResponse({"error": "unauthenticated"}, status_code=401)
        report = TurnReport.model_validate(await request.json())
        await store.touch(worker)
        started = await store.ack(worker, report.lease_id, redeliver=report.redeliver)
        if started is None:
            return JSONResponse({"error": "unknown lease"}, status_code=404)
        await store.trace(worker, "turn", report.model_dump(mode="json"))
        wrote = await store.wrote_board_since(worker, started)
        return JSONResponse({"wrote_board": wrote})

    async def drain(request: Request) -> Response:
        """Mid-turn updates for the runtime's PostToolUse hook.

        Agensh only piggybacks updates on infrastructure (MCP) tool results, so a worker deep in a
        long Bash/Edit sequence misses urgent DMs. The hook calls this after every tool, so updates
        land within one tool call wherever the worker is. Draining is idempotent across both paths.
        """
        worker = await worker_from(request)
        if worker is None:
            return JSONResponse({"error": "unauthenticated"}, status_code=401)
        await store.touch(worker)  # a worker deep in Bash/Edit is alive: keep its claims
        events, entries, omitted = await store.drain_high(
            worker, board_mode=settings.board_mode, max_board=settings.max_board_mid_turn
        )
        if events or entries:
            await store.trace(worker, "drain", {"events": len(events), "board": len(entries)})
        return JSONResponse({"text": render_updates(events, entries, omitted)})

    async def handoff(request: Request) -> Response:
        """State a fresh session needs after rotation: live claims, own recent notes, merge requests."""
        worker = await worker_from(request)
        if worker is None:
            return JSONResponse({"error": "unauthenticated"}, status_code=401)
        now = time.time()
        claims = await store.active_claims(worker)
        own = [e for e in await store.board_grep(worker, limit=200) if e.agent == worker][-15:]
        reqs = await store.merge_requests(worker=worker, limit=10)
        text = "\n".join(
            ["Handoff from your previous session (your context was reset; state lives in the infrastructure)."]
            + ["Live claims:"] + [f"- {c.render(now)}" for c in claims]
            + ["Your recent board notes:"] + [f"- {e.render()}" for e in own]
            + ["Your merge requests:"] + [f"- PR #{r.pr_number}: {r.status.value} {r.detail or ''}" for r in reqs]
        )
        return JSONResponse({"text": text, "empty": not (claims or own or reqs)})

    async def ingest_trace(request: Request) -> Response:
        worker = await worker_from(request)
        if worker is None:
            return JSONResponse({"error": "unauthenticated"}, status_code=401)
        records = (await request.json()).get("records", [])
        n = await store.trace_many(worker, records[:1000])
        return JSONResponse({"stored": n})

    async def admin_workers(request: Request) -> Response:
        if not is_admin(request):
            return JSONResponse({"error": "forbidden"}, status_code=403)
        ids = (await request.json()).get("ids", [])
        tokens = {wid: await store.register_worker(wid) for wid in ids}
        return JSONResponse({"tokens": tokens})

    async def admin_prompt(request: Request) -> Response:
        """Store the run's prompt templates and values; workers fetch their rendered copy at start."""
        if not is_admin(request):
            return JSONResponse({"error": "forbidden"}, status_code=403)
        body = await request.json()
        await store.kv_set("prompt", {k: body[k] for k in ("system_template", "card_template", "values")})
        return JSONResponse({"ok": True})

    async def worker_prompt(request: Request) -> Response:
        worker = await worker_from(request)
        if worker is None:
            return JSONResponse({"error": "unauthenticated"}, status_code=401)
        spec = await store.kv_get("prompt")
        if not spec:
            return JSONResponse({"error": "no prompt registered for this run"}, status_code=404)
        values = {**spec["values"], "worker_id": worker}
        return JSONResponse(
            {
                "system_prompt": render(spec["system_template"], **values),
                "protocol_card": render(spec["card_template"], **values),
            }
        )

    async def admin_broadcast(request: Request) -> Response:
        """System announcement to every worker (e.g. the T-45min / T-5min reminders)."""
        if not is_admin(request):
            return JSONResponse({"error": "forbidden"}, status_code=403)
        body = await request.json()
        text = body["text"]
        priority = Priority.HIGH if body.get("urgent") else Priority.LOW
        now = time.time()
        key = body.get("key") or f"broadcast-{int(now)}"
        workers = await store.worker_ids()
        kind = body.get("kind", "announcement")
        n = await store.enqueue_many(
            (w, Event(event_id=key, source=SYSTEM, kind=kind, priority=priority, observed_at=now, summary=text))
            for w in workers
        )
        await store.post_channel(SYSTEM, text)
        coord.wake()
        return JSONResponse({"queued": n})

    async def admin_status(request: Request) -> Response:
        if not is_admin(request):
            return JSONResponse({"error": "forbidden"}, status_code=403)
        return JSONResponse(await store.status())

    async def gitea_webhook(request: Request) -> Response:
        body = await request.body()
        secret = coord.webhook_secret
        if not secret or not verify_signature(secret, body, request.headers.get("x-gitea-signature")):
            return JSONResponse({"error": "bad signature"}, status_code=401)
        event_type = request.headers.get("x-gitea-event", "")
        delivery = request.headers.get("x-gitea-delivery") or str(hash(body))
        payload = json.loads(body)
        workers = set(await store.worker_ids())
        targets = route_webhook(
            event_type, delivery, payload, workers, await store.active_claims(), settings.gitea.main_branch
        )
        n = await store.enqueue_many(targets)
        for worker, _ in targets:
            coord.wake(worker)
        return JSONResponse({"routed": n})

    @contextlib.asynccontextmanager
    async def lifespan(app: Starlette):
        await coord.start()
        async with mcp_app.router.lifespan_context(mcp_app):
            yield
        await coord.stop()

    app = Starlette(
        routes=[
            Route("/healthz", healthz),
            Route("/api/turns/wait", wait_turn, methods=["POST"]),
            Route("/api/turns/next", next_turn, methods=["POST"]),
            Route("/api/turns/ack", ack_turn, methods=["POST"]),
            Route("/api/drain", drain, methods=["POST"]),
            Route("/api/handoff", handoff, methods=["GET"]),
            Route("/api/trace", ingest_trace, methods=["POST"], max_body_size=MAX_BODY),
            Route("/api/prompt", worker_prompt, methods=["GET"]),
            Route("/api/admin/workers", admin_workers, methods=["POST"]),
            Route("/api/admin/prompt", admin_prompt, methods=["PUT"]),
            Route("/api/admin/broadcast", admin_broadcast, methods=["POST"]),
            Route("/api/admin/status", admin_status, methods=["GET"]),
            # Unauthenticated until the signature is checked: cap the body before buffering it.
            Route("/api/gitea/webhook", gitea_webhook, methods=["POST"], max_body_size=MAX_BODY),
            Mount("/", app=mcp_app),  # MCP endpoint at /mcp
        ],
        lifespan=lifespan,
    )
    app.state.coord = coord
    return app
