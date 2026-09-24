"""Turn Gitea webhook payloads into per-worker events.

Routing is targeted, not broadcast: at 1,000 workers, forwarding every repository event to
everyone is O(N^2) noise. A notification goes to the people involved (author, assignees,
requested reviewers, @-mentions) and, for pushes to main, to the workers whose live claims
overlap the changed files, so they know to sync before they merge.
"""

from __future__ import annotations

import re
import time
from collections.abc import Iterable
from typing import Any

from ..coord import scope as scopes
from ..protocol import Claim, Event, Priority

MENTION = re.compile(r"@([A-Za-z0-9_.\-]+)")


def _logins(users: Iterable[dict[str, Any]] | None) -> set[str]:
    return {u.get("login", "") for u in users or [] if u}


def _mentions(text: str | None, known: set[str]) -> set[str]:
    return {m for m in MENTION.findall(text or "") if m in known}


def route_webhook(
    event_type: str,
    delivery_id: str,
    payload: dict[str, Any],
    workers: set[str],
    claims: list[Claim],
    main_branch: str = "main",
) -> list[tuple[str, Event]]:
    sender = (payload.get("sender") or {}).get("login", "")
    now = time.time()
    targets: dict[str, tuple[str, str]] = {}  # worker -> (kind, summary)

    def add(who: Iterable[str], kind: str, summary: str) -> None:
        for w in who:
            if w in workers and w != sender:
                targets.setdefault(w, (kind, summary))

    if event_type == "pull_request":
        pr = payload.get("pull_request") or {}
        action = payload.get("action", "")
        number = pr.get("number")
        title = pr.get("title", "")
        author = (pr.get("user") or {}).get("login", "")
        summary = f"PR #{number} {action} by {sender}: {title}"
        involved = {author} | _logins(pr.get("assignees")) | _logins(pr.get("requested_reviewers"))
        add(involved | _mentions(pr.get("body"), workers), f"pr_{action}", summary)
    elif event_type in ("issue_comment", "pull_request_review_comment"):
        issue = payload.get("issue") or payload.get("pull_request") or {}
        comment = payload.get("comment") or {}
        number = issue.get("number")
        author = (issue.get("user") or {}).get("login", "")
        body = comment.get("body", "")
        snippet = " ".join(body.split())[:160]
        summary = f"{sender} commented on #{number}: {snippet}"
        add({author} | _logins(issue.get("assignees")) | _mentions(body, workers), "comment", summary)
    elif event_type == "issues":
        issue = payload.get("issue") or {}
        action = payload.get("action", "")
        number = issue.get("number")
        summary = f"issue #{number} {action} by {sender}: {issue.get('title', '')}"
        author = (issue.get("user") or {}).get("login", "")
        add({author} | _logins(issue.get("assignees")) | _mentions(issue.get("body"), workers), f"issue_{action}", summary)
    elif event_type == "push" and payload.get("ref") == f"refs/heads/{main_branch}":
        changed: set[str] = set()
        for commit in payload.get("commits") or []:
            for key in ("added", "modified", "removed"):
                changed.update(commit.get(key) or [])
        if changed:
            by_worker: dict[str, list[str]] = {}
            for claim in claims:
                hit = [p for p in changed if scopes.path_in_scope(p, claim.scope)]
                if hit:
                    by_worker.setdefault(claim.worker, []).extend(hit)
            for worker, paths in by_worker.items():
                shown = ", ".join(sorted(set(paths))[:8])
                add(
                    [worker],
                    "main_advanced_in_your_scope",
                    f"main advanced ({payload.get('after', '')[:10]}) touching files in your claim: {shown}. "
                    "Merge latest main into your branch before you submit.",
                )

    return [
        (
            worker,
            Event(
                event_id=f"gitea-{delivery_id}",
                source="gitea",
                kind=kind,
                priority=Priority.LOW,
                observed_at=now,
                summary=summary,
                payload={"event": event_type},
            ),
        )
        for worker, (kind, summary) in targets.items()
    ]
