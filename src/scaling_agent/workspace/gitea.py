"""Minimal async Gitea client covering what the harness needs.

API reference: https://docs.gitea.com/api (paths below are relative to /api/v1).
"""

from __future__ import annotations

import hashlib
import hmac
from dataclasses import dataclass
from typing import Any

import httpx


class GiteaError(RuntimeError):
    def __init__(self, status: int, message: str) -> None:
        super().__init__(f"gitea {status}: {message}")
        self.status = status


@dataclass
class PullRequest:
    number: int
    state: str
    merged: bool
    mergeable: bool
    head_ref: str
    head_sha: str
    base_ref: str
    author: str
    title: str

    @classmethod
    def from_api(cls, data: dict[str, Any]) -> PullRequest:
        return cls(
            number=data["number"],
            state=data["state"],
            merged=bool(data.get("merged")),
            mergeable=bool(data.get("mergeable")),
            head_ref=data["head"]["ref"],
            head_sha=data["head"]["sha"],
            base_ref=data["base"]["ref"],
            author=(data.get("user") or {}).get("login", ""),
            title=data.get("title", ""),
        )


def verify_signature(secret: str, body: bytes, signature: str | None) -> bool:
    """Gitea signs webhook bodies with hex HMAC-SHA256 in the X-Gitea-Signature header."""
    if not signature:
        return False
    expected = hmac.new(secret.encode(), body, hashlib.sha256).hexdigest()
    return hmac.compare_digest(expected, signature.strip())


class GiteaClient:
    """Authenticates with an API token, or with Basic auth (username/password).

    Basic auth is required for creating access tokens (`POST /users/{u}/tokens` rejects token
    auth); an admin's Basic auth can create tokens for other users.
    """

    def __init__(
        self,
        base_url: str,
        token: str | None = None,
        username: str | None = None,
        password: str | None = None,
        timeout: float = 30.0,
    ) -> None:
        headers = {"Accept": "application/json"}
        auth = None
        if token:
            headers["Authorization"] = f"token {token}"
        elif username and password:
            auth = httpx.BasicAuth(username, password)
        else:
            raise ValueError("GiteaClient needs a token or a username/password")
        self._http = httpx.AsyncClient(
            base_url=base_url.rstrip("/") + "/api/v1", headers=headers, auth=auth, timeout=timeout
        )

    async def close(self) -> None:
        await self._http.aclose()

    async def _request(self, method: str, path: str, *, sudo: str | None = None, **kwargs: Any) -> Any:
        headers = kwargs.pop("headers", {}) or {}
        if sudo:
            headers["Sudo"] = sudo  # act as another user (admin tokens only)
        resp = await self._http.request(method, path, headers=headers, **kwargs)
        if resp.status_code >= 400:
            raise GiteaError(resp.status_code, resp.text[:500])
        if resp.status_code == 204 or not resp.content:
            return None
        return resp.json()

    # ------------------------------------------------------------- bootstrap

    async def ensure_org(self, org: str) -> None:
        try:
            await self._request("GET", f"/orgs/{org}")
        except GiteaError as e:
            if e.status != 404:
                raise
            await self._request("POST", "/orgs", json={"username": org, "visibility": "private"})

    async def ensure_repo(self, owner: str, repo: str, default_branch: str = "main") -> None:
        try:
            await self._request("GET", f"/repos/{owner}/{repo}")
        except GiteaError as e:
            if e.status != 404:
                raise
            await self._request(
                "POST",
                f"/orgs/{owner}/repos",
                json={"name": repo, "auto_init": True, "default_branch": default_branch, "private": True},
            )

    async def ensure_user(self, username: str, password: str) -> None:
        try:
            await self._request("GET", f"/users/{username}")
        except GiteaError as e:
            if e.status != 404:
                raise
            await self._request(
                "POST",
                "/admin/users",
                json={
                    "username": username,
                    "email": f"{username}@agents.invalid",
                    "password": password,
                    "must_change_password": False,
                },
            )

    async def create_token(self, username: str, name: str, scopes: list[str]) -> str:
        """Needs a client using an admin's Basic auth."""
        data = await self._request("POST", f"/users/{username}/tokens", json={"name": name, "scopes": scopes})
        return data["sha1"]

    async def add_collaborator(self, owner: str, repo: str, username: str, permission: str = "write") -> None:
        await self._request("PUT", f"/repos/{owner}/{repo}/collaborators/{username}", json={"permission": permission})

    async def protect_branch(
        self, owner: str, repo: str, branch: str, merger: str, pushers: list[str] | None = None
    ) -> None:
        """Only `pushers` (default: just `merger`) may push to `branch` (a name or glob such as
        `w0001/*`), only `merger` may merge into it, and nobody may force-push or delete it."""
        body = {
            "rule_name": branch,
            "enable_push": True,
            "enable_push_whitelist": True,
            "push_whitelist_usernames": pushers or [merger],
            "enable_merge_whitelist": True,
            "merge_whitelist_usernames": [merger],
            "enable_force_push": False,
        }
        try:
            await self._request("POST", f"/repos/{owner}/{repo}/branch_protections", json=body)
        except GiteaError as e:
            if e.status not in (403, 409, 422):  # already exists
                raise
            await self._request("PATCH", f"/repos/{owner}/{repo}/branch_protections/{branch}", json=body)

    async def ensure_webhook(self, owner: str, repo: str, url: str, secret: str) -> None:
        events = ["push", "pull_request", "issues", "issue_comment", "pull_request_review_comment"]
        config = {"url": url, "content_type": "json", "secret": secret}
        hooks = await self._request("GET", f"/repos/{owner}/{repo}/hooks") or []
        for hook in hooks:
            if (hook.get("config") or {}).get("url") == url:
                # The secret is write-only in Gitea; always reset it so it matches ours (e.g. after
                # the coordination server's database was recreated).
                await self._request(
                    "PATCH", f"/repos/{owner}/{repo}/hooks/{hook['id']}",
                    json={"active": True, "config": config, "events": events},
                )
                return
        await self._request(
            "POST",
            f"/repos/{owner}/{repo}/hooks",
            json={"type": "gitea", "active": True, "config": config, "events": events},
        )

    # ------------------------------------------------------------ pull requests

    async def get_pr(self, owner: str, repo: str, number: int) -> PullRequest:
        return PullRequest.from_api(await self._request("GET", f"/repos/{owner}/{repo}/pulls/{number}"))

    async def pr_files(self, owner: str, repo: str, number: int) -> list[str]:
        files: list[str] = []
        page = 1
        while True:
            batch = await self._request(
                "GET", f"/repos/{owner}/{repo}/pulls/{number}/files", params={"page": page, "limit": 50}
            )
            if not batch:
                return files
            files.extend(f["filename"] for f in batch)
            if len(batch) < 50:
                return files
            page += 1

    async def merge_pr(self, owner: str, repo: str, number: int, head_sha: str, title: str | None = None) -> None:
        """Merge; `head_commit_id` makes Gitea refuse if the branch moved after verification."""
        body: dict[str, Any] = {"Do": "merge", "head_commit_id": head_sha, "delete_branch_after_merge": True}
        if title:
            body["MergeTitleField"] = title
        await self._request("POST", f"/repos/{owner}/{repo}/pulls/{number}/merge", json=body)

    async def ensure_task_issue(self, owner: str, repo: str, title: str, body: str) -> int:
        """Issue #1 is the task (Agensh convention). Creates it on a fresh repository."""
        try:
            await self._request("GET", f"/repos/{owner}/{repo}/issues/1")
            return 1
        except GiteaError as e:
            if e.status != 404:
                raise
        data = await self._request("POST", f"/repos/{owner}/{repo}/issues", json={"title": title, "body": body})
        return int(data["number"])

    async def comment(self, owner: str, repo: str, number: int, body: str) -> None:
        await self._request("POST", f"/repos/{owner}/{repo}/issues/{number}/comments", json={"body": body})
