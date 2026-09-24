"""Configuration for the three processes: coordination server, worker runtime, launcher.

Secrets come from environment variables; everything else can live in a YAML run file
(see examples/run.example.yaml).
"""

from __future__ import annotations

from pathlib import Path
from typing import Literal

import yaml
from pydantic import BaseModel, Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class GiteaSettings(BaseModel):
    """Shared workspace. Workers push branches and open PRs; only the merge-queue bot lands on main."""

    url: str = "http://gitea:3000"  # address reachable from the coordination server
    public_url: str | None = None  # address reachable from worker sandboxes (defaults to `url`)
    owner: str = "agents"  # note: "org" is reserved in Gitea
    repo: str = "workspace"
    main_branch: str = "main"
    # Admin Basic auth: create users, tokens (the token API requires Basic auth), repo, webhook,
    # branch protection. With these set, the coordination server bootstraps Gitea on startup.
    admin_user: str | None = None
    admin_password: str | None = None
    bot_user: str = "merge-bot"
    bot_token: str | None = None  # the only identity allowed to merge into main (created if absent)
    webhook_secret: str | None = None  # generated if absent

    @property
    def worker_url(self) -> str:
        return self.public_url or self.url


class CoordSettings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="SA_COORD_", env_nested_delimiter="__")

    db_path: str = "coord.sqlite3"
    host: str = "0.0.0.0"
    port: int = 8700
    admin_token: str = Field(default="", description="bearer token for /api/admin/* and the launcher")
    self_url: str = "http://127.0.0.1:8700"  # how Gitea reaches this server (webhook target)
    board_mode: Literal["all", "relevant"] = "relevant"
    claim_ttl_s: float = 1800.0
    hot_file_threshold: int = 5
    recent_board_in_prompt: int = 30
    max_board_mid_turn: int = 20
    long_poll_s: float = 25.0
    gitea: GiteaSettings = Field(default_factory=GiteaSettings)
    # Merge queue: land PRs one at a time after re-verifying the merged result.
    merge_queue_enabled: bool = True
    verify_command: str | None = None  # run in a temp checkout of main+PR; empty = trust Gitea mergeability only
    verify_timeout_s: float = 900.0
    # Bare mirror used to detect merge conflicts with `git merge-tree` (exact, ~100ms) instead of
    # waiting on Gitea's asynchronous mergeability flag. Empty = fall back to the flag.
    conflict_mirror_dir: str = "coord-mirror.git"
    # Only a PR's author may submit it to the merge queue unless this is on (e.g. to let
    # integrator workers land peers' PRs by agreement).
    allow_foreign_merge_requests: bool = False


class RuntimeSettings(BaseSettings):
    """Settings for the worker runtime that runs inside each sandbox."""

    model_config = SettingsConfigDict(env_prefix="SA_WORKER_")

    worker_id: str
    coord_url: str  # e.g. https://coord.example.com
    token: str  # per-worker bearer token for the coordination server
    workdir: str = "/workspace/repo"  # the git checkout
    state_dir: str = "/workspace/state"  # runtime state, git credentials, harness transcripts (outside the checkout)
    harness: Literal["claude_code", "scripted", "fake"] = "claude_code"
    model: str | None = None
    # With nothing new for this long, start a "continue" turn (Agensh waits 10 minutes).
    continue_after_s: float = 60.0
    max_continue_after_s: float = 600.0  # idle backoff cap
    board_reminder: bool = True
    # Session rotation against protocol drift in long contexts: start a fresh session after N turns
    # or past a context-usage threshold; state lives outside the session (claims, board, git,
    # merge queue), so a handoff note is enough to resume.
    rotate_after_turns: int = 60
    rotate_at_context_pct: float = 70.0
    max_turn_s: float = 3600.0


class StaggerPhase(BaseModel):
    until_s: float | None = None  # phase lasts until this offset from launch start (None = forever)
    interval_s: float


class Reminder(BaseModel):
    before_end_s: float
    text: str


class ProviderSettings(BaseModel):
    kind: Literal["ags", "k8s", "local"] = "local"
    # Tencent Cloud AGS. Credentials come from TENCENTCLOUD_SECRET_ID / TENCENTCLOUD_SECRET_KEY in the
    # launcher's environment and never enter a sandbox. Console rid=9 is ap-singapore.
    region: str = "ap-singapore"
    endpoint: str = "ags.tencentcloudapi.com"  # or http(s)://host:port (e.g. the local mock AGS)
    data_plane_domain: str | None = None  # default: {region}.tencentags.com
    # Send all envd traffic to one gateway URL (routed by the E2b-Sandbox-Id header) instead of the
    # per-sandbox `{port}-{id}.{domain}` hosts. Used with the local mock AGS.
    data_plane_url: str | None = None
    # Extra envd flags in the tool's start command (the e2b-built envd needs
    # "-isnotfc -no-cgroups" outside Firecracker; AGS's own envd needs none).
    envd_flags: str = ""
    tool_name: str = "sa-worker"
    image: str | None = None  # linux/amd64 image that contains /usr/bin/envd (see deploy/worker.Dockerfile)
    image_registry_type: Literal["enterprise", "personal", "custom"] = "personal"
    role_arn: str | None = None  # required by AGS to pull from TCR/CCR and to mount storage
    cpu: str = "2"
    memory: str = "4Gi"
    disk: Literal["1Gi", "5Gi", "10Gi", "20Gi"] | None = "20Gi"
    network_mode: Literal["PUBLIC", "VPC", "SANDBOX"] = "PUBLIC"
    subnet_ids: list[str] = Field(default_factory=list)  # VPC mode (NAT gateway needed for model API egress)
    security_group_ids: list[str] = Field(default_factory=list)
    persistent: bool = True  # 常驻沙箱: a property of the tool, only for ToolType=custom
    # None = omit Timeout when starting instances of a persistent tool. For time-limited tools use
    # e.g. "24h"; the launcher then keeps them alive with UpdateSandboxInstance before expiry.
    instance_timeout: str | None = None
    keepalive_timeout: str = "24h"
    envd_version: str = "0.5.14"
    runtime_command: str = "scaling-agent worker run"
    # Kubernetes provider (plain pods standing in for AGS sandboxes).
    k8s_namespace: str = "scaling-agent"
    k8s_image_pull_policy: Literal["Always", "IfNotPresent", "Never"] = "IfNotPresent"
    k8s_cpu_request: str = "100m"
    k8s_memory_request: str = "256Mi"
    # Local provider (development): run workers as subprocesses on this machine.
    local_root: str = "./.runs"


class RunConfig(BaseModel):
    """One experiment: a task, an organization size, and its schedule."""

    name: str = "run"
    task_file: str
    workers: int = 8
    worker_prefix: str = "w"
    duration_s: float = 6 * 3600
    stagger: list[StaggerPhase] = Field(
        default_factory=lambda: [StaggerPhase(until_s=3600, interval_s=30), StaggerPhase(interval_s=3)]
    )
    # Founding phase: the first N workers bootstrap architecture and interfaces before mass ramp-up.
    founders: int = 4
    founding_s: float = 900
    # Backpressure: pause ramp-up while the merge queue is longer than this per active worker.
    max_queue_per_worker: float = 0.5
    reminders: list[Reminder] = Field(
        default_factory=lambda: [
            Reminder(
                before_end_s=45 * 60,
                text="Stop dispatching new features. Ensure the build works and existing work is merged. "
                "Land open PRs through the merge queue; if a PR cannot land, say so and move on.",
            ),
            Reminder(
                before_end_s=5 * 60,
                text="Submit anything ready to the merge queue, then stop. Confirm the build works on main "
                "and post the final state.",
            ),
        ]
    )
    coord_url: str = "http://127.0.0.1:8700"
    coord_public_url: str | None = None  # address reachable from sandboxes
    harness: Literal["claude_code", "scripted", "fake"] = "claude_code"
    model: str | None = None
    provider: ProviderSettings = Field(default_factory=ProviderSettings)
    gitea: GiteaSettings = Field(default_factory=GiteaSettings)
    # Environment variables forwarded into every worker sandbox (names only; values come from the
    # launcher's own environment so secrets never live in the run file).
    forward_env: list[str] = Field(default_factory=lambda: ["ANTHROPIC_API_KEY", "ANTHROPIC_BASE_URL"])
    # Extra non-secret settings for every worker, e.g. SA_WORKER_CONTINUE_AFTER_S.
    worker_env: dict[str, str] = Field(default_factory=dict)

    @classmethod
    def load(cls, path: str | Path) -> RunConfig:
        return cls.model_validate(yaml.safe_load(Path(path).read_text()))

    def worker_ids(self) -> list[str]:
        width = max(4, len(str(self.workers)))
        return [f"{self.worker_prefix}{i:0{width}d}" for i in range(1, self.workers + 1)]
