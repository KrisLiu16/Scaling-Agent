"""Importing a project into the shared repository, and workers adopting the checkout their image has."""

from __future__ import annotations

import subprocess
from pathlib import Path

import pytest

from scaling_agent.config import GiteaSettings, RunConfig, RuntimeSettings
from scaling_agent.launcher import Launcher
from scaling_agent.runtime.main import bootstrap_git
from scaling_agent.workspace.gitea import GiteaError
from scaling_agent.workspace.seed import MARKER, seed_repository

GIT = ["git", "-c", "user.name=t", "-c", "user.email=t@t"]


def git(*args: str, cwd: Path) -> str:
    done = subprocess.run([*GIT, *args], cwd=cwd, capture_output=True, text=True, check=True)
    return done.stdout.strip()


@pytest.fixture
def remote(tmp_path) -> Path:
    """A bare repository whose main has the README commit Gitea's auto_init would make."""
    bare = tmp_path / "remote.git"
    subprocess.run(["git", "init", "--bare", "-b", "main", str(bare)], check=True, capture_output=True)
    work = tmp_path / "init"
    work.mkdir()
    git("init", "-b", "main", cwd=work)
    (work / "README.md").write_text("auto init\n")
    git("add", "-A", cwd=work)
    git("commit", "-m", "Initial commit", cwd=work)
    git("push", str(bare), "main", cwd=work)
    return bare


@pytest.fixture
def project(tmp_path) -> Path:
    src = tmp_path / "project"
    (src / "pkg").mkdir(parents=True)
    (src / "pkg" / "core.py").write_text("VALUE = 1\n")
    (src / "run_tests.sh").write_text("echo ok\n")
    (src / ".git").mkdir()  # never copied
    (src / ".git" / "HEAD").write_text("ref: refs/heads/other\n")
    return src


def test_seed_replaces_the_tree_once_and_keeps_history(tmp_path, remote, project):
    assert seed_repository(remote.as_uri(), "tok", project, "main") is True
    check = tmp_path / "check"
    subprocess.run(["git", "clone", "-q", str(remote), str(check)], check=True)
    assert sorted(p.name for p in check.iterdir() if p.name != ".git") == ["pkg", "run_tests.sh"]
    assert (check / "pkg" / "core.py").read_text() == "VALUE = 1\n"
    assert git("log", "--format=%s", cwd=check).splitlines() == [MARKER, "Initial commit"]  # a fast-forward
    assert seed_repository(remote.as_uri(), "tok", project, "main") is False  # already there
    assert git("rev-list", "--count", "HEAD", cwd=check) == "2"


def test_seed_token_is_not_on_the_command_line(remote, project, monkeypatch):
    seen: list[list[str]] = []
    real = subprocess.run

    def spy(cmd, *a, **kw):
        seen.append(list(cmd))
        return real(cmd, *a, **kw)

    monkeypatch.setattr("scaling_agent.workspace.seed.subprocess.run", spy)
    seed_repository(remote.as_uri(), "SECRET-TOKEN", project, "main")
    assert seen and all("SECRET-TOKEN" not in " ".join(cmd) for cmd in seen)


class FakeGitea:
    def __init__(self, fail_first: int = 0) -> None:
        self.calls: list[tuple[str, ...]] = []
        self.fail_first = fail_first

    async def create_token(self, user, name, scopes):
        self.calls.append(("create", user, name))
        if self.fail_first:
            self.fail_first -= 1
            raise GiteaError(404, "no such user yet")
        return "bot-token"

    async def delete_token(self, user, name):
        self.calls.append(("delete", user, name))


def launcher_with(gitea: FakeGitea, seed_dir: str) -> Launcher:
    run = RunConfig(task_file="t.md", gitea=GiteaSettings(url="http://gitea:3000/", seed_dir=seed_dir))
    lch = Launcher.__new__(Launcher)
    lch.run, lch.gitea = run, gitea
    lch._clock = lambda: 0.0

    async def no_sleep(_):
        return None

    lch._sleep = no_sleep
    return lch


async def test_launcher_seeds_as_the_merge_bot_and_removes_its_token(monkeypatch, tmp_path):
    gitea = FakeGitea(fail_first=2)  # the coordination server creates the bot while it bootstraps
    got: dict = {}
    monkeypatch.setattr(
        "scaling_agent.launcher.seed_repository",
        lambda url, token, seed_dir, branch: got.update(url=url, token=token, seed=seed_dir, branch=branch) or True,
    )
    await launcher_with(gitea, str(tmp_path))._seed_repository()
    assert got == {"url": "http://gitea:3000/agents/workspace.git", "token": "bot-token", "seed": tmp_path, "branch": "main"}
    kinds = [c[0] for c in gitea.calls]
    assert kinds == ["create", "create", "create", "delete"] and gitea.calls[0][1] == "merge-bot"


async def test_launcher_removes_the_token_even_when_the_import_fails(monkeypatch, tmp_path):
    gitea = FakeGitea()

    def boom(*a):
        raise RuntimeError("push rejected")

    monkeypatch.setattr("scaling_agent.launcher.seed_repository", boom)
    with pytest.raises(RuntimeError, match="push rejected"):
        await launcher_with(gitea, str(tmp_path))._seed_repository()
    assert gitea.calls[-1][0] == "delete"


def test_adopting_an_existing_checkout(tmp_path, remote, project, monkeypatch):
    seed_repository(remote.as_uri(), "tok", project, "main")
    # the image's own checkout: same files, different history, an untracked build artifact, no remote
    app = tmp_path / "app"
    app.mkdir()
    git("init", "-b", "main", cwd=app)
    (app / "pkg").mkdir()
    (app / "pkg" / "core.py").write_text("VALUE = 1\n")
    (app / "run_tests.sh").write_text("echo ok\n")
    git("add", "-A", cwd=app)
    git("commit", "-m", "image history", cwd=app)
    (app / "pkg.egg-info").mkdir()
    (app / "pkg.egg-info" / "PKG-INFO").write_text("editable install\n")
    monkeypatch.setenv("GITEA_REPO_URL", remote.as_uri())
    monkeypatch.setenv("SA_MAIN_BRANCH", "main")
    monkeypatch.delenv("GITEA_TOKEN", raising=False)
    settings = RuntimeSettings(
        worker_id="w1", coord_url="http://c", token="t", workdir=str(app), state_dir=str(tmp_path / "state"), adopt_checkout=True
    )
    bootstrap_git(settings)
    assert git("remote", "get-url", "origin", cwd=app) == remote.as_uri()
    assert git("rev-parse", "HEAD", cwd=app) == git("rev-parse", "origin/main", cwd=app)
    assert git("log", "--format=%s", cwd=app).splitlines()[0] == MARKER
    assert (app / "pkg.egg-info" / "PKG-INFO").exists()  # the environment the image built survives
    assert git("status", "--porcelain", "--untracked-files=no", cwd=app) == ""
    # the worker starts working; a restarted runtime must not throw that away
    (app / "pkg" / "core.py").write_text("VALUE = 2\n")
    git("add", "-A", cwd=app)
    git("commit", "-m", "worker change", cwd=app)
    (app / "scratch.txt").write_text("uncommitted\n")
    (app / "run_tests.sh").write_text("echo edited\n")
    head = git("rev-parse", "HEAD", cwd=app)
    bootstrap_git(settings)
    assert git("rev-parse", "HEAD", cwd=app) == head
    assert (app / "scratch.txt").read_text() == "uncommitted\n" and (app / "run_tests.sh").read_text() == "echo edited\n"


def test_an_existing_checkout_is_left_alone_without_adopt(tmp_path, remote, project, monkeypatch):
    app = tmp_path / "app"
    app.mkdir()
    git("init", "-b", "main", cwd=app)
    (app / "f").write_text("x")
    git("add", "-A", cwd=app)
    git("commit", "-m", "local", cwd=app)
    monkeypatch.setenv("GITEA_REPO_URL", remote.as_uri())
    monkeypatch.delenv("GITEA_TOKEN", raising=False)
    bootstrap_git(RuntimeSettings(worker_id="w1", coord_url="http://c", token="t", workdir=str(app), state_dir=str(tmp_path / "s")))
    assert git("remote", cwd=app) == ""
