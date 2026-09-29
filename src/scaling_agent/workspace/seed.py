"""Import an existing project into the shared repository as the starting point of a run."""

from __future__ import annotations

import os
import shutil
import subprocess
import tempfile
from pathlib import Path

MARKER = "Import task repository"


def _git(*args: str, cwd: str, env: dict[str, str]) -> str:
    done = subprocess.run(["git", *args], cwd=cwd, env=env, capture_output=True, text=True, check=False)
    if done.returncode:
        raise RuntimeError(f"git {args[0]} failed: {done.stderr.strip()[-500:]}")
    return done.stdout


def seed_repository(repo_url: str, token: str, seed_dir: Path, branch: str) -> bool:
    """Add one commit to `branch` that makes its tree equal to `seed_dir` (a fast-forward, so a
    protected branch accepts it from its merge identity). False if that import is already there."""
    env = {
        **os.environ,
        "GIT_TERMINAL_PROMPT": "0",
        "GIT_CONFIG_COUNT": "1",  # the token travels in the environment, not on a command line
        "GIT_CONFIG_KEY_0": "http.extraHeader",
        "GIT_CONFIG_VALUE_0": f"Authorization: token {token}",
        "GIT_AUTHOR_NAME": "seed", "GIT_AUTHOR_EMAIL": "seed@agents.invalid",
        "GIT_COMMITTER_NAME": "seed", "GIT_COMMITTER_EMAIL": "seed@agents.invalid",
    }  # fmt: skip
    with tempfile.TemporaryDirectory(prefix="sa-seed-") as tmp:
        repo = str(Path(tmp) / "repo")
        _git("clone", "--quiet", "--branch", branch, repo_url, repo, cwd=tmp, env=env)
        if MARKER in _git("log", "--format=%s", cwd=repo, env=env).splitlines():
            return False
        _git("rm", "-rq", "--ignore-unmatch", ".", cwd=repo, env=env)
        shutil.copytree(seed_dir, repo, dirs_exist_ok=True, ignore=shutil.ignore_patterns(".git"), symlinks=True)
        _git("add", "-A", cwd=repo, env=env)
        _git("commit", "--quiet", "--allow-empty", "-m", MARKER, cwd=repo, env=env)
        _git("push", "--quiet", "origin", f"HEAD:{branch}", cwd=repo, env=env)
    return True
