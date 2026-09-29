"""Checks every harness adapter applies to the worker's shell commands."""

from __future__ import annotations

import re
import shlex

FORCE_FLAGS = {"-f", "--force", "--force-with-lease", "--mirror", "--delete", "-d"}


def pushes_protected(command: str, protected: str = "main") -> bool:
    """True if any `git push` in a shell command targets the protected branch or forces/deletes."""
    for segment in re.split(r"&&|\|\||;|\||\n", command):
        try:
            tokens = shlex.split(segment)
        except ValueError:
            tokens = segment.split()
        if "git" not in tokens or "push" not in tokens:
            continue
        args = tokens[tokens.index("push") + 1 :]
        for arg in args:
            if arg in FORCE_FLAGS or arg.startswith("--force"):
                return True
            dest = arg.lstrip("+").split(":")[-1]
            if dest in (protected, f"refs/heads/{protected}") or arg.startswith("+"):
                return True
    return False


def push_denied_reason(protected: str = "main") -> str:
    return (
        f"Do not push to {protected} or force-push. Push your own branch, open a PR, and "
        "submit it with the merge_request tool; the merge queue lands it."
    )
