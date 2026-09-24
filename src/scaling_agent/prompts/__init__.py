"""Prompt templates (Jinja2): the static worker prompt and the per-turn protocol card."""

from __future__ import annotations

from importlib import resources

from jinja2 import Environment, StrictUndefined


def _env() -> Environment:
    return Environment(undefined=StrictUndefined, keep_trailing_newline=True, autoescape=False)


def template_text(name: str) -> str:
    return resources.files(__package__).joinpath(name).read_text()


def render(template: str, **values: object) -> str:
    return _env().from_string(template).render(**values)
