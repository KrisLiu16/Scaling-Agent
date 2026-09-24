"""Work scopes: what a claim covers, used for overlap detection and relevance-filtered delivery.

A scope is a list of items. Each item is either
  * an area tag, written `area:<name>` (e.g. `area:latex-writer`), matched case-insensitively; or
  * a repository path or glob (e.g. `src/writers/latex.py`, `src/writers/`, `tests/test_latex*`).

Overlap is deliberately conservative for globs: a false positive costs one direct message
between two workers, while a false negative costs a duplicated or conflicting change.
"""

from __future__ import annotations

from fnmatch import fnmatchcase

WILDCARDS = set("*?[")
AREA_PREFIX = "area:"


def normalize_item(item: str) -> str:
    item = "".join(str(item).split())  # no whitespace (and no newlines) inside a path or tag
    if item.lower().startswith(AREA_PREFIX):
        return AREA_PREFIX + item[len(AREA_PREFIX) :].strip().lower()
    while item.startswith("./"):
        item = item[2:]
    while "//" in item:
        item = item.replace("//", "/")
    return item.lstrip("/")


def normalize(scope: list[str] | None) -> list[str]:
    seen: dict[str, None] = {}
    for raw in scope or []:
        item = normalize_item(raw)
        if item and item != AREA_PREFIX:
            seen[item] = None
    return list(seen)


def _has_wildcard(item: str) -> bool:
    return any(ch in WILDCARDS for ch in item)


def _literal_prefix(item: str) -> str:
    for i, ch in enumerate(item):
        if ch in WILDCARDS:
            return item[:i]
    return item


def _contains(directory: str, path: str) -> bool:
    directory = directory.rstrip("/")
    return bool(directory) and path.startswith(directory + "/")


def items_overlap(a: str, b: str) -> bool:
    a_area, b_area = a.startswith(AREA_PREFIX), b.startswith(AREA_PREFIX)
    if a_area or b_area:
        return a_area and b_area and a == b
    a_glob, b_glob = _has_wildcard(a), _has_wildcard(b)
    if not a_glob and not b_glob:
        return a.rstrip("/") == b.rstrip("/") or _contains(a, b) or _contains(b, a)
    if a_glob and b_glob:
        pa, pb = _literal_prefix(a), _literal_prefix(b)
        return pa.startswith(pb) or pb.startswith(pa)
    literal, pattern = (b, a) if a_glob else (a, b)
    return (
        fnmatchcase(literal, pattern)
        or fnmatchcase(literal.rstrip("/"), pattern)
        or _contains(literal, _literal_prefix(pattern))
        or literal.rstrip("/") == _literal_prefix(pattern).rstrip("/")
    )


def overlapping_items(a: list[str], b: list[str]) -> list[tuple[str, str]]:
    return [(x, y) for x in a for y in b if items_overlap(x, y)]


def scopes_overlap(a: list[str], b: list[str]) -> bool:
    return any(items_overlap(x, y) for x in a for y in b)


def path_in_scope(path: str, scope: list[str]) -> bool:
    path = normalize_item(path)
    return any(not item.startswith(AREA_PREFIX) and items_overlap(path, item) for item in scope)
