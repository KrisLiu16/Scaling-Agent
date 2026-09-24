"""`board_grep` query language: ',' is OR, '&' is AND, case-insensitive substring match.

`a&b,c&d` means (a AND b) OR (c AND d).
"""

from __future__ import annotations


def parse_query(query: str) -> list[list[str]]:
    """Parse into disjunctive normal form: a list of OR-ed clauses, each a list of AND-ed terms."""
    clauses: list[list[str]] = []
    for raw_clause in query.split(","):
        terms = [t.strip().lower() for t in raw_clause.split("&")]
        terms = [t for t in terms if t]
        if terms:
            clauses.append(terms)
    return clauses


def matches(text: str, clauses: list[list[str]]) -> bool:
    haystack = text.lower()
    return any(all(term in haystack for term in clause) for clause in clauses)
