"""Build search query strings from multi-value keyword / state / country fields."""

from __future__ import annotations

import re
from typing import Iterable


def parse_multi_line_csv(s: str) -> list[str]:
    """Split on commas, semicolons, or newlines; strip; drop empties."""
    if not s or not str(s).strip():
        return []
    parts = re.split(r"[,;\n]+", str(s).strip())
    return [p.strip() for p in parts if p.strip()]


def build_playwright_queries(
    keywords: str,
    countries: str,
    states: str = "",
    *,
    max_queries: int = 500,
) -> list[str]:
    """
    Cartesian product: each keyword x each state (or none) x each country.
    If countries empty, uses a single empty country (keyword-only queries).
    """
    kws = parse_multi_line_csv(keywords)
    if not kws:
        kws = ["business"]
    sts = parse_multi_line_csv(states)
    cts = parse_multi_line_csv(countries)
    if not sts:
        sts = [""]
    if not cts:
        cts = [""]

    out: list[str] = []
    seen: set[str] = set()
    for kw in kws:
        for st in sts:
            for ct in cts:
                parts = [p for p in (kw, st, ct) if p]
                q = " ".join(parts)
                q = " ".join(q.split())
                if q and q not in seen:
                    seen.add(q)
                    out.append(q)
                    if len(out) >= max_queries:
                        return out
    return out
