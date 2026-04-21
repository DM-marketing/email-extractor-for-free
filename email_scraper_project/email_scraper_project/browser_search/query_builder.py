"""Build search query strings from multi-value keyword / state / country fields."""

from __future__ import annotations

import re


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
    b2b_enrich: bool = False,
) -> list[str]:
    """
    Cartesian product: each keyword x each state (or none) x each country.
    If countries empty, uses a single empty country (keyword-only queries).
    When ``b2b_enrich`` is True, adds "company" / "services" style queries per region.
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

    def _append(q: str) -> bool:
        nonlocal out
        q = " ".join(q.split())
        if not q or q in seen:
            return False
        seen.add(q)
        out.append(q)
        return len(out) >= max_queries

    out: list[str] = []
    seen: set[str] = set()
    for kw in kws:
        for st in sts:
            for ct in cts:
                parts = [p for p in (kw, st, ct) if p]
                q = " ".join(parts)
                if _append(q):
                    return out
                if b2b_enrich:
                    region = " ".join(p for p in (st, ct) if p).strip() or ct or st or "USA"
                    if _append(f"{kw} company {region}".strip()):
                        return out
                    if _append(f"{kw} services {region}".strip()):
                        return out
                    if st and ct and _append(f"{kw} company {st} {ct}".strip()):
                        return out
    return out
