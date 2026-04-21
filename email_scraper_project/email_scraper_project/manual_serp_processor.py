"""
Manual SERP paste → clean → extract emails/domains → optional light crawl → qualify → CSV.

Used by Streamlit tab "Manual SERP Extractor"; reuses threaded_crawler fetch helpers and lead_qualifier.
"""

from __future__ import annotations

import csv
import html as html_module
import logging
import random
import re
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Iterable
from urllib.parse import urlparse

from bs4 import BeautifulSoup

from email_scraper_project.config import (
    manual_leads_csv_path,
    manual_qualified_leads_csv_path,
)
from email_scraper_project.lead_qualifier import (
    QualifiedLead,
    email_keep_decision,
    normalize_domain_host,
    qualify_email_rows,
    should_drop_collected_host,
)

logger = logging.getLogger("leadgen.manual_serp")

# --- Text cleaning ---

_TAG_RE = re.compile(r"<[^>]+>", re.I | re.S)
_URL_IN_TEXT_RE = re.compile(
    r"https?://[^\s\"'<>)\]]+",
    re.I,
)
_EMAIL_LIKE_RE = re.compile(
    r"\b[A-Za-z0-9][A-Za-z0-9._%+-]*\s*(?:@|\[at\]|\(at\))\s*[A-Za-z0-9.-]+\s*(?:\.|\(dot\)|\[dot\])\s*[A-Za-z]{2,}\b",
    re.I,
)
_STANDARD_EMAIL_RE = re.compile(
    r"\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}\b",
)


def clean_text(raw_text: str) -> str:
    """Strip HTML-ish tags, decode entities, normalize whitespace, dedupe lines."""
    if not raw_text or not str(raw_text).strip():
        return ""
    s = str(raw_text)
    if "<" in s and ">" in s:
        try:
            soup = BeautifulSoup(s, "html.parser")
            for tag in soup(["script", "style"]):
                tag.decompose()
            s = soup.get_text(" ", strip=False)
        except Exception:
            s = _TAG_RE.sub(" ", s)
    s = html_module.unescape(s)
    s = _TAG_RE.sub(" ", s)
    lines = []
    seen_lines: set[str] = set()
    for line in s.splitlines():
        line = " ".join(line.split())
        if line and line.lower() not in seen_lines:
            seen_lines.add(line.lower())
            lines.append(line)
    return "\n".join(lines)


# --- Email extraction & normalization ---

def _normalize_obfuscated_email_fragment(text: str) -> str:
    t = text
    t = re.sub(r"\s*\[\s*at\s*\]\s*", "@", t, flags=re.I)
    t = re.sub(r"\s*\(\s*at\s*\)\s*", "@", t, flags=re.I)
    t = re.sub(r"\s+at\s+", "@", t, flags=re.I)
    t = re.sub(r"\s*\[\s*dot\s*\]\s*", ".", t, flags=re.I)
    t = re.sub(r"\s*\(\s*dot\s*\)\s*", ".", t, flags=re.I)
    t = re.sub(r"\s+dot\s+", ".", t, flags=re.I)
    t = re.sub(r"\s+", "", t)
    return t.strip()


def extract_emails(text: str) -> list[str]:
    """Extract email addresses including mild obfuscation patterns."""
    if not text:
        return []
    found: list[str] = []
    seen: set[str] = set()

    for m in _STANDARD_EMAIL_RE.finditer(text):
        e = m.group(0).lower().strip()
        if e not in seen:
            seen.add(e)
            found.append(e)

    for m in _EMAIL_LIKE_RE.finditer(text):
        raw = m.group(0)
        norm = _normalize_obfuscated_email_fragment(raw).lower()
        if "@" in norm and _STANDARD_EMAIL_RE.match(norm):
            if norm not in seen:
                seen.add(norm)
                found.append(norm)

    chunk = text
    for pat in (
        r"([a-z0-9._%+-]+)\s*\[\s*at\s*\]\s*([a-z0-9.-]+)\s*\[\s*dot\s*\]\s*([a-z]{2,})",
        r"([a-z0-9._%+-]+)\s*\(\s*at\s*\)\s*([a-z0-9.-]+)\s*\(\s*dot\s*\)\s*([a-z]{2,})",
    ):
        for m in re.finditer(pat, chunk, re.I):
            candidate = f"{m.group(1)}@{m.group(2)}.{m.group(3)}".lower()
            if candidate not in seen:
                seen.add(candidate)
                found.append(candidate)

    return found


# --- Domains ---

def _host_from_url(url: str) -> str | None:
    try:
        p = urlparse(url if "://" in url else f"https://{url}")
        h = (p.netloc or p.path.split("/")[0]).lower()
        if not h or "." not in h:
            return None
        return normalize_domain_host(h.split(":")[0])
    except Exception:
        return None


def extract_domains(text: str, emails: list[str]) -> set[str]:
    """Collect domains from URLs, email RHS, and obvious host-like tokens."""
    out: set[str] = set()
    for m in _URL_IN_TEXT_RE.finditer(text or ""):
        h = _host_from_url(m.group(0))
        if h and not should_drop_collected_host(h):
            out.add(h)
    for e in emails:
        if "@" in e:
            dom = e.split("@", 1)[1].strip().lower()
            dom = normalize_domain_host(dom)
            if dom and not should_drop_collected_host(dom):
                out.add(dom)
    for m in re.finditer(r"\b(?:[a-z0-9-]+\.)+[a-z]{2,}\b", (text or "").lower()):
        token = m.group(0).strip(".")
        if "@" in token:
            continue
        if token.count(".") >= 1 and len(token) < 120:
            h = normalize_domain_host(token)
            if h and not should_drop_collected_host(h) and "." in h:
                out.add(h)
    return out


# --- Filtering ---

def filter_emails(emails: Iterable[str], *, remove_low_intent: bool) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    for e in emails:
        el = (e or "").strip().lower()
        if not el or el in seen:
            continue
        seen.add(el)
        ok, reason = email_keep_decision(el)
        if not ok:
            if reason in ("fake_or_test", "invalid"):
                continue
            if remove_low_intent and reason.startswith("low_intent"):
                continue
        out.append(el)
    return out


def email_quality_tier(email: str) -> str:
    ok, reason = email_keep_decision((email or "").lower())
    if not ok:
        return "low"
    if reason.startswith("high_intent") or "high_intent" in reason:
        return "high"
    if reason in ("name_like_local", "short_alpha_local"):
        return "medium"
    return "medium"


# --- Company name ---

def extract_company_name(domain: str, text_chunk: str) -> str:
    """Heuristic company label from domain + optional SERP context."""
    d = normalize_domain_host(domain or "")
    base = d.split(".")[0] if d else ""
    guess = base.replace("-", " ").title() if base else ""
    if text_chunk:
        for m in re.finditer(
            r"\b([A-Z][a-z]+(?:\s+[A-Z][a-z]+){1,4})\b",
            text_chunk[:4000],
        ):
            frag = m.group(1)
            if len(frag) > 5 and base and base[:4] in frag.lower():
                return frag
        for m in re.finditer(
            r"\b([A-Z][a-z]+(?:\s+[a-z]+){0,3}\s+(?:Inc|LLC|Ltd|Corp|Company)\.?)\b",
            text_chunk[:4000],
        ):
            return m.group(1).strip()
    return guess or d


# --- Light crawl (reuse threaded_crawler internals; 2 URLs only) ---

def _light_fetch_emails_for_domain(domain: str, headless_pw: bool = True) -> tuple[str | None, str]:
    """
    HTTP fetch /contact and /about only; optional Playwright on same URLs if empty.
    Returns (first_email_or_none, status_note).
    """
    try:
        from email_scraper_project.email_txt_crawler.threaded_crawler import (
            _emails_from_page,
            _fetch_requests,
            _playwright_fetch_urls,
            _session,
        )
    except Exception as e:
        logger.debug("import threaded_crawler: %s", e)
        return None, "import_error"

    dom = normalize_domain_host(domain)
    if not dom or should_drop_collected_host(dom):
        return None, "skipped_host"

    urls = [f"https://{dom}/contact", f"https://{dom}/about"]
    session = _session()
    collected: list[tuple[str, str, str]] = []
    for url in urls:
        time.sleep(random.uniform(0.2, 0.65))
        try:
            status, html = _fetch_requests(url, session)
            if html and len(html) > 80:
                collected.extend(_emails_from_page(html, url, dom))
        except Exception as e:
            logger.debug("manual fetch %s: %s", url, e)

    if collected:
        return collected[0][0].lower(), "crawled"

    try:
        pairs = _playwright_fetch_urls(urls, headless=headless_pw)
        for u, html in pairs:
            if html:
                collected.extend(_emails_from_page(html, u, dom))
                if collected:
                    return collected[0][0].lower(), "crawled"
    except Exception as e:
        logger.debug("manual playwright %s: %s", dom, e)
        return None, f"playwright_error:{e}"

    return None, "no_email"


# --- Pipeline result ---

@dataclass
class ManualSerpResult:
    raw_emails_found: int = 0
    fixed_obfuscation_count: int = 0
    filtered_emails_count: int = 0
    domains_found: int = 0
    crawled_domains_count: int = 0
    final_lead_rows: int = 0
    qualified_rows: int = 0
    manual_leads: list[dict[str, Any]] = field(default_factory=list)
    qualified_leads: list[dict[str, Any]] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)


MANUAL_SOURCE = "manual_serp"
MANUAL_SCORE_BOOST = 2


def _dedupe_leads(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    seen: set[tuple[str, str]] = set()
    out: list[dict[str, Any]] = []
    for r in rows:
        dom = (r.get("domain") or "").lower()
        em = (r.get("email") or "").lower()
        key = (dom, em)
        if key in seen:
            continue
        seen.add(key)
        out.append(r)
    return out


def run_manual_serp(
    raw_text: str,
    *,
    fix_broken_emails: bool = True,
    crawl_if_no_email: bool = True,
    run_qualification: bool = True,
    remove_low_intent: bool = True,
    extract_companies: bool = True,
    progress: Callable[[str], None] | None = None,
) -> ManualSerpResult:
    res = ManualSerpResult()

    def _p(msg: str) -> None:
        if progress:
            progress(msg)

    combined = (raw_text or "").strip()
    if not combined:
        res.errors.append("No input text.")
        return res

    _p("Cleaning text…")
    cleaned = clean_text(combined)
    if not cleaned.strip():
        res.errors.append("No usable text after cleaning.")
        return res

    _p("Extracting emails…")
    first_pass = extract_emails(cleaned)
    res.raw_emails_found = len(first_pass)
    first_set = set(first_pass)
    raw_emails = list(first_pass)
    fixed_from_obfuscation: set[str] = set()
    if fix_broken_emails:
        relaxed = cleaned
        for a, b in (("[at]", "@"), ("(at)", "@"), ("[dot]", "."), ("(dot)", ".")):
            relaxed = relaxed.replace(a, b).replace(a.upper(), b)
        second_pass = extract_emails(relaxed)
        for e in second_pass:
            if e not in first_set:
                fixed_from_obfuscation.add(e)
        raw_emails = list(dict.fromkeys(raw_emails + list(second_pass)))
        res.fixed_obfuscation_count = len(fixed_from_obfuscation)

    _p("Extracting domains…")
    domains = extract_domains(cleaned, raw_emails)
    res.domains_found = len(domains)

    _p("Filtering emails…")
    filtered = filter_emails(raw_emails, remove_low_intent=remove_low_intent)
    res.filtered_emails_count = len(filtered)

    email_to_domain: dict[str, str] = {}
    for e in filtered:
        dom = e.split("@", 1)[1].strip().lower()
        dom = normalize_domain_host(dom)
        if dom and not should_drop_collected_host(dom):
            email_to_domain[e] = dom

    rows: list[dict[str, Any]] = []
    for email, dom in email_to_domain.items():
        cn = (
            extract_company_name(dom, cleaned)
            if extract_companies
            else normalize_domain_host(dom).split(".")[0].title()
        )
        st = "fixed" if email in fixed_from_obfuscation else "extracted"
        rows.append(
            {
                "domain": dom,
                "email": email,
                "company_name": cn,
                "source": MANUAL_SOURCE,
                "status": st,
                "email_quality": email_quality_tier(email),
                "notes": "from_serp_paste",
            }
        )

    covered_domains = set(email_to_domain.values())
    for dom in domains:
        if dom in covered_domains:
            continue
        cn = extract_company_name(dom, cleaned) if extract_companies else dom.split(".")[0].title()
        rows.append(
            {
                "domain": dom,
                "email": None,
                "company_name": cn,
                "source": MANUAL_SOURCE,
                "status": "extracted",
                "email_quality": "low",
                "notes": "domain_only_in_serp",
            }
        )

    rows = _dedupe_leads(rows)

    if crawl_if_no_email:
        _p("Light crawl for domains without email…")
        updated: list[dict[str, Any]] = []
        for r in rows:
            if r.get("email"):
                updated.append(r)
                continue
            dom = r.get("domain") or ""
            em, st = _light_fetch_emails_for_domain(dom)
            if em:
                res.crawled_domains_count += 1
                ok, _ = email_keep_decision(em)
                if remove_low_intent and not ok:
                    r = {**r, "notes": (r.get("notes") or "") + f"; crawl_rejected:{st}"}
                    updated.append(r)
                    continue
                r["email"] = em
                r["status"] = "crawled"
                r["email_quality"] = email_quality_tier(em)
                r["notes"] = (r.get("notes") or "") + f"; {st}"
                if extract_companies:
                    r["company_name"] = extract_company_name(dom, cleaned)
            else:
                r["notes"] = (r.get("notes") or "") + f"; crawl:{st}"
            updated.append(r)
        rows = _dedupe_leads(updated)

    res.final_lead_rows = len(rows)
    res.manual_leads = rows

    qualified_dicts: list[dict[str, Any]] = []
    if run_qualification:
        _p("Running lead qualification…")
        triples: list[tuple[str, str, str]] = []
        for r in rows:
            em = r.get("email")
            if not em:
                continue
            dom = r.get("domain") or ""
            triples.append((em, dom, f"https://{dom}/"))

        try:
            leads_q: list[QualifiedLead] = []
            if triples:
                leads_q = qualify_email_rows(
                    triples,
                    use_openai=False,
                    require_target_industry=False,
                    strict_industry_filter=False,
                    selected_industries=None,
                    keyword_inferred_industries=None,
                    progress=lambda m: _p(m),
                )
            by_key = {(r.get("email", "").lower(), r.get("domain", "").lower()): r for r in rows}
            for L in leads_q:
                sc = min(10, int(L.score) + MANUAL_SCORE_BOOST)
                notes = (L.notes or "") + f" | manual_serp_boost:+{MANUAL_SCORE_BOOST}"
                r0 = by_key.get((L.email.lower(), L.domain.lower()))
                company = (r0 or {}).get("company_name") or extract_company_name(L.domain, cleaned)
                qualified_dicts.append(
                    {
                        "domain": L.domain,
                        "email": L.email,
                        "company_name": company,
                        "industry": L.industry,
                        "score": sc,
                        "classification": L.classification,
                        "matched_industry": L.matched_industry,
                        "source": MANUAL_SOURCE,
                        "notes": notes,
                    }
                )
            res.qualified_rows = len(qualified_dicts)
            res.qualified_leads = qualified_dicts
        except Exception as e:
            logger.exception("qualification failed")
            res.errors.append(f"Qualification failed: {e}")

    return res


def write_manual_leads_csv(rows: Iterable[dict[str, Any]], path: str | Path) -> None:
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = [
        "domain",
        "email",
        "company_name",
        "source",
        "status",
        "email_quality",
        "notes",
    ]
    with open(p, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        w.writeheader()
        for r in rows:
            w.writerow({k: r.get(k, "") for k in fieldnames})


def write_manual_qualified_csv(rows: Iterable[dict[str, Any]], path: str | Path) -> None:
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = [
        "domain",
        "email",
        "company_name",
        "industry",
        "score",
        "classification",
        "matched_industry",
        "source",
        "notes",
    ]
    with open(p, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        w.writeheader()
        for r in rows:
            row = {k: r.get(k, "") for k in fieldnames}
            if "matched_industry" in row:
                row["matched_industry"] = "true" if row["matched_industry"] else "false"
            w.writerow(row)


__all__ = [
    "ManualSerpResult",
    "clean_text",
    "extract_emails",
    "extract_domains",
    "filter_emails",
    "extract_company_name",
    "run_manual_serp",
    "write_manual_leads_csv",
    "write_manual_qualified_csv",
    "MANUAL_SOURCE",
    "MANUAL_SCORE_BOOST",
]
