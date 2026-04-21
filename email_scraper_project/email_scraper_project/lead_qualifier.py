"""
B2B lead qualification: domain classification, email filtering, industry hints,
scoring, CSV export, and optional OpenAI summaries.

Used by the Streamlit tab "AI Lead Intelligence" and by collection/crawl hooks.
"""

from __future__ import annotations

import csv
import json
import logging
import os
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Iterable, Sequence


def _parse_keyword_lines(s: str) -> list[str]:
    """Split keywords on commas, semicolons, or newlines (same rules as query_builder)."""
    if not s or not str(s).strip():
        return []
    parts = re.split(r"[,;\n]+", str(s).strip())
    return [p.strip() for p in parts if p.strip()]

import requests
from bs4 import BeautifulSoup

logger = logging.getLogger("leadgen.qualifier")

# --- Collection-time noise (directories, aggregators, news at scale) ---
NOISE_HOST_SUBSTRINGS: frozenset[str] = frozenset(
    {
        "yelp.",
        "yellowpages.",
        "angi.",
        "homeadvisor.",
        "thumbtack.",
        "houzz.",
        "clutch.co",
        "crunchbase.",
        "linkedin.",
        "indeed.",
        "glassdoor.",
        "bbb.org",
        "mapquest.",
        "foursquare.",
        "manta.",
        "zoominfo.",
        "dnb.com",
        "kompass.",
        "europages.",
        "thomasnet.",
        "facebook.",
        "instagram.",
        "twitter.",
        "x.com",
        "pinterest.",
        "reddit.",
        "youtube.",
        "wikipedia.",
        "wikimedia.",
        "medium.com",
        "bloomberg.",
        "reuters.",
        "cnn.com",
        "nytimes.",
        "bbc.",
        "forbes.",
        "wsj.com",
        "theguardian.",
        "cbsnews.",
        "npr.org",
        "ap.org",
        "scribd.",
    }
)

def _is_government_host(host: str) -> bool:
    h = host.lower().strip().rstrip(".")
    if h.endswith(".gov") or h.endswith(".mil"):
        return True
    parts = h.split(".")
    return len(parts) >= 2 and parts[-1] in ("gov", "mil")

SAAS_KEYWORDS = (
    "saas",
    "software as a service",
    "cloud platform",
    "api integration",
    "subscription billing",
    "devops",
    "kubernetes",
    "microservices",
    "machine learning platform",
    "crm software",
    "erp software",
)

MEDIA_KEYWORDS = (
    "magazine",
    "newsroom",
    "editorial",
    "subscribe to our newsletter",
    "breaking news",
    "journalism",
    "podcast",
)

DIRECTORY_KEYWORDS = (
    "top 10",
    "list of companies",
    "directory",
    "find a contractor",
    "compare quotes",
    "search results",
)

ENTERPRISE_KEYWORDS = (
    "fortune 500",
    "global offices",
    "investor relations",
    "sec filing",
    "annual report pdf",
    "careers at",
    "worldwide headquarters",
)

SMB_SIGNALS = (
    "family owned",
    "locally owned",
    "since 19",
    "call us today",
    "free estimate",
    "licensed & insured",
    "serving the",
    "we service",
)

PROCESS_KEYWORDS = (
    "manual",
    "spreadsheet",
    "excel",
    "inventory",
    "tracking",
    "paperwork",
    "dispatch",
    "work order",
    "schedule jobs",
)

INDUSTRY_PATTERNS: dict[str, tuple[str, ...]] = {
    "construction": ("construction", "contractor", "builder", "concrete", "roofing", "excavat"),
    "hvac": ("hvac", "heating", "cooling", "air conditioning", "furnace", "ductwork"),
    "logistics": ("logistics", "freight", "trucking", "fleet", "shipping", "broker"),
    "warehouse": ("warehouse", "distribution center", "3pl", "storage", "fulfillment"),
    "manufacturing": ("manufacturing", "fabrication", "machine shop", "industrial", "plant"),
    "repair": ("repair", "rewind", "maintenance", "service center", "technician"),
}

KNOWN_INDUSTRY_KEYS: frozenset[str] = frozenset(INDUSTRY_PATTERNS.keys())


def infer_industry_from_keyword(keyword: str) -> str:
    """
    Map a search keyword / query phrase to a single canonical industry key, or "".

    Used for campaign tagging and scoring (not a guarantee of site vertical).
    """
    low = " ".join((keyword or "").lower().split())
    if not low:
        return ""
    # Phrase-level rules first (campaign examples)
    if "warehouse" in low and "company" in low:
        return "logistics"
    if any(p in low for p in ("hvac", "heating and cooling", "air conditioning", "furnace", "ductwork")):
        return "hvac"
    if any(
        p in low
        for p in (
            "construction",
            "contractor",
            "concrete",
            "roofing",
            "excavat",
            "general contractor",
        )
    ):
        return "construction"
    if any(p in low for p in ("3pl", "freight broker", "logistics company", "shipping company", "trucking company")):
        return "logistics"
    if any(p in low for p in ("warehouse", "distribution center", "fulfillment center", "storage facility")):
        return "warehouse"
    if any(p in low for p in ("manufacturing", "fabrication", "machine shop", "cnc ", "industrial plant")):
        return "manufacturing"
    if any(p in low for p in ("repair", "maintenance", "rewind", "field service", "technician")):
        return "repair"
    return ""


def infer_industries_from_keywords_text(block: str) -> list[str]:
    """Return ordered unique industries inferred from a multi-line / comma-separated keyword block."""
    out: list[str] = []
    for kw in _parse_keyword_lines(block or ""):
        ind = infer_industry_from_keyword(kw)
        if ind and ind not in out:
            out.append(ind)
    return out


def normalize_industry_selection(selected: Sequence[str] | None) -> frozenset[str]:
    """Keep only known industry keys (lowercase)."""
    if not selected:
        return frozenset()
    return frozenset(x.strip().lower() for x in selected if x and x.strip().lower() in KNOWN_INDUSTRY_KEYS)


def compute_matched_industry(
    detected: str,
    selected: frozenset[str],
    keyword_inferred: frozenset[str],
) -> bool:
    """True when homepage industry aligns with selected filters and/or keyword-inferred set."""
    if not detected:
        return False
    allowed = selected | keyword_inferred
    if not allowed:
        return False
    return detected in allowed

FAKE_EMAIL_LOCALS = frozenset(
    {
        "test",
        "example",
        "sample",
        "fake",
        "demo",
        "admin",
        "webmaster",
        "postmaster",
        "hostmaster",
        "abuse",
        "privacy",
    }
)

_LOW_INTENT_LOCAL_EXACT = frozenset(
    {
        "noreply",
        "no-reply",
        "donotreply",
        "mailer-daemon",
        "mailerdaemon",
        "bounce",
        "newsletter",
        "newsletters",
        "media",
        "press",
        "support",
        "helpdesk",
        "help",
        "billing",
        "accounts",
        "abuse",
    }
)


def _local_low_intent(root: str) -> bool:
    r = root.lower()
    if r in _LOW_INTENT_LOCAL_EXACT:
        return True
    for p in ("noreply", "no-reply", "donotreply", "mailer-daemon", "bounce"):
        if r.startswith(p):
            return True
    if r.startswith("newsletter"):
        return True
    return False


_HIGH_HEAD_LOCALS = frozenset(
    {
        "info",
        "contact",
        "sales",
        "hello",
        "office",
        "enquiries",
        "enquiry",
    }
)


def should_drop_collected_host(host: str | None) -> bool:
    """Return True if this hostname should never be saved during domain collection."""
    if not host:
        return True
    h = host.lower().strip().lstrip(".")
    for frag in NOISE_HOST_SUBSTRINGS:
        if frag in h:
            return True
    if _is_government_host(h):
        return True
    return False


def _fetch_homepage(domain: str, timeout: float = 12.0) -> tuple[str, str]:
    """Return (html, final_url) for https homepage."""
    base = domain.lower().strip().lstrip(".")
    if not base or should_drop_collected_host(base):
        return "", ""
    url = f"https://{base}/"
    try:
        r = requests.get(
            url,
            timeout=timeout,
            headers={"User-Agent": "Mozilla/5.0 LeadQualifierBot/1.0"},
            allow_redirects=True,
        )
        if r.status_code >= 400:
            return "", url
        return r.text or "", str(r.url)
    except Exception as e:
        logger.debug("homepage fetch %s: %s", base, e)
        return "", url


def _title_from_html(html: str) -> str:
    if not html:
        return ""
    try:
        soup = BeautifulSoup(html, "html.parser")
        t = soup.find("title")
        if t and t.string:
            return t.string.strip()
    except Exception:
        pass
    return ""


def _visible_text_sample(html: str, max_chars: int = 12000) -> str:
    if not html:
        return ""
    try:
        soup = BeautifulSoup(html, "html.parser")
        for tag in soup(["script", "style", "noscript"]):
            tag.decompose()
        return " ".join(soup.get_text(" ", strip=True).split())[:max_chars].lower()
    except Exception:
        return html[:max_chars].lower()


def detect_industry(text: str) -> str:
    if not text:
        return ""
    for industry, kws in INDUSTRY_PATTERNS.items():
        for kw in kws:
            if kw in text:
                return industry
    return ""


def classify_domain(domain: str, html: str, title: str) -> str:
    h = domain.lower()
    blob = f"{title} {_visible_text_sample(html)}".lower()

    if should_drop_collected_host(h):
        return "directory/listing"
    if _is_government_host(h):
        return "government"

    if any(k in blob for k in DIRECTORY_KEYWORDS) or "directory" in h:
        return "directory/listing"
    if any(k in blob for k in MEDIA_KEYWORDS) or "blog" in h:
        return "media/blog"
    if any(k in blob for k in SAAS_KEYWORDS) or any(x in h for x in ("app.", "api.", "cloud.")):
        return "saas/tool"
    if "wordpress.com" in h or "blogspot." in h or "medium.com" in h:
        return "media/blog"

    # Default: treat as business if we got real HTML
    if len(html) > 500:
        return "business"
    return "unknown"


def email_keep_decision(email: str) -> tuple[bool, str]:
    e = (email or "").strip().lower()
    if "@" not in e:
        return False, "invalid"
    local, _, domain = e.partition("@")
    if not local or not domain:
        return False, "invalid"
    root = local.split("+", 1)[0]
    if root in FAKE_EMAIL_LOCALS or root.startswith("test") or "example" in domain:
        return False, "fake_or_test"
    if _local_low_intent(root):
        return False, "low_intent_local"
    head = root.split(".", 1)[0]
    if head in _HIGH_HEAD_LOCALS:
        return True, "high_intent_local"
    if "." in root and re.match(r"^[a-z]{2,20}\.[a-z]{2,20}$", root):
        return True, "name_like_local"
    if re.match(r"^[a-z]{3,20}$", root):
        return True, "short_alpha_local"
    return False, "generic_local"


def score_lead(
    domain: str,
    classification: str,
    industry: str,
    html: str,
    contact_url: str,
    email_reason: str,
    *,
    selected_industries: frozenset[str] | None = None,
    keyword_inferred_industries: frozenset[str] | None = None,
) -> tuple[int, str]:
    """0–10 score using B2B heuristics (classification penalties + positive signals)."""
    notes: list[str] = []
    score = 0
    text = _visible_text_sample(html)
    raw_low = (html or "").lower()
    sel = selected_industries or frozenset()
    kw_inf = keyword_inferred_industries or frozenset()
    det = (industry or "").strip().lower()

    if classification == "saas/tool":
        score -= 3
        notes.append("penalty:saas/tool")
    elif classification == "media/blog":
        score -= 3
        notes.append("penalty:media/blog")
    elif classification == "directory/listing":
        score -= 2
        notes.append("penalty:directory")
    elif classification == "government":
        score -= 3
        notes.append("penalty:government")

    low = text
    if "/services" in raw_low or "services" in low[:2500]:
        score += 3
        notes.append("+services_page_or_copy")
    if len(html) < 9000 and raw_low.count("<table") < 4 and "bootstrap" not in raw_low:
        score += 2
        notes.append("+simple_or_legacy_html_heuristic")
    for kw in PROCESS_KEYWORDS:
        if kw in low:
            score += 2
            notes.append(f"+process:{kw}")
            break
    for s in SMB_SIGNALS:
        if s in low:
            score += 2
            notes.append(f"+smb:{s}")
            break
    for ent in ENTERPRISE_KEYWORDS:
        if ent in low:
            score -= 2
            notes.append(f"penalty:enterprise:{ent}")
            break

    if contact_url and any(x in contact_url.lower() for x in ("/contact", "/about", "/team")):
        score += 1
        notes.append("+contactish_source_url")

    if email_reason.startswith("high_intent") or email_reason == "high_intent_local":
        score += 1
        notes.append("+strong_email_local")

    if classification == "business":
        notes.append("class:business")

    if det:
        if sel and det in sel:
            score += 3
            notes.append("+industry_matches_selected")
        if kw_inf and det in kw_inf:
            score += 2
            notes.append("+keyword_industry_match")
        campaign = sel | kw_inf
        if sel and det and det not in campaign:
            score -= 2
            notes.append("-industry_unrelated_to_campaign")

    score = max(0, min(10, score))
    return score, "; ".join(notes)


def maybe_ai_summary(
    domain: str,
    industry: str,
    score: int,
    classification: str,
) -> str:
    key = os.environ.get("OPENAI_API_KEY", "").strip()
    if not key:
        return ""
    try:
        import urllib.request

        payload = {
            "model": "gpt-4o-mini",
            "messages": [
                {
                    "role": "user",
                    "content": (
                        f"In one sentence, describe this B2B lead for outreach: "
                        f"domain={domain}, industry={industry or 'unknown'}, "
                        f"classification={classification}, score={score}/10. "
                        f"Assume they may use manual processes or spreadsheets."
                    ),
                }
            ],
            "max_tokens": 80,
        }
        req = urllib.request.Request(
            "https://api.openai.com/v1/chat/completions",
            data=json.dumps(payload).encode("utf-8"),
            headers={
                "Authorization": f"Bearer {key}",
                "Content-Type": "application/json",
            },
            method="POST",
        )
        with urllib.request.urlopen(req, timeout=25) as resp:
            data = json.loads(resp.read().decode("utf-8", errors="ignore"))
        return (
            data.get("choices", [{}])[0]
            .get("message", {})
            .get("content", "")
            .strip()
        )
    except Exception as e:
        logger.debug("openai summary skipped: %s", e)
        return ""


def pitch_angle(industry: str, score: int) -> str:
    parts = []
    if industry:
        parts.append(f"Position custom Access database for {industry} workflows.")
    else:
        parts.append("Position custom Access database for operations and job tracking.")
    if score >= 7:
        parts.append("Emphasize replacing spreadsheets and manual double-entry.")
    else:
        parts.append("Lead with a quick audit of their current tracking pain.")
    return " ".join(parts)


def _company_from_domain(domain: str) -> str:
    host = domain.lower().strip().lstrip(".")
    host = host.removeprefix("www.")
    base = host.split(".")[0] if host else ""
    return base.replace("-", " ").title() if base else host


def _name_guess_from_email(email: str) -> str:
    local = email.split("@", 1)[0].lower()
    local = local.split("+", 1)[0]
    if "." in local:
        return " ".join(p.capitalize() for p in local.split(".") if p.isalpha())
    return ""


def parse_emails_txt(path: str | os.PathLike[str]) -> list[tuple[str, str, str]]:
    rows: list[tuple[str, str, str]] = []
    p = os.fspath(path)
    try:
        for line in open(p, encoding="utf-8", errors="ignore"):
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            parts = line.split("\t")
            if len(parts) >= 3:
                rows.append((parts[0].strip(), parts[1].strip(), parts[2].strip()))
    except OSError:
        pass
    return rows


def normalize_domain_host(host: str) -> str:
    """Strip www. for dedupe and one-domain-per-row behavior."""
    h = (host or "").lower().strip().lstrip(".")
    if h.startswith("www."):
        return h[4:]
    return h


def parse_domains_txt(path: str | os.PathLike[str]) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    p = os.fspath(path)
    try:
        for line in open(p, encoding="utf-8", errors="ignore"):
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            host = line.replace("http://", "").replace("https://", "").split("/")[0].strip().lower()
            if not host:
                continue
            host = normalize_domain_host(host)
            if host in seen:
                continue
            seen.add(host)
            out.append(host)
    except OSError:
        pass
    return out


@dataclass
class QualifiedLead:
    domain: str
    email: str
    industry: str
    classification: str
    score: int
    contact_page_url: str
    notes: str
    ai_summary: str = ""
    company: str = ""
    pitch_angle: str = ""
    detected_industry: str = ""
    matched_industry: bool = False


def qualify_email_rows(
    rows: Iterable[tuple[str, str, str]],
    *,
    use_openai: bool = False,
    require_target_industry: bool = False,
    strict_industry_filter: bool = False,
    selected_industries: frozenset[str] | None = None,
    keyword_inferred_industries: frozenset[str] | None = None,
    progress: Callable[[str], None] | None = None,
) -> list[QualifiedLead]:
    out: list[QualifiedLead] = []
    seen: set[tuple[str, str]] = set()
    cache: dict[str, tuple[str, str, str, str]] = {}

    sel = selected_industries or frozenset()
    kw_inf = keyword_inferred_industries or frozenset()

    for email, domain, src in rows:
        dom = normalize_domain_host(domain)
        if should_drop_collected_host(dom):
            continue
        key = (email.lower(), dom)
        if key in seen:
            continue
        seen.add(key)

        if dom not in cache:
            if progress:
                progress(f"Fetching {dom} …")
            html, final_url = _fetch_homepage(dom)
            title = _title_from_html(html)
            cls = classify_domain(dom, html, title)
            ind = detect_industry(_visible_text_sample(html) + " " + title.lower())
            cache[dom] = (html, final_url, cls, ind)
        else:
            html, final_url, cls, ind = cache[dom]

        ok, reason = email_keep_decision(email)
        if not ok:
            continue

        if strict_industry_filter and sel:
            if not ind or ind not in sel:
                continue
        elif require_target_industry and not ind:
            continue

        contact_url = src or final_url or f"https://{dom}/"
        score, notes = score_lead(
            dom,
            cls,
            ind,
            html,
            contact_url,
            reason,
            selected_industries=sel,
            keyword_inferred_industries=kw_inf,
        )
        summary = ""
        if use_openai:
            summary = maybe_ai_summary(dom, ind, score, cls)
        company = _company_from_domain(dom)
        pa = pitch_angle(ind, score)
        matched = compute_matched_industry(ind, sel, kw_inf)
        ql = QualifiedLead(
            domain=dom,
            email=email.lower(),
            industry=ind,
            classification=cls,
            score=score,
            contact_page_url=contact_url,
            notes=notes + f" | email:{reason}",
            ai_summary=summary,
            company=company,
            pitch_angle=pa,
            detected_industry=ind,
            matched_industry=matched,
        )
        out.append(ql)
    return out


def qualify_domains_only(
    domains: Iterable[str],
    *,
    require_target_industry: bool = False,
    strict_industry_filter: bool = False,
    selected_industries: frozenset[str] | None = None,
    keyword_inferred_industries: frozenset[str] | None = None,
    progress: Callable[[str], None] | None = None,
) -> list[QualifiedLead]:
    rows = [(f"info@{d}", d, f"https://{d}/contact") for d in domains if not should_drop_collected_host(d)]
    return qualify_email_rows(
        rows,
        use_openai=False,
        require_target_industry=require_target_industry,
        strict_industry_filter=strict_industry_filter,
        selected_industries=selected_industries,
        keyword_inferred_industries=keyword_inferred_industries,
        progress=progress,
    )


def write_qualified_csv(leads: Iterable[QualifiedLead], path: str | os.PathLike[str]) -> None:
    p = Path(os.fspath(path))
    p.parent.mkdir(parents=True, exist_ok=True)
    with open(p, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(
            [
                "domain",
                "email",
                "industry",
                "classification",
                "score",
                "contact_page_url",
                "notes",
                "detected_industry",
                "matched_industry",
            ]
        )
        for L in leads:
            notes = L.notes
            if L.ai_summary:
                notes = f"{notes} | ai: {L.ai_summary}"
            det = L.detected_industry or L.industry
            w.writerow(
                [
                    L.domain,
                    L.email,
                    L.industry,
                    L.classification,
                    L.score,
                    L.contact_page_url,
                    notes,
                    det,
                    "true" if L.matched_industry else "false",
                ]
            )


def write_outreach_csv(leads: Iterable[QualifiedLead], path: str | os.PathLike[str]) -> None:
    p = Path(os.fspath(path))
    p.parent.mkdir(parents=True, exist_ok=True)
    with open(p, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["name", "email", "company", "industry", "pitch_angle"])
        for L in leads:
            name = _name_guess_from_email(L.email)
            w.writerow([name, L.email, L.company, L.industry, L.pitch_angle])


__all__ = [
    "NOISE_HOST_SUBSTRINGS",
    "KNOWN_INDUSTRY_KEYS",
    "normalize_domain_host",
    "should_drop_collected_host",
    "infer_industry_from_keyword",
    "infer_industries_from_keywords_text",
    "normalize_industry_selection",
    "compute_matched_industry",
    "qualify_email_rows",
    "qualify_domains_only",
    "write_qualified_csv",
    "write_outreach_csv",
    "parse_emails_txt",
    "parse_domains_txt",
    "QualifiedLead",
]
