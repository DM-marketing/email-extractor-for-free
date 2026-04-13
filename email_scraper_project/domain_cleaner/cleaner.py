"""URL normalization and business-domain filtering (language-agnostic heuristics)."""

from __future__ import annotations

import re
from urllib.parse import parse_qs, unquote, urlparse, urlunparse

# Substrings in hostname (lowercase) — social, engines, UGC (language-independent IDs).
_BLOCKED_HOST_PARTS = frozenset(
    {
        "facebook.",
        "fb.com",
        "twitter.",
        "t.co",
        "x.com",
        "instagram.",
        "linkedin.",
        "pinterest.",
        "tiktok.",
        "reddit.",
        "youtube.",
        "youtu.be",
        "vimeo.",
        "tumblr.",
        "snapchat.",
        "whatsapp.",
        "telegram.",
        "discord.",
        "medium.com",
        "wikipedia.",
        "wikimedia.",
    }
)

# Exact suffix match for search / portal hosts.
_SEARCH_ENGINE_SUFFIXES = (
    "google.com",
    "google.co.uk",
    "bing.com",
    "duckduckgo.com",
    "search.yahoo.com",
    "yahoo.com",
    "startpage.com",
    "ecosia.org",
    "qwant.com",
    "baidu.com",
)

# Known directory / job-board style domains (substring match on netloc).
_BAD_DIRECTORY_PARTS = frozenset(
    {
        "indeed.",
        "glassdoor.",
        "monster.",
        "ziprecruiter.",
        "reed.co",
        "totaljobs.",
        "careerbuilder.",
        "simplyhired.",
        "yellowpages.",
        "yelp.",
        "thomasnet.",
        "angi.",
        "homeadvisor.",
        "houzz.",
        "clutch.co",
        "mapquest.",
        "foursquare.",
        "bbb.org",
        "chamberofcommerce.",
        "manta.",
        "hotfrog.",
        "cylex.",
        "europages.",
        "kompass.",
        "alibaba.",
        "amazon.",
        "ebay.",
        "etsy.",
        "walmart.",
        "target.",
    }
)

# Optional: drop very low-signal TLDs (not language-specific).
_DISALLOWED_TLDS = frozenset(
    {
        "tk",
        "ml",
        "ga",
        "cf",
        "gq",
        "xyz",
        "top",
        "click",
        "download",
        "stream",
    }
)

_TRACKING_QUERY_KEYS = frozenset(
    {
        "utm_source",
        "utm_medium",
        "utm_campaign",
        "utm_term",
        "utm_content",
        "gclid",
        "fbclid",
        "msclkid",
        "yclid",
        "mc_eid",
    }
)


def normalize_url(raw: str) -> str | None:
    """
    Resolve common search-engine redirect wrappers to a real URL.
    Returns a string URL or None if parsing fails.
    """
    if not raw or not isinstance(raw, str):
        return None
    raw = raw.strip()
    if not raw.startswith(("http://", "https://")):
        raw = "https://" + raw

    try:
        parsed = urlparse(raw)
    except Exception:
        return None

    # DuckDuckGo HTML redirect
    if "duckduckgo.com" in (parsed.netloc or "").lower() and parsed.query:
        qs = parse_qs(parsed.query)
        if "uddg" in qs and qs["uddg"]:
            return unquote(qs["uddg"][0])

    # Yahoo /search/... RU= encoded target
    full = raw
    if "RU=" in full:
        try:
            part = full.split("RU=", 1)[1]
            part = part.split("/RK", 1)[0].split("/RS", 1)[0]
            candidate = unquote(part)
            if candidate.startswith("http"):
                return candidate
        except Exception:
            pass

    # Bing /ck/a? u=...
    if "/ck/a?" in full or "bing.com/ck/a" in full:
        try:
            qs = parse_qs(urlparse(full).query)
            if "u" in qs and qs["u"]:
                return unquote(qs["u"][0])
        except Exception:
            pass

    # Strip tracking query params
    if parsed.query:
        pairs = []
        for key, values in parse_qs(parsed.query, keep_blank_values=True).items():
            lk = key.lower()
            if lk in _TRACKING_QUERY_KEYS:
                continue
            for v in values:
                pairs.append((key, v))
        if pairs:
            from urllib.parse import urlencode

            new_query = urlencode(pairs, doseq=True)
        else:
            new_query = ""
        parsed = parsed._replace(query=new_query)

    return urlunparse(parsed)


def _hostname_parts(netloc: str) -> tuple[str, str | None]:
    host = netloc.lower().strip()
    if not host:
        return "", None
    if "@" in host:
        host = host.split("@", 1)[-1]
    if ":" in host:
        host = host.split(":", 1)[0]
    if host.startswith("www."):
        host = host[4:]
    parts = host.split(".")
    if len(parts) >= 2:
        tld = parts[-1]
    else:
        tld = None
    return host, tld


def clean_domain(url: str) -> str | None:
    """
    Return registrable-style hostname (no www) for a business site, or None to drop.
    """
    normalized = normalize_url(url)
    if not normalized:
        return None
    try:
        parsed = urlparse(normalized)
    except Exception:
        return None

    scheme = (parsed.scheme or "").lower()
    if scheme not in ("http", "https"):
        return None

    netloc = parsed.netloc or ""
    host, tld = _hostname_parts(netloc)
    if not host or not tld:
        return None

    if len(tld) == 2 and tld.isalpha():
        pass
    elif tld in _DISALLOWED_TLDS:
        return None

    for suf in _SEARCH_ENGINE_SUFFIXES:
        if host == suf or host.endswith("." + suf):
            return None

    for fragment in _BLOCKED_HOST_PARTS:
        if fragment in host:
            return None

    for bad in _BAD_DIRECTORY_PARTS:
        if bad in host:
            return None

    path_lower = (parsed.path or "").lower()
    if any(x in path_lower for x in ("/wp-content/", "/wp-includes/")):
        pass

    if re.search(r"\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}", host):
        return None

    if ".." in host or host.startswith(".") or host.endswith("."):
        return None

    return host
