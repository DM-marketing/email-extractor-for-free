"""Decode Yahoo redirect URLs (r.search.yahoo.com) to the destination link."""

from __future__ import annotations

import re
from urllib.parse import unquote, urlparse


def decode_yahoo_redirect_url(href: str) -> str | None:
    """
    If href is a Yahoo click-through URL containing ``RU=`` (often percent-encoded),
    return the decoded target URL. Otherwise return None.
    """
    if not href or "yahoo.com" not in href.lower():
        return None
    low = href.lower()
    if "r.search.yahoo.com" not in low and "/ru=" not in low and "ru=" not in low:
        return None
    # Path-style: .../RU=https%3a%2f%2fexample.com/...
    m = re.search(r"/RU=([^/?#]+)", href, flags=re.IGNORECASE)
    if not m:
        # Query-style rare
        m = re.search(r"[\?&]RU=([^&]+)", href, flags=re.IGNORECASE)
    if not m:
        return None
    raw = unquote(m.group(1).replace("+", " "))
    if raw.startswith("http://") or raw.startswith("https://"):
        return raw.split()[0]
    return None


def resolve_yahoo_result_href(href: str) -> str:
    """Return destination URL when Yahoo wraps the link; else original."""
    if not href:
        return href
    decoded = decode_yahoo_redirect_url(href)
    return decoded if decoded else href


__all__ = ["decode_yahoo_redirect_url", "resolve_yahoo_result_href"]
