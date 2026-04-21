"""
Bing SERP tracking URLs: extract `u=` query param and base64-decode to the destination URL.

Falls back to returning the original href when not a Bing tracking link.
"""

from __future__ import annotations

import base64
import binascii
import logging
from urllib.parse import parse_qs, unquote, urlparse

logger = logging.getLogger("leadgen.bing_decode")


def _b64_decode_to_url(blob: str) -> str | None:
    """Decode base64 (standard or urlsafe) to a UTF-8 URL string."""
    s = blob.strip()
    if not s:
        return None
    pad = "=" * ((4 - len(s) % 4) % 4)
    s_padded = s + pad
    for fn in (base64.urlsafe_b64decode, base64.b64decode):
        try:
            raw = fn(s_padded)
            text = raw.decode("utf-8", errors="strict").strip()
            if text.startswith("http://") or text.startswith("https://"):
                return text
        except (binascii.Error, UnicodeDecodeError, ValueError):
            continue
    return None


def decode_bing_u_parameter(u_raw: str) -> str | None:
    """
    Decode the Bing ``u`` query value (may be URL-encoded, sometimes chained).

    Bing ``/ck/a`` links often prefix the base64 blob with a short marker (e.g. ``a1``,
    ``a2``) before the actual URL-safe base64; strip those and retry.
    """
    if not u_raw:
        return None
    current = unquote(u_raw)
    for _ in range(5):
        current = current.strip()
        if current.startswith("http://") or current.startswith("https://"):
            return current.split()[0]
        variants = [current]
        if len(current) >= 4 and current[:2] in ("a1", "a2", "a3") and current[2:3].isalnum():
            variants.insert(0, current[2:])
        seen_v: set[str] = set()
        for v in variants:
            if not v or v in seen_v:
                continue
            seen_v.add(v)
            decoded = _b64_decode_to_url(v)
            if decoded:
                return decoded
        nxt = unquote(current)
        if nxt == current:
            break
        current = nxt
    return None


def decode_bing_tracking_url(href: str) -> str | None:
    """
    If href is a bing.com tracking URL with `u=`, return decoded destination URL.
    Otherwise return None (caller keeps original href).
    """
    if not href or "bing.com" not in href.lower():
        return None
    if "u=" not in href and "&u" not in href.lower():
        return None
    try:
        q = urlparse(href).query
        if not q:
            return None
        qs = parse_qs(q, keep_blank_values=True)
        for key in ("u", "U"):
            vals = qs.get(key)
            if vals and vals[0]:
                out = decode_bing_u_parameter(vals[0])
                if out:
                    return out
    except Exception as e:
        logger.debug("bing decode parse error: %s", e)
    return None


def resolve_search_result_href(href: str) -> str:
    """Apply Bing decoder when applicable; otherwise return href unchanged."""
    if not href:
        return href
    decoded = decode_bing_tracking_url(href)
    return decoded if decoded else href
