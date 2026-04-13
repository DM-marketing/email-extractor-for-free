"""Email extraction with regex and mailto: (spec: regex + urllib-style parsing via href)."""

from __future__ import annotations

import re
from typing import Iterable

_MAILTO_RE = re.compile(
    r"mailto:\s*([A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,63})",
    re.I,
)
_EMAIL_RE = re.compile(
    r"(?<![A-Za-z0-9._%+-])"
    r"([A-Za-z0-9][A-Za-z0-9._%+-]*@[A-Za-z0-9][A-Za-z0-9.-]*\.[A-Za-z]{2,63})"
)

_SKIP = ("example.com", "test.com", "sample.com", "sentry.io", "w3.org")


def extract_emails_from_html(html: str) -> list[str]:
    if not html:
        return []
    found: list[str] = []
    for m in _MAILTO_RE.finditer(html):
        found.append(m.group(1).lower())
    low = html.lower()
    low = low.replace("[at]", "@").replace("[dot]", ".")
    for m in _EMAIL_RE.finditer(low):
        found.append(m.group(1).lower())
    seen: set[str] = set()
    out: list[str] = []
    for e in found:
        if e in seen:
            continue
        seen.add(e)
        if any(x in e for x in _SKIP):
            continue
        if e.endswith((".png", ".jpg", ".gif")):
            continue
        out.append(e)
    return out


def iter_mailto_hrefs(hrefs: Iterable[str | None]) -> list[str]:
    emails: list[str] = []
    for h in hrefs:
        if not h:
            continue
        m = _MAILTO_RE.search(h)
        if m:
            emails.append(m.group(1).lower())
    return emails
