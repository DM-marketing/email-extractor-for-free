"""Email extraction: regex, obfuscation patterns, mailto/data-* hints."""

from __future__ import annotations

import re
from typing import Iterable

# RFC-ish practical pattern (not full RFC 5322).
_EMAIL_RE = re.compile(
    r"(?<![A-Za-z0-9._%+-])"
    r"([A-Za-z0-9][A-Za-z0-9._%+-]*@[A-Za-z0-9][A-Za-z0-9.-]*\.[A-Za-z]{2,63})"
    r"(?![A-Za-z0-9._%+-])"
)

_MAILTO_RE = re.compile(
    r'mailto:\s*([A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,63})',
    re.I,
)

_DATA_EMAIL_RE = re.compile(
    r"""data-email\s*=\s*['\"]([A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,63})['\"]""",
    re.I,
)

# "name [at] domain [dot] com" and variants (language-neutral tokens).
_OBFUSCATION_PATTERNS = [
    (
        re.compile(
            r"\b([A-Za-z0-9._%+-]+)\s*(?:\[at\]|\(at\)|\s+at\s+)\s*"
            r"([A-Za-z0-9.-]+)\s*(?:\[dot\]|\(dot\)|\s+dot\s+)\s*"
            r"([A-Za-z]{2,63})\b",
            re.I,
        ),
        lambda m: f"{m.group(1)}@{m.group(2)}.{m.group(3)}",
    ),
]

_BAD_LOCALPART_HINTS = ("example", "test", "sample", "email", "yourname", "username")
_BAD_EXT = (
    ".png",
    ".jpg",
    ".jpeg",
    ".gif",
    ".webp",
    ".svg",
    ".css",
    ".js",
    ".ico",
    ".woff",
    ".ttf",
)


def _normalize_obfuscated_text(text: str) -> str:
    t = text
    replacements = [
        (" [at] ", "@"),
        ("(at)", "@"),
        (" at ", "@"),
        ("[@]", "@"),
        (" [dot] ", "."),
        ("(dot)", "."),
        (" dot ", "."),
        ("[dot]", "."),
        ("[at]", "@"),
    ]
    for a, b in replacements:
        t = t.replace(a, b)
    t = re.sub(r"\s+@\s+", "@", t)
    t = re.sub(r"\s+\.\s+", ".", t)
    return t


def extract_emails_from_text(raw: str) -> list[str]:
    """Return unique lowercase emails from HTML/text."""
    if not raw:
        return []

    collected: list[str] = []

    for m in _MAILTO_RE.finditer(raw):
        collected.append(m.group(1).lower())

    for m in _DATA_EMAIL_RE.finditer(raw):
        collected.append(m.group(1).lower())

    prepared = _normalize_obfuscated_text(raw.lower())
    for pattern, _ in _OBFUSCATION_PATTERNS:
        for m in pattern.finditer(prepared):
            try:
                email = f"{m.group(1)}@{m.group(2)}.{m.group(3)}".lower()
                collected.append(email)
            except IndexError:
                continue

    for m in _EMAIL_RE.finditer(prepared):
        collected.append(m.group(1).lower())

    # De-dupe preserving order
    seen: set[str] = set()
    out: list[str] = []
    for e in collected:
        if e in seen:
            continue
        seen.add(e)
        if any(e.endswith(ext) for ext in _BAD_EXT):
            continue
        local = e.split("@", 1)[0]
        if any(h in local for h in _BAD_LOCALPART_HINTS):
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
