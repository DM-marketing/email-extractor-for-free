"""File-based signal so the GUI (or user) can skip the current / next Playwright engine."""

from __future__ import annotations

import logging
from pathlib import Path

from email_scraper_project.config import data_dir

logger = logging.getLogger("leadgen.playwright_collect")

SKIP_FILENAME = "leadgen_skip_engine.txt"


def skip_engine_request_path() -> Path:
    return data_dir() / SKIP_FILENAME


def request_skip_engine(engine: str) -> None:
    """
    Write a skip request consumed by ``playwright_collector`` between engines
    and during CAPTCHA waits.

    Values: ``bing``, ``duckduckgo`` (or ``ddg``), ``yahoo``, ``google``, ``all``.
    """
    name = (engine or "").strip().lower()
    if not name:
        return
    p = skip_engine_request_path()
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(name, encoding="utf-8")
    logger.info("Skip engine requested: %s (file %s)", name, p)


def clear_skip_engine_request() -> None:
    try:
        skip_engine_request_path().unlink(missing_ok=True)
    except OSError:
        pass


def peek_skip_engine_request() -> str | None:
    p = skip_engine_request_path()
    if not p.is_file():
        return None
    try:
        line = p.read_text(encoding="utf-8", errors="ignore").strip().lower()
        return line or None
    except OSError:
        return None


def consume_skip_engine_request_if_matches(active_engine: str) -> bool:
    """
    If the skip file asks to skip ``active_engine`` (or ``all``), delete the file
    and return ``True``. Otherwise return ``False`` and leave the file unchanged.
    """
    if not (active_engine or "").strip():
        return False
    line = peek_skip_engine_request()
    if not line:
        return False
    aliases = {"ddg": "duckduckgo"}
    want = aliases.get(line, line)
    eng = aliases.get(active_engine.lower(), active_engine.lower())
    if want in ("all", "*", "any") or want == eng:
        clear_skip_engine_request()
        logger.info("Consuming skip-engine request for %s", eng)
        return True
    return False


__all__ = [
    "SKIP_FILENAME",
    "skip_engine_request_path",
    "request_skip_engine",
    "clear_skip_engine_request",
    "peek_skip_engine_request",
    "consume_skip_engine_request_if_matches",
]
