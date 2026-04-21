"""File-based cooperative stop for Playwright domain collection (GUI / CLI)."""

from __future__ import annotations

import logging
from pathlib import Path

from email_scraper_project.config import data_dir

logger = logging.getLogger("leadgen.playwright_collect")

STOP_FILENAME = "leadgen_stop_collection.txt"


def stop_collection_request_path() -> Path:
    return data_dir() / STOP_FILENAME


def request_stop_collection() -> None:
    """Create the stop marker file; collector polls this between steps."""
    p = stop_collection_request_path()
    p.parent.mkdir(parents=True, exist_ok=True)
    try:
        p.write_text("1\n", encoding="utf-8")
    except OSError as e:
        logger.warning("Could not write stop collection file %s: %s", p, e)
    else:
        logger.info("Stop collection requested (%s)", p)


def clear_stop_collection_request() -> None:
    try:
        stop_collection_request_path().unlink(missing_ok=True)
    except OSError:
        pass


def peek_stop_collection_requested() -> bool:
    p = stop_collection_request_path()
    if not p.is_file():
        return False
    try:
        return p.stat().st_size > 0
    except OSError:
        return False


__all__ = [
    "STOP_FILENAME",
    "stop_collection_request_path",
    "request_stop_collection",
    "clear_stop_collection_request",
    "peek_stop_collection_requested",
]
