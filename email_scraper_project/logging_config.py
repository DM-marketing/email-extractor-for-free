"""Structured logging helpers for domain collection and crawling."""

from __future__ import annotations

import json
import logging
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping, Optional, TextIO


class JsonLogFormatter(logging.Formatter):
    """One JSON object per line for machine-friendly logs."""

    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, Any] = {
            "ts": datetime.now(timezone.utc).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }
        extra = getattr(record, "structured", None)
        if isinstance(extra, Mapping):
            payload["data"] = dict(extra)
        if record.exc_info:
            payload["exc_info"] = self.formatException(record.exc_info)
        return json.dumps(payload, ensure_ascii=False)


def setup_logging(
    name: str = "leadgen",
    level: int = logging.INFO,
    json_file: Optional[str] = None,
    stream: Optional[TextIO] = None,
) -> logging.Logger:
    logger = logging.getLogger(name)
    logger.setLevel(level)
    logger.handlers.clear()

    h = logging.StreamHandler(stream or sys.stdout)
    h.setLevel(level)
    h.setFormatter(
        logging.Formatter("%(asctime)s | %(levelname)s | %(name)s | %(message)s")
    )
    logger.addHandler(h)

    if json_file:
        fh = logging.FileHandler(json_file, encoding="utf-8")
        fh.setLevel(level)
        fh.setFormatter(JsonLogFormatter())
        logger.addHandler(fh)

    return logger


def log_event(logger: logging.Logger, message: str, **data: Any) -> None:
    """Log with optional structured payload on the LogRecord."""
    logger.info(message, extra={"structured": data})


def ensure_leadgen_file_log(log_path: Path | None = None) -> Path:
    """
    Attach a single FileHandler on the ``leadgen`` logger (children propagate here).
    Returns the log file path (default: logs.txt in data_dir).
    """
    from email_scraper_project.config import main_log_txt_path

    path = log_path if log_path is not None else main_log_txt_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    log = logging.getLogger("leadgen")
    log.setLevel(logging.INFO)
    resolved = str(path.resolve())
    for h in log.handlers:
        if isinstance(h, logging.FileHandler):
            bf = getattr(h, "baseFilename", "") or ""
            if bf.replace("\\", "/").lower() == resolved.replace("\\", "/").lower():
                return path
    fh = logging.FileHandler(path, encoding="utf-8")
    fh.setFormatter(logging.Formatter("%(asctime)s | %(levelname)s | %(name)s | %(message)s"))
    log.addHandler(fh)
    return path
