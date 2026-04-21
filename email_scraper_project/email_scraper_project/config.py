"""Project paths and shared defaults (Scrapy project root = parent of inner package)."""

from __future__ import annotations

import os
from pathlib import Path

_PACKAGE_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = _PACKAGE_DIR.parent


def data_dir() -> Path:
    override = os.environ.get("LEADGEN_DATA_DIR", "").strip()
    if override:
        return Path(override).expanduser().resolve()
    return PROJECT_ROOT


def domains_path() -> Path:
    return data_dir() / "domains.txt"


def emails_csv_path() -> Path:
    return data_dir() / "emails.csv"


def emails_txt_path() -> Path:
    return data_dir() / "emails.txt"


def main_log_txt_path() -> Path:
    """Primary human-readable log file (append)."""
    return data_dir() / "logs.txt"


def leads_json_path() -> Path:
    return data_dir() / "leads.json"


def qualified_leads_csv_path() -> Path:
    return data_dir() / "qualified_leads.csv"


def outreach_ready_csv_path() -> Path:
    return data_dir() / "outreach_ready.csv"


def manual_leads_csv_path() -> Path:
    return data_dir() / "manual_leads.csv"


def manual_qualified_leads_csv_path() -> Path:
    return data_dir() / "manual_qualified_leads.csv"


def logs_dir() -> Path:
    d = data_dir() / "logs"
    d.mkdir(parents=True, exist_ok=True)
    return d
