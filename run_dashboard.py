"""Launch the Streamlit dashboard (run from this directory)."""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent
APP = ROOT / "email_scraper_project" / "gui" / "app.py"


def main() -> None:
    subprocess.run(
        [sys.executable, "-m", "streamlit", "run", str(APP)],
        cwd=str(ROOT),
        check=False,
    )


if __name__ == "__main__":
    main()
