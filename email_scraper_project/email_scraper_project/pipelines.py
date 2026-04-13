"""Optional JSONL export of lead items."""

from __future__ import annotations

import json
import os
from typing import Any, TextIO

from itemadapter import ItemAdapter

from email_scraper_project.config import leads_json_path


class JsonLinesExportPipeline:
    """Append each item as one JSON line to leads.json (UTF-8)."""

    def open_spider(self) -> None:
        self._fh: TextIO | None = None
        if os.environ.get("LEADGEN_JSON_EXPORT", "1").lower() in ("0", "false"):
            return
        path = leads_json_path()
        path.parent.mkdir(parents=True, exist_ok=True)
        self._fh = open(path, "a", encoding="utf-8")

    def close_spider(self) -> None:
        if self._fh:
            self._fh.close()
            self._fh = None

    def process_item(self, item: Any):
        if self._fh:
            row = dict(ItemAdapter(item))
            self._fh.write(json.dumps(row, ensure_ascii=False) + "\n")
        return item


class EmailScraperProjectPipeline:
    def process_item(self, item):
        return item
