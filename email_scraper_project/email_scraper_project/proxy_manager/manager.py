"""Free HTTP proxy list fetch + dead-proxy eviction (best-effort, optional)."""

from __future__ import annotations

import logging
import random
import threading
import time
from typing import Optional

import requests

logger = logging.getLogger("leadgen.proxy")

# Public free endpoints (no API key). Reliability varies.
_PROXY_SOURCES = (
    "https://api.proxyscrape.com/v2/?request=get&protocol=http&timeout=10000&country=all&ssl=all&anonymity=all",
    "https://raw.githubusercontent.com/TheSpeedX/PROXY-List/master/http.txt",
)


class ProxyManager:
    """Thread-safe round-robin over proxies with simple failure scoring."""

    def __init__(
        self,
        refresh_seconds: int = 600,
        test_url: str = "http://httpbin.org/ip",
        test_timeout: float = 8.0,
    ) -> None:
        self._lock = threading.Lock()
        self._proxies: list[str] = []
        self._failures: dict[str, int] = {}
        self._refresh_seconds = refresh_seconds
        self._last_fetch = 0.0
        self._test_url = test_url
        self._test_timeout = test_timeout

    def _parse_lines(self, text: str) -> list[str]:
        out: list[str] = []
        for line in text.splitlines():
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            if "://" in line:
                line = line.split("://", 1)[-1]
            if ":" in line and all(c.isdigit() or c == "." or c == ":" for c in line.split("@")[-1]):
                hostport = line.split("@")[-1]
                out.append(f"http://{hostport}")
        return out

    def fetch_list(self) -> list[str]:
        found: list[str] = []
        for src in _PROXY_SOURCES:
            try:
                r = requests.get(src, timeout=20)
                r.raise_for_status()
                found.extend(self._parse_lines(r.text))
            except Exception as e:
                logger.debug("proxy source failed %s: %s", src, e)
        # de-dupe, cap size
        uniq = list(dict.fromkeys(found))[:200]
        log_event = getattr(logger, "info", None)
        if uniq and log_event:
            logger.info("Loaded %s candidate proxies", len(uniq))
        return uniq

    def ensure_pool(self) -> None:
        now = time.time()
        with self._lock:
            if self._proxies and (now - self._last_fetch) < self._refresh_seconds:
                return
        fresh = self.fetch_list()
        with self._lock:
            self._proxies = fresh
            self._failures.clear()
            self._last_fetch = now

    def mark_bad(self, proxy_url: str) -> None:
        with self._lock:
            self._failures[proxy_url] = self._failures.get(proxy_url, 0) + 1

    def pick(self) -> Optional[str]:
        self.ensure_pool()
        with self._lock:
            candidates = [p for p in self._proxies if self._failures.get(p, 0) < 3]
            if not candidates:
                return None
            return random.choice(candidates)

    def requests_proxies_dict(self, proxy_url: Optional[str]) -> Optional[dict[str, str]]:
        if not proxy_url:
            return None
        return {"http": proxy_url, "https": proxy_url}

    def quick_validate(self, proxy_url: str) -> bool:
        try:
            r = requests.get(
                self._test_url,
                proxies=self.requests_proxies_dict(proxy_url),
                timeout=self._test_timeout,
            )
            return r.status_code == 200
        except Exception:
            return False
