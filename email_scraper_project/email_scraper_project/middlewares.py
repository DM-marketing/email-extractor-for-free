"""Downloader middleware: rotating browser-like headers and optional free proxies."""

from __future__ import annotations

import os
import random
from scrapy import Request
from scrapy.crawler import Crawler

from email_scraper_project.search_engine.client import default_headers


class RotatingHeadersMiddleware:
    """Assign realistic User-Agent and Accept headers per outbound request."""

    @classmethod
    def from_crawler(cls, crawler: Crawler):
        return cls()

    def process_request(self, request: Request):
        if request.meta.get("skip_header_rotation"):
            return None
        h = default_headers()
        for k, v in h.items():
            request.headers.setdefault(k, v)
        return None


class OptionalFreeProxyMiddleware:
    """If LEADGEN_USE_PROXIES=1, attach a random free HTTP proxy (best-effort)."""

    _manager = None

    def __init__(self) -> None:
        self.enabled = os.environ.get("LEADGEN_USE_PROXIES", "").lower() in (
            "1",
            "true",
            "yes",
        )

    @classmethod
    def from_crawler(cls, crawler: Crawler):
        return cls()

    def _mgr(self):
        if OptionalFreeProxyMiddleware._manager is None:
            from email_scraper_project.proxy_manager import ProxyManager

            OptionalFreeProxyMiddleware._manager = ProxyManager()
        return OptionalFreeProxyMiddleware._manager

    def process_request(self, request: Request):
        if not self.enabled or request.meta.get("proxy"):
            return None
        try:
            mgr = self._mgr()
            p = mgr.pick()
            if p:
                request.meta["proxy"] = p
        except Exception:
            pass
        return None

    def process_exception(self, request: Request, exception: BaseException) -> None:
        if not self.enabled:
            return None
        proxy = request.meta.get("proxy")
        if proxy:
            try:
                self._mgr().mark_bad(proxy)
            except Exception:
                pass
        return None
