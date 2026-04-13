"""
Crawl domains from domains.txt, prioritize contact/about/team/privacy pages,
extract emails (including obfuscated patterns), export CSV: email, domain, source_url.
"""

from __future__ import annotations

import csv
from pathlib import Path
from urllib.parse import urlparse

import scrapy

from email_scraper_project.config import domains_path, emails_csv_path
from email_scraper_project.crawler.constants import CrawlDefaults
from email_scraper_project.email_extractor import extract_emails_from_text
from email_scraper_project.email_extractor.extract import iter_mailto_hrefs


class EmailSpider(scrapy.Spider):
    name = "email_spider"

    custom_settings = {
        "ROBOTSTXT_OBEY": False,
        "DOWNLOAD_TIMEOUT": 25,
        "RETRY_TIMES": 3,
        "RETRY_HTTP_CODES": [429, 500, 502, 503, 504],
        "CONCURRENT_REQUESTS": CrawlDefaults.CONCURRENT_REQUESTS,
        "CONCURRENT_REQUESTS_PER_DOMAIN": CrawlDefaults.CONCURRENT_REQUESTS_PER_DOMAIN,
        "DOWNLOAD_DELAY": CrawlDefaults.DOWNLOAD_DELAY,
        "AUTOTHROTTLE_ENABLED": True,
        "AUTOTHROTTLE_START_DELAY": 1,
        "AUTOTHROTTLE_MAX_DELAY": 30,
        "AUTOTHROTTLE_TARGET_CONCURRENCY": CrawlDefaults.AUTOTHROTTLE_TARGET_CONCURRENCY,
        "DOWNLOADER_MIDDLEWARES": {
            "email_scraper_project.middlewares.RotatingHeadersMiddleware": 400,
            "email_scraper_project.middlewares.OptionalFreeProxyMiddleware": 750,
        },
        "ITEM_PIPELINES": {
            "email_scraper_project.pipelines.JsonLinesExportPipeline": 250,
        },
    }

    HIGH_PRIORITY = (
        "contact",
        "about",
        "team",
        "privacy",
        "impressum",
        "kontakt",
    )

    collected_emails: set[str] = set()
    visited_urls: set[str] = set()
    domain_counts: dict[str, int] = {}
    emails_by_domain: dict[str, int] = {}

    def __init__(
        self,
        max_pages: str | None = None,
        early_stop: str | None = None,
        domains_file: str | None = None,
        *args,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self.max_pages = int(max_pages or CrawlDefaults.MAX_PAGES_PER_DOMAIN)
        self.early_stop = int(early_stop or CrawlDefaults.EARLY_STOP_EMAIL_COUNT)
        self._domains_file = domains_file or str(domains_path())

    @classmethod
    def update_settings(cls, settings):
        super().update_settings(settings)
        p = str(emails_csv_path())
        settings.set(
            "FEEDS",
            {
                p: {
                    "format": "csv",
                    "overwrite": settings.getbool("EMAIL_FEED_OVERWRITE", False),
                    "fields": ["email", "domain", "source_url"],
                }
            },
            priority="spider",
        )

    def _load_existing_csv_emails(self) -> None:
        path = emails_csv_path()
        if not path.exists():
            return
        try:
            with open(path, newline="", encoding="utf-8", errors="ignore") as f:
                reader = csv.DictReader(f)
                for row in reader:
                    em = (row.get("email") or row.get("Email") or "").strip().lower()
                    if em and "@" in em:
                        self.collected_emails.add(em)
        except Exception as e:
            self.logger.warning("Could not load existing emails CSV: %s", e)

    async def start(self):
        self._load_existing_csv_emails()
        dom_path = Path(self._domains_file)
        if not dom_path.is_file():
            self.logger.error("domains file missing: %s", self._domains_file)
            return

        with open(dom_path, "r", encoding="utf-8", errors="ignore") as f:
            lines = [
                ln.strip()
                for ln in f
                if ln.strip() and not ln.strip().startswith("#")
            ]

        if not lines:
            self.logger.error("domains file empty: %s", self._domains_file)
            return

        for line in lines:
            url = line
            if not url.startswith("http"):
                url = "http://" + url
            dom = urlparse(url).netloc
            self.domain_counts.setdefault(dom, 0)
            self.emails_by_domain.setdefault(dom, 0)
            yield scrapy.Request(
                url,
                callback=self.parse,
                errback=self.handle_error,
                dont_filter=True,
            )

    def handle_error(self, failure):
        req = failure.request
        self.logger.info(
            "request_failed",
            extra={"structured": {"url": req.url, "err": repr(failure.value)}},
        )

    def _path_depth(self, url: str) -> int:
        try:
            p = urlparse(url).path or "/"
            return len([x for x in p.split("/") if x])
        except Exception:
            return 0

    def _extract_from_response(self, response) -> list[str]:
        text = response.text or ""
        found = extract_emails_from_text(text)
        found.extend(iter_mailto_hrefs(response.css("a::attr(href)").getall()))
        # Basic JS-rendered hints: email in script assignment strings
        if "@" in text:
            for chunk in response.css("script::text").getall():
                found.extend(extract_emails_from_text(chunk))
        # De-dupe
        seen: set[str] = set()
        out: list[str] = []
        for e in found:
            el = e.lower().strip()
            if el not in seen:
                seen.add(el)
                out.append(el)
        return out

    def parse(self, response):
        if response.status in (403, 404, 429):
            self.logger.info(
                "blocked_or_missing",
                extra={"structured": {"status": response.status, "url": response.url}},
            )
            return

        domain = urlparse(response.url).netloc.lower()
        if domain.startswith("www."):
            domain = domain[4:]

        if self.domain_counts.get(domain, 0) >= self.max_pages:
            return

        self.domain_counts[domain] = self.domain_counts.get(domain, 0) + 1
        self.visited_urls.add(response.url)

        emails = self._extract_from_response(response)
        for email in emails:
            if email in self.collected_emails:
                continue
            self.collected_emails.add(email)
            self.emails_by_domain[domain] = self.emails_by_domain.get(domain, 0) + 1
            yield {
                "email": email,
                "domain": domain,
                "source_url": response.url,
            }

        if self.emails_by_domain.get(domain, 0) >= self.early_stop:
            self.logger.info(
                "early_stop_domain",
                extra={
                    "structured": {
                        "domain": domain,
                        "emails": self.emails_by_domain[domain],
                    }
                },
            )
            return

        if self._path_depth(response.url) > CrawlDefaults.MAX_DEPTH_SLASHES:
            return

        priority_links: list[str] = []
        normal_links: list[str] = []

        for link in response.css("a::attr(href)").getall():
            url = response.urljoin(link).strip()
            low = url.lower()

            if any(
                low.endswith(ext)
                for ext in (
                    ".png",
                    ".jpg",
                    ".jpeg",
                    ".gif",
                    ".webp",
                    ".svg",
                    ".css",
                    ".js",
                    ".ico",
                    ".pdf",
                    ".zip",
                )
            ):
                continue

            try:
                link_host = urlparse(low).netloc.lower()
            except Exception:
                continue
            if link_host.startswith("www."):
                link_host = link_host[4:]
            if link_host != domain:
                continue
            if low in self.visited_urls:
                continue

            if any(k in low for k in self.HIGH_PRIORITY):
                priority_links.append(low)
            else:
                normal_links.append(low)

        for url in priority_links[: CrawlDefaults.PRIORITY_LINK_CAP]:
            yield scrapy.Request(url, callback=self.parse, errback=self.handle_error)

        for url in normal_links[: CrawlDefaults.NORMAL_LINK_CAP]:
            yield scrapy.Request(url, callback=self.parse, errback=self.handle_error)
