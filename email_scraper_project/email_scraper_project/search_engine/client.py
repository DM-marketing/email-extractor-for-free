"""Multi-engine search with pagination, retries, URL normalization, and failover."""

from __future__ import annotations

import logging
import random
import time
from typing import Callable, Optional

import requests
from bs4 import BeautifulSoup
from requests.exceptions import (
    ConnectTimeout,
    ConnectionError as RequestsConnectionError,
    ProxyError,
    RequestException,
)
from urllib.parse import urlparse

from email_scraper_project.browser_search.bing_url_decode import resolve_search_result_href
from email_scraper_project.domain_cleaner import clean_domain, normalize_url
from email_scraper_project.lead_qualifier import should_drop_collected_host

logger = logging.getLogger("leadgen.search")

UserAgentFactory = Callable[[], dict[str, str]]


def _expanded_user_agents() -> list[str]:
    return [
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2 Safari/605.1.15",
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:123.0) Gecko/20100101 Firefox/123.0",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:123.0) Gecko/20100101 Firefox/123.0",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36 Edg/122.0.0.0",
    ]


def default_headers() -> dict[str, str]:
    ua = random.choice(_expanded_user_agents())
    langs = ["en-US,en;q=0.9", "en-GB,en;q=0.9", "en-US,en;q=0.8,es;q=0.5"]
    return {
        "User-Agent": ua,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": random.choice(langs),
        "Cache-Control": "no-cache",
        "Pragma": "no-cache",
    }


def _should_retry_without_proxy(exc: BaseException) -> bool:
    """Detect dead/broken HTTP proxies (free lists, bad tunnels, refused connections)."""
    if isinstance(exc, (ProxyError, ConnectTimeout, RequestsConnectionError)):
        return True
    msg = str(exc).lower()
    if "proxy" in msg or "tunnel connection failed" in msg or "bad request" in msg:
        return True
    if "actively refused" in msg or "10061" in msg:
        return True
    if "max retries exceeded" in msg and (
        "proxy" in msg or "connecttimeout" in msg or "connection" in msg
    ):
        return True
    return False


class SearchClient:
    ENGINES = ("duckduckgo", "bing", "yahoo", "startpage")

    def __init__(
        self,
        session: Optional[requests.Session] = None,
        delay_range: tuple[float, float] = (2.0, 5.0),
        max_retries: int = 3,
        proxies_dict: Optional[dict[str, str]] = None,
        trust_env: bool = False,
    ) -> None:
        self.session = session or requests.Session()
        # Windows/macOS often set HTTP(S)_PROXY; those break search if misconfigured.
        self.session.trust_env = trust_env
        self.delay_range = delay_range
        self.max_retries = max_retries
        self.proxies_dict = proxies_dict

    def _sleep_jitter(self) -> None:
        time.sleep(random.uniform(*self.delay_range))

    def _get(self, url: str, **kwargs) -> requests.Response:
        kwargs.setdefault("timeout", 25)
        kwargs.setdefault("headers", default_headers())
        if self.proxies_dict:
            kwargs["proxies"] = dict(self.proxies_dict)

        had_explicit_proxy = bool(self.proxies_dict)
        direct_fallback_used = False
        last_exc: Exception | None = None

        for attempt in range(self.max_retries):
            try:
                r = self.session.get(url, **kwargs)
                if r.status_code in (403, 429):
                    wait = (attempt + 1) * 5 + random.uniform(1, 4)
                    logger.warning(
                        "HTTP %s for %s — backing off %.1fs",
                        r.status_code,
                        urlparse(url).netloc,
                        wait,
                    )
                    time.sleep(wait)
                    kwargs["headers"] = default_headers()
                    continue
                r.raise_for_status()
                return r
            except Exception as e:
                last_exc = e
                if (
                    had_explicit_proxy or kwargs.get("proxies")
                ) and not direct_fallback_used and _should_retry_without_proxy(e):
                    logger.warning(
                        "Search via proxy failed (%s); retrying same URL with direct connection.",
                        e,
                    )
                    kwargs.pop("proxies", None)
                    direct_fallback_used = True
                    kwargs["headers"] = default_headers()
                    time.sleep(random.uniform(0.6, 1.8))
                    continue

                if isinstance(e, RequestException):
                    wait = (attempt + 1) * 2 + random.uniform(0.5, 2)
                    logger.debug("request retry %s: %s", attempt + 1, e)
                    time.sleep(wait)
                    kwargs["headers"] = default_headers()
                    continue
                raise
        raise last_exc if last_exc else RuntimeError("request failed")

    def fetch_duckduckgo(self, query: str, start: int) -> list[str]:
        url = "https://html.duckduckgo.com/html/"
        params = {"q": query, "s": str(start)}
        r = self._get(url, params=params)
        soup = BeautifulSoup(r.text, "html.parser")
        links: list[str] = []
        for a in soup.select("a.result__a"):
            href = a.get("href")
            if not href:
                continue
            norm = normalize_url(href) or href
            if norm:
                links.append(norm)
        return links

    def fetch_bing(self, query: str, first: int) -> list[str]:
        url = "https://www.bing.com/search"
        params = {"q": query, "first": str(first)}
        r = self._get(url, params=params)
        soup = BeautifulSoup(r.text, "html.parser")
        links: list[str] = []
        for a in soup.select("li.b_algo h2 a"):
            href = a.get("href")
            if href:
                resolved = resolve_search_result_href(href)
                n = normalize_url(resolved) or resolved
                links.append(n)
        return links

    def fetch_yahoo(self, query: str, b: int) -> list[str]:
        url = "https://search.yahoo.com/search"
        params = {"p": query, "b": str(b)}
        r = self._get(url, params=params)
        soup = BeautifulSoup(r.text, "html.parser")
        links: list[str] = []
        for a in soup.select("div.algo h3 a, h3.title a"):
            href = a.get("href")
            if not href:
                continue
            n = normalize_url(href) or href
            if n.startswith("http"):
                links.append(n)
        for a in soup.find_all("a", href=True):
            href = a["href"]
            if "RU=" in href:
                n = normalize_url(href)
                if n:
                    links.append(n)
        return links

    def fetch_startpage(self, query: str, page: int) -> list[str]:
        url = "https://www.startpage.com/sp/search"
        params = {"query": query, "page": str(page)}
        r = self._get(url, params=params)
        soup = BeautifulSoup(r.text, "html.parser")
        links: list[str] = []
        for sel in ("a.w-gl__result-title", "a.result-link"):
            for a in soup.select(sel):
                href = a.get("href")
                if href:
                    n = normalize_url(href) or href
                    links.append(n)
        return links

    def search_engine_domains(
        self,
        engine: str,
        query: str,
        max_pages: int,
        offset_step: int = 10,
    ) -> list[str]:
        domains: list[str] = []
        seen: set[str] = set()
        try:
            for page_idx in range(max_pages):
                if engine == "duckduckgo":
                    links = self.fetch_duckduckgo(query, page_idx * offset_step)
                elif engine == "bing":
                    links = self.fetch_bing(query, 1 + page_idx * 10)
                elif engine == "yahoo":
                    links = self.fetch_yahoo(query, 1 + page_idx * 10)
                elif engine == "startpage":
                    links = self.fetch_startpage(query, page_idx + 1)
                else:
                    break

                if not links:
                    logger.info("engine=%s query=%r page=%s no links", engine, query, page_idx)
                    break

                for link in links:
                    low = link.lower()
                    if "y.js" in low:
                        continue
                    if "bing.com" in low and ("/ck/" in low or "aclick" in low):
                        continue
                    d = clean_domain(link)
                    if d and d not in seen and not should_drop_collected_host(d):
                        seen.add(d)
                        domains.append(d)

                self._sleep_jitter()
        except Exception as e:
            logger.error("engine %s failed for %r: %s", engine, query, e)
        return domains

    def search_with_failover(
        self,
        query: str,
        max_pages_per_engine: int,
        engines_order: Optional[tuple[str, ...]] = None,
    ) -> dict[str, list[str]]:
        order = engines_order or self.ENGINES
        results: dict[str, list[str]] = {}
        for eng in order:
            try:
                found = self.search_engine_domains(eng, query, max_pages_per_engine)
                results[eng] = found
                if found:
                    logger.info("engine=%s domains=%s query=%r", eng, len(found), query)
            except Exception as e:
                logger.error("failover skip engine=%s err=%s", eng, e)
                results[eng] = []
            self._sleep_jitter()
        return results


def build_search_queries(
    keywords: str,
    country: str,
    state: str = "",
    city: str = "",
    industry: str = "",
    *,
    b2b_enrich: bool = False,
) -> list[str]:
    """Generate diverse query strings from user inputs."""
    kws = [k.strip() for k in keywords.replace(";", ",").split(",") if k.strip()]
    if not kws:
        kws = ["business"]

    loc_bits = [p.strip() for p in (city, state, country) if p and p.strip()]
    location = " ".join(loc_bits) if loc_bits else (country or "").strip() or "USA"

    industry = (industry or "").strip()
    queries: list[str] = []
    templates = [
        "{kw} in {loc}",
        "{kw} near {loc}",
        "{kw} {loc}",
        "{loc} {kw}",
    ]
    if industry:
        templates.extend(
            [
                "{kw} {ind} in {loc}",
                "{ind} {kw} {loc}",
            ]
        )

    seen: set[str] = set()
    for kw in kws:
        for tpl in templates:
            q = tpl.format(kw=kw, loc=location, ind=industry).strip()
            q = " ".join(q.split())
            if q and q not in seen:
                seen.add(q)
                queries.append(q)
    if b2b_enrich:
        st_only = (state or "").strip()
        for kw in kws:
            for extra in (
                f"{kw} company {location}".strip(),
                f"{kw} services {location}".strip(),
            ):
                q = " ".join(extra.split())
                if q and q not in seen:
                    seen.add(q)
                    queries.append(q)
            if st_only:
                for extra in (
                    f"{kw} company {st_only}".strip(),
                    f"{kw} services {st_only} {country or 'USA'}".strip(),
                ):
                    q = " ".join(extra.split())
                    if q and q not in seen:
                        seen.add(q)
                        queries.append(q)
    return queries
