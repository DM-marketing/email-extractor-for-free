"""
Playwright-based search: Bing, DuckDuckGo, Yahoo, Google (optional).

Each enabled engine runs for every query, up to per_engine_max new domains per engine
(so Bing cannot consume the whole budget before others run).
"""

from __future__ import annotations

import logging
import random
import time
from pathlib import Path
from typing import Callable, Optional
from urllib.parse import parse_qs, quote_plus, unquote, urljoin, urlparse

from playwright.sync_api import Page, sync_playwright

from email_scraper_project.browser_search.bing_url_decode import decode_bing_tracking_url
from email_scraper_project.browser_search.query_builder import build_playwright_queries
from email_scraper_project.config import domains_path
from email_scraper_project.domain_cleaner import clean_domain
from email_scraper_project.logging_config import ensure_leadgen_file_log

logger = logging.getLogger("leadgen.playwright_collect")

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:123.0) Gecko/20100101 Firefox/123.0",
]

ENGINE_ORDER_DEFAULT: tuple[str, ...] = ("bing", "duckduckgo", "yahoo", "google")

# Max SERP pages (first page + paginations) per engine when not overridden.
DEFAULT_SERP_PAGES_PER_ENGINE: dict[str, int] = {
    "bing": 10,
    "duckduckgo": 10,
    "yahoo": 10,
    "google": 10,
}


def merge_serp_pages_per_engine(user: dict[str, int] | None) -> dict[str, int]:
    """Merge user overrides (only keys with value >= 1) into defaults; cap at 100."""
    out = dict(DEFAULT_SERP_PAGES_PER_ENGINE)
    if user:
        for k, v in user.items():
            kk = k.lower().strip()
            if kk == "ddg":
                kk = "duckduckgo"
            if kk in out and isinstance(v, int) and v >= 1:
                out[kk] = min(int(v), 100)
    return out


def _jitter(min_s: float = 2.0, max_s: float = 5.0) -> None:
    time.sleep(random.uniform(min_s, max_s))


def _maybe_captcha(
    page: Page,
    captcha_mode: str,
    captcha_wait_ms: int,
) -> None:
    url = page.url.lower()
    try:
        body = page.content().lower()
    except Exception:
        body = ""
    suspicious = (
        "recaptcha" in body
        or "/sorry/" in url
        or "unusual traffic" in body
        or "captcha" in body
        or "verify you are human" in body
    )
    if not suspicious:
        return
    logger.warning("CAPTCHA or bot-check detected (url=%s)", page.url)
    if captcha_mode == "stdin":
        print(
            "\n>>> Solve the challenge in the browser window, then press Enter here to continue...\n",
            flush=True,
        )
        try:
            input()
        except EOFError:
            logger.warning("No stdin (EOF); waiting %s ms instead.", captcha_wait_ms)
            page.wait_for_timeout(min(captcha_wait_ms, 300_000))
    else:
        logger.warning("Waiting %s ms for manual solve…", captcha_wait_ms)
        page.wait_for_timeout(captcha_wait_ms)


def _scroll_results(page: Page) -> None:
    try:
        page.evaluate("window.scrollBy(0, Math.min(2000, document.body.scrollHeight))")
        page.mouse.wheel(0, 1800)
    except Exception:
        pass
    _jitter(0.8, 1.8)


def _resolve_href_with_browser(page: Page, href: str) -> str:
    if not href or href.startswith("#"):
        return href
    abs_url = href if href.startswith("http") else urljoin(page.url, href)
    if "bing.com" not in abs_url.lower():
        return abs_url
    newp = page.context.new_page()
    try:
        newp.goto(abs_url, wait_until="domcontentloaded", timeout=25_000)
        return newp.url or abs_url
    except Exception as e:
        logger.debug("browser resolve failed %s: %s", abs_url, e)
        return abs_url
    finally:
        try:
            newp.close()
        except Exception:
            pass


def _harvest_href(page: Page, href: str, domains: set[str], seen_urls: set[str]) -> None:
    if not href or href.startswith("#"):
        return
    abs_url = href if href.startswith("http") else urljoin(page.url, href)
    target = decode_bing_tracking_url(abs_url) or abs_url
    low = target.lower()
    if "bing.com" in urlparse(target).netloc.lower() and "aclick" in low:
        target = _resolve_href_with_browser(page, abs_url)
    if target in seen_urls:
        return
    seen_urls.add(target)
    host = clean_domain(target)
    if host:
        domains.add(host)


def _ddg_resolve_href(href: str) -> str | None:
    if not href:
        return None
    h = href.strip()
    if h.startswith("//"):
        h = "https:" + h
    if "uddg=" in h:
        try:
            qs = parse_qs(urlparse(h).query)
            u = qs.get("uddg", [""])[0]
            if u:
                return unquote(u)
        except Exception:
            pass
    if h.startswith("http") and "duckduckgo.com" not in urlparse(h).netloc.lower():
        return h
    return None


def _harvest_ddg_href(page: Page, href: str, domains: set[str], seen_urls: set[str]) -> None:
    if not href or href.startswith("#"):
        return
    if href.startswith("/"):
        abs_url = urljoin(page.url, href)
    elif href.startswith("//"):
        abs_url = "https:" + href
    elif href.startswith("http"):
        abs_url = href
    else:
        abs_url = urljoin(page.url, href)
    target = _ddg_resolve_href(abs_url) or (
        abs_url if abs_url.startswith("http") and "duckduckgo.com" not in urlparse(abs_url).netloc.lower() else None
    )
    if not target:
        return
    if "duckduckgo.com" in urlparse(target).netloc.lower():
        return
    if target in seen_urls:
        return
    seen_urls.add(target)
    host = clean_domain(target)
    if host:
        domains.add(host)


def _bing_page(
    page: Page,
    query: str,
    domains: set[str],
    seen_urls: set[str],
    per_engine_cap: int,
    max_total: int | None,
    captcha_mode: str,
    captcha_wait_ms: int,
    max_serp_pages: int,
) -> None:
    start = len(domains)
    url = f"https://www.bing.com/search?q={quote_plus(query)}"
    page.goto(url, wait_until="domcontentloaded", timeout=60_000)
    _maybe_captcha(page, captcha_mode, captcha_wait_ms)

    serp_done = 0
    while (len(domains) - start) < per_engine_cap and serp_done < max_serp_pages:
        if max_total is not None and len(domains) >= max_total:
            break
        _maybe_captcha(page, captcha_mode, captcha_wait_ms)
        page.wait_for_timeout(800)
        _scroll_results(page)

        for a in page.locator("li.b_algo h2 a").all():
            if (len(domains) - start) >= per_engine_cap:
                break
            if max_total is not None and len(domains) >= max_total:
                break
            try:
                href = a.get_attribute("href")
                if href:
                    _harvest_href(page, href, domains, seen_urls)
            except Exception:
                continue

        for a in page.locator('li.b_algo a[href*="bing.com/aclick"]').all():
            if (len(domains) - start) >= per_engine_cap:
                break
            if max_total is not None and len(domains) >= max_total:
                break
            try:
                href = a.get_attribute("href")
                if href:
                    _harvest_href(page, href, domains, seen_urls)
            except Exception:
                continue

        serp_done += 1
        if (len(domains) - start) >= per_engine_cap or serp_done >= max_serp_pages:
            break

        pag = page.locator("a.sb_pagN")
        if pag.count() == 0:
            break
        try:
            pag.first.click()
            page.wait_for_load_state("domcontentloaded", timeout=45_000)
            _jitter()
        except Exception:
            break


def _duckduckgo_page(
    page: Page,
    query: str,
    domains: set[str],
    seen_urls: set[str],
    per_engine_cap: int,
    max_total: int | None,
    captcha_mode: str,
    captcha_wait_ms: int,
    max_serp_pages: int,
) -> None:
    """HTML SERP; pagination via ``s`` offset."""
    start = len(domains)
    offset = 0
    serp_done = 0
    while (len(domains) - start) < per_engine_cap and serp_done < max_serp_pages:
        if max_total is not None and len(domains) >= max_total:
            break
        url = f"https://duckduckgo.com/html/?q={quote_plus(query)}&s={offset}"
        page.goto(url, wait_until="domcontentloaded", timeout=60_000)
        _maybe_captcha(page, captcha_mode, captcha_wait_ms)
        page.wait_for_timeout(700)
        _scroll_results(page)

        before = len(domains)
        for a in page.locator("a.result__a").all():
            if (len(domains) - start) >= per_engine_cap:
                break
            if max_total is not None and len(domains) >= max_total:
                break
            try:
                href = a.get_attribute("href")
                if href:
                    _harvest_ddg_href(page, href, domains, seen_urls)
            except Exception:
                continue

        serp_done += 1
        if len(domains) == before:
            break
        if serp_done >= max_serp_pages:
            break
        offset += 10
        _jitter()


def _yahoo_page(
    page: Page,
    query: str,
    domains: set[str],
    seen_urls: set[str],
    per_engine_cap: int,
    max_total: int | None,
    captcha_mode: str,
    captcha_wait_ms: int,
    max_serp_pages: int,
) -> None:
    start = len(domains)
    url = f"https://search.yahoo.com/search?p={quote_plus(query)}"
    page.goto(url, wait_until="domcontentloaded", timeout=60_000)
    _maybe_captcha(page, captcha_mode, captcha_wait_ms)

    serp_done = 0
    while (len(domains) - start) < per_engine_cap and serp_done < max_serp_pages:
        if max_total is not None and len(domains) >= max_total:
            break
        _scroll_results(page)
        for sel in ("h3.title a", "a[data-mysyg-url]", "div.algo a"):
            for a in page.locator(sel).all():
                if (len(domains) - start) >= per_engine_cap:
                    break
                if max_total is not None and len(domains) >= max_total:
                    break
                try:
                    href = a.get_attribute("href")
                    if not href or "yahoo.com" in href or href.startswith("/"):
                        continue
                    _harvest_href(page, href, domains, seen_urls)
                except Exception:
                    continue
        serp_done += 1
        if (len(domains) - start) >= per_engine_cap or serp_done >= max_serp_pages:
            break
        next_loc = page.locator("a.next")
        if next_loc.count() == 0:
            next_loc = page.locator('a[aria-label="Next"]')
        try:
            if next_loc.count() == 0:
                break
            next_loc.first.click()
            page.wait_for_load_state("domcontentloaded", timeout=45_000)
            _jitter()
        except Exception:
            break


def _google_page(
    page: Page,
    query: str,
    domains: set[str],
    seen_urls: set[str],
    per_engine_cap: int,
    max_total: int | None,
    captcha_mode: str,
    captcha_wait_ms: int,
    max_serp_pages: int,
) -> None:
    start = len(domains)
    url = f"https://www.google.com/search?q={quote_plus(query)}&num=100"
    page.goto(url, wait_until="domcontentloaded", timeout=60_000)
    _maybe_captcha(page, captcha_mode, max(captcha_wait_ms, 600_000))

    serp_done = 0
    while (len(domains) - start) < per_engine_cap and serp_done < max_serp_pages:
        if max_total is not None and len(domains) >= max_total:
            break
        _scroll_results(page)
        for a in page.locator("div#search a").all():
            if (len(domains) - start) >= per_engine_cap:
                break
            if max_total is not None and len(domains) >= max_total:
                break
            try:
                href = a.get_attribute("href")
                if not href or href.startswith("#"):
                    continue
                if "/url?q=" in href:
                    q = parse_qs(urlparse(href).query).get("q", [""])[0]
                    href = q or href
                if "google.com" in href:
                    continue
                _harvest_href(page, href, domains, seen_urls)
            except Exception:
                continue
        serp_done += 1
        if (len(domains) - start) >= per_engine_cap or serp_done >= max_serp_pages:
            break
        nxt = page.locator("a#pnnext, td.b a#pnnext")
        try:
            if nxt.count() == 0:
                break
            nxt.first.click()
            page.wait_for_load_state("domcontentloaded", timeout=45_000)
            _jitter()
        except Exception:
            break


_ENGINE_FUNCS = {
    "bing": _bing_page,
    "duckduckgo": _duckduckgo_page,
    "yahoo": _yahoo_page,
    "google": _google_page,
}


def collect_domains_playwright(
    keyword: str = "",
    country: str = "",
    max_results: int = 100,
    *,
    states: str = "",
    queries: Optional[list[str]] = None,
    per_engine_max: Optional[int] = None,
    use_bing: bool = True,
    use_duckduckgo: bool = True,
    use_yahoo: bool = True,
    use_google: bool = False,
    engine_order: tuple[str, ...] = ENGINE_ORDER_DEFAULT,
    pages_per_engine: dict[str, int] | None = None,
    headless: bool = False,
    output_path: Optional[Path] = None,
    append: bool = True,
    captcha_mode: str = "stdin",
    captcha_wait_ms: int = 300_000,
    log_callback: Optional[Callable[[str], None]] = None,
) -> dict[str, int]:
    """
    For each query, run each **enabled** engine in ``engine_order``, collecting up to
    ``per_engine_max`` **new** domains from that engine (then moving on so all engines run).

    ``max_results`` caps **total** unique domains; when reached, remaining engines/queries stop.

    ``pages_per_engine``: optional map e.g. ``{"bing": 1, "google": 2, "duckduckgo": 3}``.
    Omitted engines use ``DEFAULT_SERP_PAGES_PER_ENGINE`` (10). Each value is max **SERP**
    pages (first results page counts as 1).
    """
    ensure_leadgen_file_log()
    serp_limits = merge_serp_pages_per_engine(pages_per_engine)
    out = Path(output_path) if output_path else domains_path()
    out.parent.mkdir(parents=True, exist_ok=True)

    if queries is None:
        queries = build_playwright_queries(
            keyword or "business",
            country or "",
            states or "",
        )
    if not queries:
        queries = ["business"]

    flags = {
        "bing": use_bing,
        "duckduckgo": use_duckduckgo,
        "yahoo": use_yahoo,
        "google": use_google,
    }
    enabled = [e for e in engine_order if e in _ENGINE_FUNCS and flags.get(e, False)]
    if not enabled:
        logger.warning("No search engines enabled.")
        return {"new_domains": 0, "total_collected": 0, "output": str(out)}

    n_en = len(enabled)
    if per_engine_max is None:
        per_engine_max = max(15, min(60, max(20, max_results // max(1, n_en))))

    existing: set[str] = set()
    if append and out.exists():
        for line in out.read_text(encoding="utf-8", errors="ignore").splitlines():
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            u = line.replace("http://", "").replace("https://", "").split("/")[0]
            if u:
                existing.add(u.lower())

    domains: set[str] = set()
    seen_urls: set[str] = set()
    max_total = max(1, min(max_results, 5000))

    def log(msg: str) -> None:
        logger.info(msg)
        if log_callback:
            log_callback(msg)

    cap_mode = (captcha_mode or "stdin").lower()
    if cap_mode not in ("stdin", "wait"):
        cap_mode = "wait"

    lim_str = ", ".join(f"{e}={serp_limits[e]}" for e in enabled)
    log(
        f"Engines: {', '.join(enabled)} | {len(queries)} query/queries | "
        f"up to {per_engine_max} new domains per engine per query | total cap {max_total} | "
        f"SERP pages: {lim_str}"
    )

    with sync_playwright() as p:
        browser = p.chromium.launch(
            headless=headless,
            args=["--disable-blink-features=AutomationControlled"],
        )
        context = browser.new_context(
            user_agent=random.choice(USER_AGENTS),
            viewport={"width": 1366, "height": 768},
            locale="en-US",
        )
        page = context.new_page()
        try:
            stop_all = False
            for qi, query in enumerate(queries):
                if stop_all or len(domains) >= max_total:
                    break
                log(f"=== Query {qi + 1}/{len(queries)}: {query!r} ===")
                for eng in enabled:
                    if len(domains) >= max_total:
                        stop_all = True
                        break
                    before = len(domains)
                    npg = serp_limits.get(eng, DEFAULT_SERP_PAGES_PER_ENGINE.get(eng, 10))
                    log(
                        f"--- {eng.upper()} (target +{per_engine_max} domains; "
                        f"max {npg} SERP page(s); total cap {max_total}) ---"
                    )
                    try:
                        _ENGINE_FUNCS[eng](
                            page,
                            query,
                            domains,
                            seen_urls,
                            per_engine_max,
                            max_total,
                            cap_mode,
                            captcha_wait_ms,
                            npg,
                        )
                    except Exception as e:
                        logger.exception("Engine %s failed for %r: %s", eng, query, e)
                        log(f"ERROR {eng}: {e}")
                    added = len(domains) - before
                    log(f"--- {eng.upper()} done: +{added} domains (session total {len(domains)}) ---")
                    _jitter(1.0, 2.5)
        finally:
            context.close()
            browser.close()

    new_hosts = [d for d in domains if d.lower() not in existing]
    mode = "a" if append else "w"
    with open(out, mode, encoding="utf-8") as f:
        for d in sorted(new_hosts):
            f.write(f"https://{d}\n")
            existing.add(d.lower())

    log(
        f"Saved {len(new_hosts)} new domains to {out} "
        f"(unique this run: {len(domains)}; per-engine cap was {per_engine_max})"
    )
    return {
        "new_domains": len(new_hosts),
        "total_collected": len(domains),
        "output": str(out),
        "queries_run": len(queries),
        "engines": enabled,
    }


__all__ = [
    "collect_domains_playwright",
    "ENGINE_ORDER_DEFAULT",
    "DEFAULT_SERP_PAGES_PER_ENGINE",
    "merge_serp_pages_per_engine",
]
