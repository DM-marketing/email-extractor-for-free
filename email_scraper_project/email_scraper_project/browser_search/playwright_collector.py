"""
Playwright-based search: Bing, DuckDuckGo, Yahoo, Google (optional).

Each enabled engine runs for every query, up to per_engine_max new domains per engine
(so Bing cannot consume the whole budget before others run).
"""

from __future__ import annotations

import asyncio
import logging
import os
import random
import re
import sys
import time
from contextlib import contextmanager
from pathlib import Path
from typing import Callable, Optional
from urllib.parse import parse_qs, quote_plus, unquote, urljoin, urlparse

from playwright.sync_api import Page, sync_playwright

from email_scraper_project.browser_search.bing_url_decode import decode_bing_tracking_url
from email_scraper_project.browser_search.query_builder import build_playwright_queries
from email_scraper_project.browser_search.skip_engine_request import (
    clear_skip_engine_request,
    consume_skip_engine_request_if_matches,
)
from email_scraper_project.browser_search.stop_collection_request import (
    clear_stop_collection_request,
    peek_stop_collection_requested,
)
from email_scraper_project.browser_search.yahoo_url_decode import resolve_yahoo_result_href
from email_scraper_project.config import domains_path, logs_dir
from email_scraper_project.domain_cleaner import clean_domain
from email_scraper_project.lead_qualifier import should_drop_collected_host
from email_scraper_project.logging_config import ensure_leadgen_file_log

logger = logging.getLogger("leadgen.playwright_collect")

# Set before each engine run so skip-file polling targets the right name.
_active_pw_engine: str = ""


@contextmanager
def windows_playwright_asyncio_guard():
    """
    Playwright's sync driver launches the browser via asyncio subprocesses.

    On Windows, Streamlit (and some other hosts) may install an asyncio policy whose
    default loop cannot create subprocess transports, which raises NotImplementedError.
    Proactor is required for ``create_subprocess_exec`` to work.
    """
    if sys.platform != "win32":
        yield
        return
    old = asyncio.get_event_loop_policy()
    if isinstance(old, asyncio.WindowsProactorEventLoopPolicy):
        yield
        return
    try:
        asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())
        yield
    finally:
        asyncio.set_event_loop_policy(old)


class EngineSkipped(Exception):
    """Raised when ``leadgen_skip_engine.txt`` requests skipping the active engine."""


class CollectionStopped(Exception):
    """Raised when ``leadgen_stop_collection.txt`` requests stopping collection."""


def _set_active_pw_engine(engine: str) -> None:
    global _active_pw_engine
    _active_pw_engine = (engine or "").strip().lower()


def _raise_if_skip() -> None:
    if not _active_pw_engine:
        return
    if consume_skip_engine_request_if_matches(_active_pw_engine):
        raise EngineSkipped()


def _raise_if_interrupts() -> None:
    if peek_stop_collection_requested():
        raise CollectionStopped()
    _raise_if_skip()


# During ``collect_domains_playwright`` only: append each new domain immediately (flush).
_domain_write_hook: Callable[[str], None] | None = None


def _maybe_persist_domain_live(host: str) -> None:
    global _domain_write_hook
    fn = _domain_write_hook
    if fn is not None:
        fn(host)


@contextmanager
def _domain_write_hook_scope(write_fn: Optional[Callable[[str], None]]):
    global _domain_write_hook
    prev = _domain_write_hook
    _domain_write_hook = write_fn
    try:
        yield
    finally:
        _domain_write_hook = prev

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


_STEALTH_INIT = """
(() => {
  try {
    Object.defineProperty(navigator, "webdriver", { get: () => undefined });
    Object.defineProperty(navigator, "languages", { get: () => ["en-US", "en"] });
    window.chrome = window.chrome || { runtime: {} };
  } catch (e) {}
})();
"""


def _is_bot_challenge(page: Page) -> bool:
    """
    Detect consent walls / CAPTCHA / hard bot blocks.

    Avoid matching normal pages that merely mention ``recaptcha`` in script tags
    (that caused false positives on Google SERPs).
    """
    url = (page.url or "").lower()
    if "google." in url and "/sorry/" in url:
        return True
    if "consent.google.com" in url or "consent.yahoo.com" in url:
        return True
    if "duckduckgo.com" in url and ("418" in url or "static-pages/418" in url):
        return True
    if "bing.com" in url and ("captcha" in url or "/challenge" in url):
        return True
    if "startpage.com" in url and "captcha" in url:
        return True
    try:
        body = page.content().lower()
    except Exception:
        body = ""
    if "our systems have detected unusual traffic" in body:
        return True
    if "detected unusual traffic from your computer network" in body:
        return True
    if "unusual traffic from your computer network" in body:
        return True
    if "before you continue to google" in body:
        return True
    if "verify you're a human" in body or "verify you are a human" in body:
        return True
    if "i'm not a robot" in body or "im not a robot" in body:
        return True
    if "are you a robot" in body:
        return True
    if "if this persists" in body and "duckduckgo" in body:
        return True
    return False


def _wait_until_challenge_cleared(page: Page, captcha_mode: str, captcha_wait_ms: int) -> None:
    """Block until the challenge/consent page is gone, or until timeout."""
    _raise_if_interrupts()
    if not _is_bot_challenge(page):
        return
    logger.warning("Bot check, consent, or CAPTCHA page detected (url=%s)", page.url)
    cap = (captcha_mode or "wait").lower()
    if cap == "stdin":
        print(
            "\n>>> Complete the check in the browser (CAPTCHA / consent). "
            "When the normal results page is visible, press Enter here to continue…\n",
            flush=True,
        )
        try:
            input()
        except EOFError:
            logger.warning("No stdin (EOF); using timed poll instead.")
            cap = "wait"
    deadline = time.monotonic() + max(1, int(captcha_wait_ms)) / 1000.0
    poll_ms = 2000
    while time.monotonic() < deadline:
        _raise_if_interrupts()
        try:
            page.wait_for_timeout(poll_ms)
        except Exception:
            break
        if not _is_bot_challenge(page):
            logger.info("Challenge cleared; continuing.")
            return
    logger.warning(
        "Timed out waiting for challenge to clear after %s ms (url=%s)",
        captcha_wait_ms,
        page.url,
    )


def _maybe_screenshot_serp(page: Page, engine: str, serp_idx: int, query: str) -> None:
    """Optional full-page PNG for debugging (``LEADGEN_SERP_SCREENSHOT=1``). Domains still come from links."""
    if os.environ.get("LEADGEN_SERP_SCREENSHOT", "").strip().lower() not in ("1", "true", "yes"):
        return
    try:
        d = logs_dir() / "serp_screenshots"
        d.mkdir(parents=True, exist_ok=True)
        safe = re.sub(r"[^\w\-]+", "_", query).strip("_")[:45] or "q"
        path = d / f"{engine}_p{serp_idx}_{safe}.png"
        page.screenshot(path=str(path), full_page=True)
        logger.info("SERP screenshot: %s", path)
    except Exception as e:
        logger.debug("SERP screenshot failed: %s", e)


def _dismiss_common_banners(page: Page) -> None:
    """Light-touch cookie / notice dismiss (best-effort)."""
    for sel in (
        "#bnp_btn_accept",
        "#L2AgLb",
        'button:has-text("Accept all")',
        'button:has-text("Accept")',
        'button:has-text("Got it")',
        'button[aria-label="Accept all"]',
    ):
        try:
            loc = page.locator(sel).first
            if loc.count() > 0 and loc.is_visible():
                loc.click(timeout=2500)
                page.wait_for_timeout(400)
        except Exception:
            continue


def _bing_search_from_homepage(page: Page, query: str) -> None:
    page.goto("https://www.bing.com/", wait_until="domcontentloaded", timeout=60_000)
    _dismiss_common_banners(page)
    box = page.locator("#sb_form_q, form#sb_form textarea[name='q'], textarea#sb_form_q").first
    box.wait_for(state="visible", timeout=25_000)
    box.click()
    box.fill("")
    box.fill(query)
    box.press("Enter")
    page.wait_for_load_state("domcontentloaded", timeout=60_000)


def _google_search_from_homepage(page: Page, query: str) -> None:
    page.goto("https://www.google.com/", wait_until="domcontentloaded", timeout=60_000)
    _jitter(0.35, 0.9)
    _dismiss_common_banners(page)
    box = page.locator('textarea[name="q"], input[name="q"]').first
    box.wait_for(state="visible", timeout=25_000)
    box.fill(query)
    box.press("Enter")
    page.wait_for_load_state("domcontentloaded", timeout=60_000)


def _yahoo_search_from_homepage(page: Page, query: str) -> None:
    page.goto("https://search.yahoo.com/", wait_until="domcontentloaded", timeout=60_000)
    _dismiss_common_banners(page)
    box = page.locator('input[name="p"], input#yschsp, header input[type="search"]').first
    box.wait_for(state="visible", timeout=25_000)
    box.fill(query)
    box.press("Enter")
    page.wait_for_load_state("domcontentloaded", timeout=60_000)


def _ddg_search_from_homepage(page: Page, query: str) -> None:
    page.goto("https://duckduckgo.com/", wait_until="domcontentloaded", timeout=60_000)
    page.wait_for_timeout(600)
    _dismiss_common_banners(page)
    box = page.locator('input[name="q"], #searchbox_input, [data-testid="searchbox_input"]').first
    box.wait_for(state="visible", timeout=25_000)
    box.fill(query)
    box.press("Enter")
    page.wait_for_load_state("domcontentloaded", timeout=60_000)
    page.wait_for_timeout(1200)


def _ddg_blocked_or_error(page: Page) -> bool:
    u = (page.url or "").lower()
    if "418" in u or "static-pages/418" in u:
        return True
    try:
        html = page.content().lower()
    except Exception:
        return False
    if "if this persists" in html and "email us" in html:
        return True
    if "error getting results" in html:
        return True
    return False


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
    if "r.search.yahoo.com" in abs_url.lower():
        abs_url = resolve_yahoo_result_href(abs_url) or abs_url

    netloc = urlparse(abs_url).netloc.lower()
    low = abs_url.lower()
    if "bing.com" in netloc:
        decoded = decode_bing_tracking_url(abs_url)
        if decoded:
            abs_url = decoded
        elif "aclick" in low or "/ck/a" in low:
            # Rare short href without ``u=`` — follow interstitial then decode.
            bounced = _resolve_href_with_browser(page, abs_url)
            abs_url = decode_bing_tracking_url(bounced) or bounced

    target = decode_bing_tracking_url(abs_url) or abs_url
    low = target.lower()
    if "bing.com" in urlparse(target).netloc.lower() and ("aclick" in low or "/ck/a" in low):
        bounced = _resolve_href_with_browser(page, target)
        target = decode_bing_tracking_url(bounced) or bounced
    if target in seen_urls:
        return
    seen_urls.add(target)
    host = clean_domain(target)
    if host and not should_drop_collected_host(host):
        domains.add(host)
        _maybe_persist_domain_live(host)


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
    if host and not should_drop_collected_host(host):
        domains.add(host)
        _maybe_persist_domain_live(host)


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
    _raise_if_interrupts()
    _bing_search_from_homepage(page, query)
    _wait_until_challenge_cleared(page, captcha_mode, captcha_wait_ms)

    serp_done = 0
    while (len(domains) - start) < per_engine_cap and serp_done < max_serp_pages:
        _raise_if_interrupts()
        if max_total is not None and len(domains) >= max_total:
            break
        _wait_until_challenge_cleared(page, captcha_mode, captcha_wait_ms)
        page.wait_for_timeout(800)
        _maybe_screenshot_serp(page, "bing", serp_done, query)
        _scroll_results(page)

        for a in page.locator("li.b_algo h2 a, li.b_algo .b_title_link").all():
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


def _duckduckgo_harvest_main_js(page: Page, domains: set[str], seen_urls: set[str], start: int, per_engine_cap: int, max_total: int | None) -> int:
    """Harvest from duckduckgo.com JS SERP; returns count of domains added this call."""
    before = len(domains)
    seen_hrefs: set[str] = set()
    main_selectors = (
        "a[data-testid='result-title-a']",
        "article[data-testid='result'] h2 a",
        "li[data-layout='organic'] h2 a",
        "section[data-area='main'] ol li h2 a",
        "div[data-testid='result'] a",
    )
    for sel in main_selectors:
        if (len(domains) - start) >= per_engine_cap:
            break
        for a in page.locator(sel).all():
            if (len(domains) - start) >= per_engine_cap:
                break
            if max_total is not None and len(domains) >= max_total:
                break
            try:
                href = a.get_attribute("href")
                if not href or href in seen_hrefs:
                    continue
                seen_hrefs.add(href)
                _harvest_ddg_href(page, href, domains, seen_urls)
            except Exception:
                continue
    return len(domains) - before


def _duckduckgo_html_offset_pages(
    page: Page,
    query: str,
    domains: set[str],
    seen_urls: set[str],
    start: int,
    per_engine_cap: int,
    max_total: int | None,
    captcha_mode: str,
    captcha_wait_ms: int,
    max_serp_pages: int,
) -> None:
    """Static HTML SERP fallback (``html.duckduckgo.com``)."""
    offset = 0
    serp_done = 0
    ddg_link_selectors = (
        "a.result__a",
        ".result__a",
        ".results a.result__a",
        "table.links_main a.result__a",
        "td.result a",
        "a.result-link",
    )
    while (len(domains) - start) < per_engine_cap and serp_done < max_serp_pages:
        _raise_if_interrupts()
        if max_total is not None and len(domains) >= max_total:
            break
        url = f"https://html.duckduckgo.com/html/?q={quote_plus(query)}&s={offset}"
        page.goto(url, wait_until="domcontentloaded", timeout=60_000)
        _wait_until_challenge_cleared(page, captcha_mode, captcha_wait_ms)
        page.wait_for_timeout(700)
        if _ddg_blocked_or_error(page):
            logger.warning(
                "DuckDuckGo HTML block/error (URL=%s). Stopping DDG for this query.",
                page.url[:200],
            )
            break
        _maybe_screenshot_serp(page, "duckduckgo_html", serp_done, query)
        _scroll_results(page)

        before = len(domains)
        seen_hrefs: set[str] = set()
        for sel in ddg_link_selectors:
            if (len(domains) - start) >= per_engine_cap:
                break
            for a in page.locator(sel).all():
                if (len(domains) - start) >= per_engine_cap:
                    break
                if max_total is not None and len(domains) >= max_total:
                    break
                try:
                    href = a.get_attribute("href")
                    if not href or href in seen_hrefs:
                        continue
                    seen_hrefs.add(href)
                    _harvest_ddg_href(page, href, domains, seen_urls)
                except Exception:
                    continue

        serp_done += 1
        if len(domains) == before:
            if serp_done == 1:
                logger.warning("DuckDuckGo HTML: no harvestable links for query=%r", query)
            break
        if serp_done >= max_serp_pages:
            break
        offset += 10
        _jitter()


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
    """
    DuckDuckGo: type query on the homepage (less ``?q=``-direct), then harvest JS SERP.
    Falls back to ``html.duckduckgo.com`` when the main site is blocked or yields no links.
    """
    start = len(domains)
    _raise_if_interrupts()
    use_html_only = False
    try:
        _ddg_search_from_homepage(page, query)
    except Exception as e:
        logger.warning("DuckDuckGo homepage navigation failed (%s); using HTML fallback.", e)
        use_html_only = True

    if not use_html_only:
        _wait_until_challenge_cleared(page, captcha_mode, captcha_wait_ms)
        if _ddg_blocked_or_error(page):
            logger.warning("DuckDuckGo homepage blocked; using HTML fallback.")
            use_html_only = True

    if use_html_only:
        _duckduckgo_html_offset_pages(
            page,
            query,
            domains,
            seen_urls,
            start,
            per_engine_cap,
            max_total,
            captcha_mode,
            captcha_wait_ms,
            max_serp_pages,
        )
        return

    serp_done = 0
    while (len(domains) - start) < per_engine_cap and serp_done < max_serp_pages:
        _raise_if_interrupts()
        if max_total is not None and len(domains) >= max_total:
            break
        _wait_until_challenge_cleared(page, captcha_mode, captcha_wait_ms)
        _maybe_screenshot_serp(page, "duckduckgo", serp_done, query)
        _scroll_results(page)
        added = _duckduckgo_harvest_main_js(page, domains, seen_urls, start, per_engine_cap, max_total)
        if added == 0 and serp_done == 0:
            logger.warning("DuckDuckGo JS SERP returned no links; trying HTML fallback.")
            _duckduckgo_html_offset_pages(
                page,
                query,
                domains,
                seen_urls,
                start,
                per_engine_cap,
                max_total,
                captcha_mode,
                captcha_wait_ms,
                max_serp_pages,
            )
            break

        serp_done += 1
        if (len(domains) - start) >= per_engine_cap or serp_done >= max_serp_pages:
            break
        nxt = page.locator(
            "button[name='next'], a[data-testid='pagination-next'], "
            "nav[aria-label='pagination'] a:last-of-type, .btn--alt"
        ).first
        try:
            if nxt.count() == 0 or not nxt.is_visible():
                break
            nxt.click()
            page.wait_for_load_state("domcontentloaded", timeout=45_000)
            page.wait_for_timeout(900)
            _jitter()
        except Exception:
            break


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
    _raise_if_interrupts()
    _yahoo_search_from_homepage(page, query)
    _wait_until_challenge_cleared(page, captcha_mode, captcha_wait_ms)

    serp_done = 0
    while (len(domains) - start) < per_engine_cap and serp_done < max_serp_pages:
        _raise_if_interrupts()
        if max_total is not None and len(domains) >= max_total:
            break
        _wait_until_challenge_cleared(page, captcha_mode, captcha_wait_ms)
        _maybe_screenshot_serp(page, "yahoo", serp_done, query)
        _scroll_results(page)
        for sel in ("div.algo h3 a", "h3.title a", "a[data-mysyg-url]", "div.algo a"):
            for a in page.locator(sel).all():
                if (len(domains) - start) >= per_engine_cap:
                    break
                if max_total is not None and len(domains) >= max_total:
                    break
                try:
                    href = a.get_attribute("href")
                    if not href or href.startswith("/"):
                        continue
                    low = href.lower()
                    if "yahoo.com" in low and "r.search.yahoo.com" not in low:
                        continue
                    if "images.search.yahoo.com" in low or "video.search.yahoo.com" in low:
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
    _raise_if_interrupts()
    # Homepage + search box (avoid direct ``/search?q=...&num=100`` which triggers bot checks).
    _google_search_from_homepage(page, query)
    _wait_until_challenge_cleared(page, captcha_mode, max(captcha_wait_ms, 900_000))

    serp_done = 0
    while (len(domains) - start) < per_engine_cap and serp_done < max_serp_pages:
        _raise_if_interrupts()
        if max_total is not None and len(domains) >= max_total:
            break
        _wait_until_challenge_cleared(page, captcha_mode, max(captcha_wait_ms, 900_000))
        _maybe_screenshot_serp(page, "google", serp_done, query)
        _scroll_results(page)
        for a in page.locator('div#search a[href^="http"], div#search a[href^="/url?q="]').all():
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
                if not href.startswith("http"):
                    continue
                if "google.com" in href or "googleusercontent.com" in href:
                    continue
                _harvest_href(page, href, domains, seen_urls)
            except Exception:
                continue
        serp_done += 1
        if (len(domains) - start) >= per_engine_cap or serp_done >= max_serp_pages:
            break
        nxt = page.locator("a#pnnext, td.b a#pnnext, a[aria-label='Next page']")
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


def _launch_context(
    p,
    *,
    headless: bool,
    log: Callable[[str], None],
):
    """
    Return ``(browser, context)`` where ``browser`` may be ``None`` if a persistent
    context was used (close context only).

    Environment:

    - ``LEADGEN_PW_USER_DATA_DIR``: if set, use a persistent Chromium profile (helps
      Google sessions / consent). Directory is created if missing.
    - ``LEADGEN_PW_CHANNEL``: optional Chromium channel name, e.g. ``chrome`` or ``msedge``.
    """
    launch_args = [
        "--disable-blink-features=AutomationControlled",
        "--disable-dev-shm-usage",
        "--no-first-run",
        "--disable-popup-blocking",
    ]
    ctx_opts = dict(
        user_agent=random.choice(USER_AGENTS),
        viewport={"width": 1366, "height": 768},
        locale="en-US",
        extra_http_headers={"Accept-Language": "en-US,en;q=0.9"},
    )
    user_data = os.environ.get("LEADGEN_PW_USER_DATA_DIR", "").strip()
    channel_env = os.environ.get("LEADGEN_PW_CHANNEL", "").strip() or None

    if user_data:
        ud = Path(user_data).expanduser().resolve()
        ud.mkdir(parents=True, exist_ok=True)
        log(f"Using persistent browser profile: {ud}")
        kw = dict(
            headless=headless,
            user_data_dir=str(ud),
            args=launch_args,
            **ctx_opts,
        )
        if channel_env:
            kw["channel"] = channel_env
        try:
            context = p.chromium.launch_persistent_context(**kw)
        except Exception as e:
            logger.warning("Persistent launch failed (%s); retrying without channel.", e)
            kw.pop("channel", None)
            context = p.chromium.launch_persistent_context(**kw)
        context.add_init_script(_STEALTH_INIT)
        return None, context

    channels: list[str | None] = []
    if channel_env:
        channels.append(channel_env)
    for ch in ("chrome", "msedge", None):
        if ch not in channels:
            channels.append(ch)

    browser = None
    last_err: Exception | None = None
    for ch in channels:
        try:
            lkw: dict = {"headless": headless, "args": launch_args}
            if ch:
                lkw["channel"] = ch
            browser = p.chromium.launch(**lkw)
            log(f"Chromium launcher: {ch or 'bundled'}")
            break
        except Exception as e:
            last_err = e
            logger.debug("chromium.launch channel=%r failed: %s", ch, e)
    if browser is None:
        raise RuntimeError(f"Could not launch Chromium (last error: {last_err})")
    context = browser.new_context(**ctx_opts)
    context.add_init_script(_STEALTH_INIT)
    return browser, context


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
    b2b_enrich: bool = False,
    unlimited: bool = False,
) -> dict[str, int]:
    """
    For each query, run each **enabled** engine in ``engine_order``, collecting up to
    ``per_engine_max`` **new** domains from that engine (then moving on so all engines run).

    ``max_results`` caps **total** unique domains unless ``unlimited`` is true; when reached,
    remaining engines/queries stop.

    Domains are appended to the output file **immediately** (with flush) as they are harvested.

    ``pages_per_engine``: optional map e.g. ``{"bing": 1, "google": 2, "duckduckgo": 3}``.
    Omitted engines use ``DEFAULT_SERP_PAGES_PER_ENGINE`` (10). Each value is max **SERP**
    pages (first results page counts as 1).

    Cooperative stop: create ``leadgen_stop_collection.txt`` in the data directory (see
    ``stop_collection_request``) or use the Streamlit Stop button; polling uses the same
    checkpoints as skip-engine requests.
    """
    ensure_leadgen_file_log()
    clear_stop_collection_request()
    serp_limits = merge_serp_pages_per_engine(pages_per_engine)
    out = Path(output_path) if output_path else domains_path()
    out.parent.mkdir(parents=True, exist_ok=True)

    if queries is None:
        queries = build_playwright_queries(
            keyword or "business",
            country or "",
            states or "",
            b2b_enrich=b2b_enrich,
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
    pem_budget = max(1, int(max_results)) if not unlimited else max(1, int(max_results) or 1_000_000)
    if per_engine_max is None:
        per_engine_max = max(15, min(60, max(20, pem_budget // max(1, n_en))))

    existing: set[str] = set()
    if append and out.exists():
        for line in out.read_text(encoding="utf-8", errors="ignore").splitlines():
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            u = line.replace("http://", "").replace("https://", "").split("/")[0]
            if u:
                existing.add(u.lower())

    if not append:
        try:
            out.write_text("", encoding="utf-8")
        except OSError as e:
            logger.error("Could not truncate %s: %s", out, e)
            return {"new_domains": 0, "total_collected": 0, "output": str(out), "queries_run": 0, "engines": []}
        existing.clear()

    domains: set[str] = set()
    seen_urls: set[str] = set()
    if unlimited:
        max_total: int | None = None
    else:
        max_total = max(1, min(int(max_results), 2_000_000))

    new_writes = 0
    domain_fh = None
    try:
        domain_fh = open(out, "a", encoding="utf-8")
    except OSError as e:
        logger.error("Could not open %s for append: %s", out, e)
        return {"new_domains": 0, "total_collected": 0, "output": str(out), "queries_run": len(queries), "engines": enabled}

    def log(msg: str) -> None:
        logger.info(msg)
        if log_callback:
            log_callback(msg)

    def _append_domain_line(host: str) -> None:
        nonlocal new_writes
        key = (host or "").strip().lower()
        if not key or key in existing:
            return
        line = f"https://{host}\n"
        try:
            domain_fh.write(line)
            domain_fh.flush()
        except OSError as e:
            logger.error("Could not append domain %r to %s: %s", host, out, e)
            return
        existing.add(key)
        new_writes += 1

    cap_mode = (captcha_mode or "stdin").lower()
    if cap_mode not in ("stdin", "wait"):
        cap_mode = "wait"

    lim_str = ", ".join(f"{e}={serp_limits[e]}" for e in enabled)
    cap_desc = "unlimited (until stop file or manual stop)" if max_total is None else str(max_total)
    log(
        f"Engines: {', '.join(enabled)} | {len(queries)} query/queries | "
        f"up to {per_engine_max} new domains per engine per query | total cap {cap_desc} | "
        f"SERP pages: {lim_str}"
    )

    try:
        with _domain_write_hook_scope(_append_domain_line):
            with windows_playwright_asyncio_guard():
                with sync_playwright() as p:
                    clear_skip_engine_request()
                    browser, context = _launch_context(p, headless=headless, log=log)
                    page = context.new_page()
                    try:
                        stop_all = False
                        pass_num = 0
                        while True:
                            for qi, query in enumerate(queries):
                                if stop_all or (max_total is not None and len(domains) >= max_total):
                                    stop_all = True
                                    break
                                log(f"=== Query {qi + 1}/{len(queries)} (pass {pass_num + 1}): {query!r} ===")
                                for eng in enabled:
                                    if max_total is not None and len(domains) >= max_total:
                                        stop_all = True
                                        break
                                    _set_active_pw_engine(eng)
                                    if consume_skip_engine_request_if_matches(eng):
                                        log(f"--- {eng.upper()} skipped (request on disk before start) ---")
                                        continue
                                    before = len(domains)
                                    npg = serp_limits.get(eng, DEFAULT_SERP_PAGES_PER_ENGINE.get(eng, 10))
                                    log(
                                        f"--- {eng.upper()} (target +{per_engine_max} domains; "
                                        f"max {npg} SERP page(s); total cap {cap_desc}) ---"
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
                                    except EngineSkipped:
                                        log(f"--- {eng.upper()} skipped (user request) ---")
                                    except CollectionStopped:
                                        log("--- STOP: collection stop requested ---")
                                        stop_all = True
                                        break
                                    except Exception as e:
                                        logger.exception("Engine %s failed for %r: %s", eng, query, e)
                                        log(f"ERROR {eng}: {e}")
                                    added = len(domains) - before
                                    log(f"--- {eng.upper()} done: +{added} domains (session total {len(domains)}) ---")
                                    _jitter(1.0, 2.5)
                                if stop_all:
                                    break
                            if stop_all:
                                break
                            if max_total is not None and len(domains) >= max_total:
                                break
                            if not unlimited:
                                break
                            pass_num += 1
                            log(f"=== Unlimited mode: restarting query list (pass {pass_num + 1}) ===")
                    finally:
                        _set_active_pw_engine("")
                        try:
                            context.close()
                        finally:
                            if browser is not None:
                                browser.close()
    finally:
        if domain_fh is not None:
            try:
                domain_fh.close()
            except OSError:
                pass

    log(
        f"Saved {new_writes} new domain line(s) to {out} "
        f"(unique this run in memory: {len(domains)}; per-engine cap was {per_engine_max})"
    )
    return {
        "new_domains": new_writes,
        "total_collected": len(domains),
        "output": str(out),
        "queries_run": len(queries),
        "engines": enabled,
    }


__all__ = [
    "CollectionStopped",
    "EngineSkipped",
    "windows_playwright_asyncio_guard",
    "collect_domains_playwright",
    "ENGINE_ORDER_DEFAULT",
    "DEFAULT_SERP_PAGES_PER_ENGINE",
    "merge_serp_pages_per_engine",
]
