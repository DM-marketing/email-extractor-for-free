"""
Threaded HTTP crawl of domains (homepage + /contact + /about); Playwright fallback if blocked.
Writes emails.txt and logs to logs.txt.
"""

from __future__ import annotations

import logging
import random
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Callable, Optional
import requests
from bs4 import BeautifulSoup
from playwright.sync_api import sync_playwright

from email_scraper_project.config import domains_path, emails_txt_path
from email_scraper_project.logging_config import ensure_leadgen_file_log
from email_scraper_project.email_txt_crawler.extract import extract_emails_from_html, iter_mailto_hrefs

logger = logging.getLogger("leadgen.email_txt")

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.4 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64; rv:123.0) Gecko/20100101 Firefox/123.0",
]


def _domain_from_line(line: str) -> str | None:
    s = line.strip()
    if not s or s.startswith("#"):
        return None
    s = s.replace("http://", "").replace("https://", "").split("/")[0].strip()
    return s.lower() or None


def _candidate_urls(domain: str) -> list[str]:
    d = domain.strip().lower().lstrip(".")
    base = f"https://{d}".rstrip("/")
    paths = ["/", "/contact", "/contact-us", "/about", "/about-us"]
    urls: list[str] = []
    for p in paths:
        urls.append(base + "/" if p == "/" else base + p)
    return urls


def _session() -> requests.Session:
    s = requests.Session()
    s.trust_env = False
    s.headers.update({"Accept-Language": "en-US,en;q=0.9", "Accept-Encoding": "gzip, deflate"})
    return s


def _fetch_requests(
    url: str,
    session: requests.Session,
    timeout: float = 18.0,
) -> tuple[int | None, str]:
    try:
        session.headers["User-Agent"] = random.choice(USER_AGENTS)
        r = session.get(url, timeout=timeout, allow_redirects=True)
        if r.status_code in (403, 429, 503):
            return r.status_code, ""
        r.raise_for_status()
        return r.status_code, r.text or ""
    except Exception as e:
        logger.debug("fetch fail %s: %s", url, e)
        return None, ""


def _emails_from_page(html: str, page_url: str, domain: str) -> list[tuple[str, str, str]]:
    soup = BeautifulSoup(html, "html.parser")
    hrefs = [a.get("href") for a in soup.find_all("a", href=True)]
    raw = extract_emails_from_html(html)
    raw.extend(iter_mailto_hrefs(hrefs))
    seen: set[str] = set()
    out: list[tuple[str, str, str]] = []
    for e in raw:
        el = e.lower().strip()
        if el in seen:
            continue
        seen.add(el)
        out.append((el, domain, page_url))
    return out


def _playwright_fetch_urls(urls: list[str], headless: bool = True) -> list[tuple[str, str]]:
    """Return list of (url, html) for first successful loads."""
    results: list[tuple[str, str]] = []
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=headless, args=["--disable-blink-features=AutomationControlled"])
        ctx = browser.new_context(user_agent=random.choice(USER_AGENTS), viewport={"width": 1280, "height": 800})
        page = ctx.new_page()
        try:
            for u in urls:
                try:
                    page.goto(u, wait_until="domcontentloaded", timeout=25_000)
                    time.sleep(random.uniform(0.4, 1.1))
                    html = page.content()
                    results.append((u, html))
                except Exception as e:
                    logger.debug("playwright %s: %s", u, e)
        finally:
            ctx.close()
            browser.close()
    return results


def _dedupe_rows(rows: list[tuple[str, str, str]]) -> list[tuple[str, str, str]]:
    seen: set[str] = set()
    out: list[tuple[str, str, str]] = []
    for email, dom, src in rows:
        el = email.lower()
        if el in seen:
            continue
        seen.add(el)
        out.append((email, dom, src))
    return out


def _process_domain_http(domain: str) -> tuple[str, list[tuple[str, str, str]], bool]:
    """
    Returns (domain, rows, needs_playwright_fallback).
    """
    session = _session()
    rows: list[tuple[str, str, str]] = []
    saw_block = False
    any_html = False
    for url in _candidate_urls(domain):
        time.sleep(random.uniform(0.15, 0.55))
        status, html = _fetch_requests(url, session)
        if status in (403, 429):
            saw_block = True
        if html and len(html) > 120:
            any_html = True
            rows.extend(_emails_from_page(html, url, domain))
    rows = _dedupe_rows(rows)
    if rows:
        return domain, rows, False
    if saw_block or not any_html:
        return domain, [], True
    return domain, [], False


def crawl_domains_to_emails_txt(
    *,
    domains_file: Optional[Path] = None,
    output_txt: Optional[Path] = None,
    log_txt: Optional[Path] = None,
    max_workers: int = 6,
    headless_fallback: bool = True,
    append: bool = True,
    log_callback: Optional[Callable[[str], None]] = None,
) -> dict[str, int]:
    """
    Read domains.txt, crawl with thread pool, append unique emails to emails.txt.
    """
    dpath = Path(domains_file) if domains_file else domains_path()
    out = Path(output_txt) if output_txt else emails_txt_path()
    ensure_leadgen_file_log(Path(log_txt) if log_txt else None)

    if not dpath.is_file():
        logger.error("domains file missing: %s", dpath)
        return {"domains": 0, "emails_new": 0, "errors": 1}

    domains_list: list[str] = []
    for line in dpath.read_text(encoding="utf-8", errors="ignore").splitlines():
        d = _domain_from_line(line)
        if d:
            domains_list.append(d)

    seen_emails: set[str] = set()
    out.parent.mkdir(parents=True, exist_ok=True)
    if not append:
        out.write_text("# email\tdomain\tsource_url\n", encoding="utf-8")
    elif out.exists():
        for line in out.read_text(encoding="utf-8", errors="ignore").splitlines():
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            part = line.split("\t")[0].split(",")[0].strip().lower()
            if "@" in part:
                seen_emails.add(part)

    write_lock = threading.Lock()
    new_count = 0

    def append_rows(rows: list[tuple[str, str, str]]) -> None:
        nonlocal new_count
        if not rows:
            return
        with write_lock:
            file_has_body = out.exists() and out.stat().st_size > 0
            with open(out, "a", encoding="utf-8") as f:
                if not file_has_body:
                    f.write("# email\tdomain\tsource_url\n")
                for email, dom, src in rows:
                    el = email.lower()
                    if el in seen_emails:
                        continue
                    seen_emails.add(el)
                    f.write(f"{email}\t{dom}\t{src}\n")
                    new_count += 1

    fallback_domains: list[str] = []
    logger.info("Crawling %s domains (workers=%s)", len(domains_list), max_workers)

    def log(msg: str) -> None:
        logger.info(msg)
        if log_callback:
            log_callback(msg)

    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        futs = {ex.submit(_process_domain_http, d): d for d in domains_list}
        for fut in as_completed(futs):
            dom = futs[fut]
            try:
                dkey, rows, need_pw = fut.result()
                if need_pw:
                    fallback_domains.append(dkey)
                    log(f"HTTP blocked or empty → Playwright fallback queued: {dkey}")
                else:
                    append_rows(rows)
                    if rows:
                        log(f"{dkey}: +{len(rows)} raw hits (deduped globally)")
            except Exception as e:
                logger.exception("worker error %s: %s", dom, e)

    for dom in fallback_domains:
        log(f"Playwright fallback: {dom}")
        pairs = _playwright_fetch_urls(_candidate_urls(dom), headless=headless_fallback)
        batch: list[tuple[str, str, str]] = []
        for url, html in pairs:
            batch.extend(_emails_from_page(html, url, dom))
        append_rows(batch)
        time.sleep(random.uniform(1.0, 2.5))

    log(f"Done. New unique emails: {new_count} → {out}")
    return {"domains": len(domains_list), "emails_new": new_count, "fallback": len(fallback_domains)}
