"""
CLI for Playwright-based domain collection + threaded email crawl to emails.txt / logs.txt.

Examples:
  python -m email_scraper_project.playwright_cli collect --keyword "motor rewinding" --country USA --results 80
  python -m email_scraper_project.playwright_cli collect --queries-file queries.txt --per-engine-max 30 --results 200
  python -m email_scraper_project.playwright_cli crawl --workers 8

While ``collect`` runs, create ``leadgen_skip_engine.txt`` in the data folder (see
``email_scraper_project.config.data_dir``) containing ``bing``, ``duckduckgo``, ``yahoo``,
``google``, or ``all`` to skip the next matching engine (same as the Streamlit skip buttons).

Create ``leadgen_stop_collection.txt`` in the same folder (or use Streamlit **Stop collect** /
**Stop Collection**) to request a graceful stop after the current browser step.

Env: ``LEADGEN_SERP_SCREENSHOT=1`` saves full-page SERP PNGs under ``logs/serp_screenshots/``.
"""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

from email_scraper_project.browser_search.playwright_collector import (
    ENGINE_ORDER_DEFAULT,
    collect_domains_playwright,
)
from email_scraper_project.browser_search.stop_collection_request import clear_stop_collection_request
from email_scraper_project.email_txt_crawler.threaded_crawler import crawl_domains_to_emails_txt
from email_scraper_project.logging_config import ensure_leadgen_file_log


def _bool_env(name: str, default: bool = False) -> bool:
    v = os.environ.get(name, "").lower()
    if v in ("1", "true", "yes"):
        return True
    if v in ("0", "false", "no"):
        return False
    return default


def _parse_engine_order(s: str) -> tuple[str, ...]:
    aliases = {"ddg": "duckduckgo"}
    allowed = frozenset(("bing", "duckduckgo", "yahoo", "google"))
    out: list[str] = []
    for part in s.split(","):
        p = aliases.get(part.strip().lower(), part.strip().lower())
        if p in allowed and p not in out:
            out.append(p)
    return tuple(out) if out else ENGINE_ORDER_DEFAULT


def _pages_from_args(args: argparse.Namespace) -> dict[str, int] | None:
    """Non-zero CLI values override defaults for that engine only."""
    d: dict[str, int] = {}
    for eng, attr in (
        ("bing", "bing_pages"),
        ("duckduckgo", "ddg_pages"),
        ("yahoo", "yahoo_pages"),
        ("google", "google_pages"),
    ):
        v = int(getattr(args, attr, 0) or 0)
        if v > 0:
            d[eng] = v
    return d if d else None


def _collect_kwargs(args: argparse.Namespace, headless: bool) -> dict:
    queries = None
    if getattr(args, "queries_file", None) and str(args.queries_file).strip():
        p = Path(args.queries_file)
        if not p.is_file():
            raise SystemExit(f"queries file not found: {p}")
        queries = [
            ln.strip()
            for ln in p.read_text(encoding="utf-8", errors="ignore").splitlines()
            if ln.strip() and not ln.strip().startswith("#")
        ]
        if not queries:
            raise SystemExit("queries file is empty")

    pem = getattr(args, "per_engine_max", 0) or None
    order = _parse_engine_order(getattr(args, "engine_order", "") or "")
    unlimited = bool(getattr(args, "unlimited", False))
    raw_results = max(10, int(getattr(args, "results", 100)))
    max_results = raw_results if unlimited else max(10, min(raw_results, 2_000_000))

    return dict(
        keyword=getattr(args, "keyword", "") or "",
        country=getattr(args, "country", "") or "",
        states=getattr(args, "states", "") or "",
        queries=queries,
        max_results=max_results,
        per_engine_max=pem,
        use_bing=not args.no_bing,
        use_duckduckgo=not getattr(args, "no_ddg", False),
        use_yahoo=not args.no_yahoo,
        use_google=getattr(args, "google", False),
        engine_order=order,
        pages_per_engine=_pages_from_args(args),
        headless=headless,
        append=not args.no_append,
        captcha_mode=args.captcha_mode,
        captcha_wait_ms=args.captcha_wait_ms,
        b2b_enrich=getattr(args, "b2b_queries", False),
        unlimited=unlimited,
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="Playwright lead pipeline")
    sub = parser.add_subparsers(dest="cmd", required=True)

    p_col = sub.add_parser("collect", help="Browser search to domains.txt")
    p_col.add_argument("--keyword", default="", help="Single keyword (use with --country / --states)")
    p_col.add_argument("--country", default="USA")
    p_col.add_argument("--states", default="", help="Comma/line-separated states (optional)")
    p_col.add_argument(
        "--queries-file",
        default="",
        help="One search query per line (overrides --keyword / --country / --states)",
    )
    p_col.add_argument(
        "--results",
        type=int,
        default=100,
        help="Max total unique domains (stop when reached); with --unlimited used mainly for per-engine sizing",
    )
    p_col.add_argument(
        "--unlimited",
        action="store_true",
        help="Ignore total domain cap; repeat queries until leadgen_stop_collection.txt or Ctrl+C",
    )
    p_col.add_argument(
        "--per-engine-max",
        type=int,
        default=0,
        help="Max NEW domains per engine per query (0 = auto from --results / num engines)",
    )
    p_col.add_argument(
        "--engine-order",
        default=",".join(ENGINE_ORDER_DEFAULT),
        help="Comma list: bing,duckduckgo,yahoo,google (alias: ddg)",
    )
    p_col.add_argument("--no-bing", action="store_true")
    p_col.add_argument("--no-ddg", action="store_true", help="Disable DuckDuckGo HTML search")
    p_col.add_argument("--no-yahoo", action="store_true")
    p_col.add_argument("--google", action="store_true", help="Enable Google (expect CAPTCHA)")
    p_col.add_argument(
        "--bing-pages",
        type=int,
        default=0,
        metavar="N",
        help="Max Bing SERP pages to open (1=first page only; 0=default 10)",
    )
    p_col.add_argument(
        "--ddg-pages",
        type=int,
        default=0,
        metavar="N",
        help="Max DuckDuckGo HTML SERP pages (0=default 10)",
    )
    p_col.add_argument(
        "--yahoo-pages",
        type=int,
        default=0,
        metavar="N",
        help="Max Yahoo SERP pages (0=default 10)",
    )
    p_col.add_argument(
        "--google-pages",
        type=int,
        default=0,
        metavar="N",
        help="Max Google SERP pages (0=default 10)",
    )
    p_col.add_argument("--no-append", action="store_true")
    p_col.add_argument(
        "--captcha-mode",
        choices=("stdin", "wait"),
        default=os.environ.get("LEADGEN_CAPTCHA_MODE", "stdin"),
        help="stdin=solve in browser then press Enter; wait=poll until cleared or --captcha-wait-ms",
    )
    p_col.add_argument("--captcha-wait-ms", type=int, default=300_000)
    p_col.add_argument(
        "--b2b-queries",
        action="store_true",
        help="Add B2B-style browser queries (company/services + region)",
    )

    p_cr = sub.add_parser("crawl", help="Threaded HTTP crawl to emails.txt")
    p_cr.add_argument("--workers", type=int, default=6)
    p_cr.add_argument("--no-append", action="store_true")
    p_cr.add_argument(
        "--fallback-headful",
        action="store_true",
        help="Use visible browser for HTTP-blocked domains (slower)",
    )

    p_all = sub.add_parser("all", help="collect then crawl")
    for p in (p_all,):
        p.add_argument("--keyword", default="")
        p.add_argument("--country", default="USA")
        p.add_argument("--states", default="")
        p.add_argument("--queries-file", default="")
        p.add_argument("--results", type=int, default=100)
        p.add_argument("--per-engine-max", type=int, default=0)
        p.add_argument("--engine-order", default=",".join(ENGINE_ORDER_DEFAULT))
        p.add_argument("--no-bing", action="store_true")
        p.add_argument("--no-ddg", action="store_true")
        p.add_argument("--no-yahoo", action="store_true")
        p.add_argument("--google", action="store_true")
        p.add_argument("--bing-pages", type=int, default=0, metavar="N")
        p.add_argument("--ddg-pages", type=int, default=0, metavar="N")
        p.add_argument("--yahoo-pages", type=int, default=0, metavar="N")
        p.add_argument("--google-pages", type=int, default=0, metavar="N")
        p.add_argument("--no-append-domains", action="store_true")
        p.add_argument("--no-append-emails", action="store_true")
        p.add_argument("--workers", type=int, default=6)
        p.add_argument(
            "--captcha-mode",
            choices=("stdin", "wait"),
            default=os.environ.get("LEADGEN_CAPTCHA_MODE", "stdin"),
        )
        p.add_argument("--captcha-wait-ms", type=int, default=300_000)
        p.add_argument("--fallback-headful", action="store_true")
        p.add_argument(
            "--b2b-queries",
            action="store_true",
            help="Add B2B-style browser queries (company/services + region)",
        )
        p.add_argument(
            "--unlimited",
            action="store_true",
            help="Ignore total domain cap during collect (then crawl)",
        )

    args = parser.parse_args()
    ensure_leadgen_file_log()
    headless = _bool_env("LEADGEN_PLAYWRIGHT_HEADLESS", False)

    if args.cmd == "collect":
        clear_stop_collection_request()
        kw = _collect_kwargs(args, headless)
        if not kw["queries"] and not (kw["keyword"] or kw["country"] or kw["states"]):
            kw["keyword"] = "business"
        collect_domains_playwright(**kw)
    elif args.cmd == "crawl":
        crawl_domains_to_emails_txt(
            max_workers=max(1, args.workers),
            headless_fallback=not args.fallback_headful,
            append=not args.no_append,
        )
    elif args.cmd == "all":
        clear_stop_collection_request()
        merged = argparse.Namespace(**vars(args))
        merged.no_append = getattr(args, "no_append_domains", False)
        kw = _collect_kwargs(merged, headless)
        if not kw["queries"] and not (kw["keyword"] or kw["country"] or kw["states"]):
            kw["keyword"] = "business"
        collect_domains_playwright(**kw)
        crawl_domains_to_emails_txt(
            max_workers=max(1, args.workers),
            headless_fallback=not args.fallback_headful,
            append=not args.no_append_emails,
        )
    else:
        parser.print_help()
        sys.exit(1)


if __name__ == "__main__":
    main()
