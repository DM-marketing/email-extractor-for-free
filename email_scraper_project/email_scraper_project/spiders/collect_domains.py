"""
Collect business domains from multiple search engines (DuckDuckGo, Bing, Yahoo, Startpage).

CLI or programmatic API for keyword + country driven queries, with optional free proxies,
retries, pagination, and structured logging.
"""

from __future__ import annotations

import argparse
import json
import logging
import random
import time
from pathlib import Path
from typing import Callable, Optional

from email_scraper_project.config import domains_path, logs_dir
from email_scraper_project.domain_cleaner import clean_domain
from email_scraper_project.lead_qualifier import should_drop_collected_host
from email_scraper_project.logging_config import log_event, setup_logging
from email_scraper_project.proxy_manager import ProxyManager
from email_scraper_project.search_engine import SearchClient
from email_scraper_project.search_engine.client import build_search_queries

LogFn = Optional[Callable[[str], None]]


def _console_log(logger: logging.Logger, log_fn: LogFn, message: str) -> None:
    logger.info(message)
    if log_fn:
        log_fn(message)


def run_domain_collection(
    keywords: str,
    country: str,
    state: str = "",
    city: str = "",
    industry: str = "",
    max_pages_per_engine: int = 3,
    engines_order: tuple[str, ...] | None = None,
    use_free_proxies: bool = False,
    output_path: Optional[Path] = None,
    json_export: Optional[Path] = None,
    append: bool = True,
    log_callback: LogFn = None,
    delay_range: tuple[float, float] = (2.0, 6.0),
    b2b_queries: bool = False,
) -> dict[str, int]:
    """
    Run multi-engine search for all generated queries; write unique domains to domains.txt.

    Returns stats: queries_run, domains_new, domains_total, engines_used.
    """
    json_log = logs_dir() / "domain_collection.jsonl"
    logger = setup_logging("leadgen.collect", logging.INFO, json_file=str(json_log))

    out = Path(output_path) if output_path else domains_path()
    out.parent.mkdir(parents=True, exist_ok=True)

    all_domains: set[str] = set()
    if append and out.exists():
        with open(out, "r", encoding="utf-8", errors="ignore") as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith("#"):
                    continue
                u = line.replace("http://", "").replace("https://", "").split("/")[0]
                if u:
                    all_domains.add(u.lower())

    queries = build_search_queries(
        keywords, country, state, city, industry, b2b_enrich=b2b_queries
    )
    if not queries:
        queries = [f"business in {country or 'USA'}"]

    engines_order = engines_order or SearchClient.ENGINES
    proxy_mgr: Optional[ProxyManager] = ProxyManager() if use_free_proxies else None
    json_out = Path(json_export) if json_export else None

    new_total = 0
    first_file_write = True
    for qi, q in enumerate(queries):
        _console_log(logger, log_callback, f"Query {qi + 1}/{len(queries)}: {q}")
        log_event(logger, "query_start", query=q, index=qi + 1, total=len(queries))

        proxies_dict = None
        p = None
        if proxy_mgr:
            p = proxy_mgr.pick()
            proxies_dict = proxy_mgr.requests_proxies_dict(p)

        client = SearchClient(delay_range=delay_range, proxies_dict=proxies_dict)
        try:
            per_engine = client.search_with_failover(
                q, max_pages_per_engine=max_pages_per_engine, engines_order=engines_order
            )
        except Exception as e:
            log_event(logger, "query_failed", query=q, error=str(e))
            if proxy_mgr and p:
                proxy_mgr.mark_bad(p)
            _console_log(logger, log_callback, f"Query failed (will continue): {e}")
            time.sleep(random.uniform(*delay_range))
            continue

        batch: set[str] = set()
        for eng, found in per_engine.items():
            for link_host in found:
                d = clean_domain(f"https://{link_host}") or link_host
                if d and not should_drop_collected_host(d) and d not in all_domains:
                    batch.add(d)
            log_event(
                logger,
                "engine_result",
                query=q,
                engine=eng,
                domains=len(found),
            )

        if batch:
            if append:
                file_mode = "a"
            else:
                file_mode = "w" if first_file_write else "a"
            first_file_write = False
            with open(out, file_mode, encoding="utf-8") as f:
                for d in sorted(batch):
                    f.write(f"http://{d}\n")
            all_domains.update(batch)
            new_total += len(batch)
            log_event(
                logger,
                "domains_saved",
                query=q,
                new=len(batch),
                cumulative=len(all_domains),
            )
            _console_log(
                logger,
                log_callback,
                f"+{len(batch)} domains | total unique: {len(all_domains)}",
            )

        if json_out:
            json_out.parent.mkdir(parents=True, exist_ok=True)
            snap = {
                "query": q,
                "per_engine": {k: len(v) for k, v in per_engine.items()},
                "new_batch": len(batch),
            }
            with open(json_out, "a", encoding="utf-8") as jf:
                jf.write(json.dumps(snap, ensure_ascii=False) + "\n")

        time.sleep(random.uniform(*delay_range))

    stats = {
        "queries_run": len(queries),
        "domains_new": new_total,
        "domains_total": len(all_domains),
        "engines_configured": len(engines_order),
    }
    log_event(logger, "collection_done", **stats)
    _console_log(logger, log_callback, f"DONE {stats}")
    logger.info("Wrote domains to %s", out)
    return stats


def main() -> None:
    parser = argparse.ArgumentParser(description="Collect domains from search engines")
    parser.add_argument("--keywords", default="dentist,lawyer", help="Comma-separated")
    parser.add_argument("--country", default="USA")
    parser.add_argument("--state", default="")
    parser.add_argument("--city", default="")
    parser.add_argument("--industry", default="")
    parser.add_argument("--pages", type=int, default=3, help="Max pages per engine per query")
    parser.add_argument("--no-append", action="store_true", help="Do not merge existing domains.txt")
    parser.add_argument("--proxies", action="store_true", help="Use free HTTP proxies (unreliable)")
    parser.add_argument(
        "--b2b-queries",
        action="store_true",
        help="Add B2B-style queries (e.g. 'keyword company Texas USA')",
    )
    parser.add_argument(
        "--output",
        type=str,
        default="",
        help="domains.txt path (default: project root domains.txt)",
    )
    parser.add_argument("--json-log", type=str, default="", help="Append JSON lines summary per query")
    args = parser.parse_args()

    out = Path(args.output) if args.output else None
    jpath = Path(args.json_log) if args.json_log else None

    run_domain_collection(
        keywords=args.keywords,
        country=args.country,
        state=args.state,
        city=args.city,
        industry=args.industry,
        max_pages_per_engine=args.pages,
        use_free_proxies=args.proxies,
        output_path=out,
        json_export=jpath,
        append=not args.no_append,
        b2b_queries=args.b2b_queries,
    )


if __name__ == "__main__":
    main()
