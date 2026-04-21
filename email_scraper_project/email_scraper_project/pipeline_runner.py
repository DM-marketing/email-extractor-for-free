"""
One-shot orchestration: build queries → Playwright collect → email crawl → qualify.

Used by the Streamlit tab "Full Lead Pipeline"; reuses existing collectors unchanged.
"""

from __future__ import annotations

import logging
import os
from dataclasses import dataclass, field
from typing import Callable, Optional

from email_scraper_project.browser_search.playwright_collector import (
    ENGINE_ORDER_DEFAULT,
    collect_domains_playwright,
)
from email_scraper_project.browser_search.query_builder import build_playwright_queries
from email_scraper_project.browser_search.skip_engine_request import clear_skip_engine_request
from email_scraper_project.config import (
    domains_path,
    emails_txt_path,
    outreach_ready_csv_path,
    qualified_leads_csv_path,
)
from email_scraper_project.email_txt_crawler.threaded_crawler import crawl_domains_to_emails_txt
from email_scraper_project.lead_qualifier import (
    QualifiedLead,
    infer_industries_from_keywords_text,
    normalize_industry_selection,
    parse_emails_txt,
    qualify_email_rows,
    write_outreach_csv,
    write_qualified_csv,
)

logger = logging.getLogger("leadgen.pipeline")

ProgressFn = Optional[Callable[[str, Optional[float]], None]]


def count_nonempty_lines(path: os.PathLike[str]) -> int:
    p = os.fspath(path)
    try:
        with open(p, encoding="utf-8", errors="ignore") as f:
            return sum(1 for line in f if line.strip() and not line.lstrip().startswith("#"))
    except OSError:
        return 0


@dataclass
class FullPipelineConfig:
    keywords: str
    country: str
    state: str = ""
    selected_industries: tuple[str, ...] = ()
    strict_industry_filter: bool = False
    max_domains: int = 80
    per_engine_max: int = 35
    serp_bing: int = 5
    serp_ddg: int = 5
    serp_yahoo: int = 5
    serp_google: int = 5
    email_workers: int = 6
    use_bing: bool = True
    use_ddg: bool = True
    use_yahoo: bool = True
    use_google: bool = False
    b2b_query_expansion: bool = True
    append_domains: bool = False
    append_emails: bool = False
    headless_browser: bool = False
    captcha_mode: str = "wait"
    captcha_wait_ms: int = 600_000
    headless_playwright_email_fallback: bool = True
    use_openai_summaries: bool = False
    serp_screenshots: bool = False
    unlimited_domains: bool = False


@dataclass
class FullPipelineResult:
    queries: list[str] = field(default_factory=list)
    keyword_detected_industries: list[str] = field(default_factory=list)
    collect_stats: dict = field(default_factory=dict)
    crawl_stats: dict = field(default_factory=dict)
    qualified_leads: list[QualifiedLead] = field(default_factory=list)
    domains_line_count: int = 0
    emails_line_count: int = 0
    qualified_count: int = 0
    high_quality_count: int = 0
    errors: list[str] = field(default_factory=list)

    @property
    def ok(self) -> bool:
        return not self.errors


def _engine_order(cfg: FullPipelineConfig) -> tuple[str, ...]:
    flags = {
        "bing": cfg.use_bing,
        "duckduckgo": cfg.use_ddg,
        "yahoo": cfg.use_yahoo,
        "google": cfg.use_google,
    }
    return tuple(e for e in ENGINE_ORDER_DEFAULT if flags.get(e, False))


def run_domain_collection_only(
    cfg: FullPipelineConfig,
    *,
    progress: ProgressFn = None,
) -> FullPipelineResult:
    """
    Build queries and run Playwright collection only (writes domains.txt incrementally).
    """
    out = FullPipelineResult()
    dom_path = domains_path()

    def _p(msg: str, pct: float | None = None) -> None:
        if progress:
            progress(msg, pct)

    prev_serp = os.environ.get("LEADGEN_SERP_SCREENSHOT")
    try:
        if cfg.serp_screenshots:
            os.environ["LEADGEN_SERP_SCREENSHOT"] = "1"
        else:
            os.environ.pop("LEADGEN_SERP_SCREENSHOT", None)
        _p("Building search queries…", 0.05)
        out.queries = build_playwright_queries(
            cfg.keywords,
            cfg.country,
            cfg.state,
            b2b_enrich=cfg.b2b_query_expansion,
        )
        if not out.queries:
            out.queries = ["business"]

        out.keyword_detected_industries = infer_industries_from_keywords_text(cfg.keywords)

        order = _engine_order(cfg)
        if not order:
            out.errors.append("No search engines enabled. Turn on at least Bing, DDG, Yahoo, or Google.")
            return out

        clear_skip_engine_request()

        pages_per_engine = {
            "bing": max(1, cfg.serp_bing),
            "duckduckgo": max(1, cfg.serp_ddg),
            "yahoo": max(1, cfg.serp_yahoo),
            "google": max(1, cfg.serp_google),
        }

        _p(
            "Collecting domains (Playwright). Keep the browser window in view if not headless; "
            "this step can take a long time when the cap is high…",
            0.12,
        )

        def _collect_log(msg: str) -> None:
            _p(msg, None)

        if cfg.unlimited_domains:
            max_for_collect = max(1_000_000, int(cfg.max_domains))
        else:
            max_for_collect = max(10, min(int(cfg.max_domains), 2_000_000))

        try:
            out.collect_stats = collect_domains_playwright(
                queries=out.queries,
                keyword="",
                country="",
                states="",
                max_results=max_for_collect,
                per_engine_max=max(5, int(cfg.per_engine_max)),
                use_bing=cfg.use_bing,
                use_duckduckgo=cfg.use_ddg,
                use_yahoo=cfg.use_yahoo,
                use_google=cfg.use_google,
                engine_order=order,
                pages_per_engine=pages_per_engine,
                headless=cfg.headless_browser,
                append=cfg.append_domains,
                captcha_mode=cfg.captcha_mode,
                captcha_wait_ms=int(cfg.captcha_wait_ms),
                log_callback=_collect_log,
                b2b_enrich=False,
                unlimited=bool(cfg.unlimited_domains),
            )
        except Exception as e:
            logger.exception("collect failed")
            out.errors.append(f"Domain collection failed: {e}")
            return out

        out.domains_line_count = count_nonempty_lines(dom_path)
        if out.domains_line_count == 0:
            out.errors.append("No domains were written to domains.txt.")
            return out

        _p("Domain collection finished.", 0.35)
    finally:
        if prev_serp is not None:
            os.environ["LEADGEN_SERP_SCREENSHOT"] = prev_serp
        else:
            os.environ.pop("LEADGEN_SERP_SCREENSHOT", None)

    return out


def run_email_extraction_phase(
    cfg: FullPipelineConfig,
    *,
    progress: ProgressFn = None,
    keyword_detected_industries: tuple[str, ...] | None = None,
) -> FullPipelineResult:
    """
    Crawl domains.txt → emails.txt, then qualify and write CSV exports.
    """
    out = FullPipelineResult()
    em_path = emails_txt_path()
    dom_path = domains_path()

    def _p(msg: str, pct: float | None = None) -> None:
        if progress:
            progress(msg, pct)

    prev_serp = os.environ.get("LEADGEN_SERP_SCREENSHOT")
    try:
        if cfg.serp_screenshots:
            os.environ["LEADGEN_SERP_SCREENSHOT"] = "1"
        else:
            os.environ.pop("LEADGEN_SERP_SCREENSHOT", None)

        if not dom_path.is_file() or count_nonempty_lines(dom_path) == 0:
            out.errors.append("No domains in domains.txt; run collection first.")
            return out

        _p("Crawling emails (threaded HTTP + optional Playwright fallback)…", 0.42)
        try:
            out.crawl_stats = crawl_domains_to_emails_txt(
                max_workers=max(1, int(cfg.email_workers)),
                headless_fallback=cfg.headless_playwright_email_fallback,
                append=cfg.append_emails,
                log_callback=lambda m: _p(m, None),
            )
        except Exception as e:
            logger.exception("crawl failed")
            out.errors.append(f"Email crawl failed: {e}")
            return out

        out.emails_line_count = count_nonempty_lines(em_path)
        rows = parse_emails_txt(em_path)
        if not rows:
            out.errors.append("No rows in emails.txt after crawl; skipping qualification.")
            return out

        _p("Qualifying leads (homepage fetch + scoring)…", 0.72)
        sel = normalize_industry_selection(cfg.selected_industries)
        kw_src = keyword_detected_industries or infer_industries_from_keywords_text(cfg.keywords)
        kw_froze = frozenset(kw_src)
        try:
            leads = qualify_email_rows(
                rows,
                use_openai=cfg.use_openai_summaries,
                require_target_industry=False,
                strict_industry_filter=bool(cfg.strict_industry_filter and sel),
                selected_industries=sel or None,
                keyword_inferred_industries=kw_froze or None,
                progress=lambda m: _p(m, None),
            )
        except Exception as e:
            logger.exception("qualify failed")
            out.errors.append(f"Lead qualification failed: {e}")
            return out

        out.qualified_leads = leads
        out.keyword_detected_industries = list(kw_src)
        out.qualified_count = len(leads)
        out.high_quality_count = sum(1 for L in leads if int(L.score) > 6)

        write_qualified_csv(leads, qualified_leads_csv_path())
        write_outreach_csv(leads, outreach_ready_csv_path())
        out.domains_line_count = count_nonempty_lines(dom_path)
        _p("Email extraction and qualification complete.", 1.0)
    finally:
        if prev_serp is not None:
            os.environ["LEADGEN_SERP_SCREENSHOT"] = prev_serp
        else:
            os.environ.pop("LEADGEN_SERP_SCREENSHOT", None)

    return out


def run_full_pipeline(
    cfg: FullPipelineConfig,
    *,
    progress: ProgressFn = None,
) -> FullPipelineResult:
    out = run_domain_collection_only(cfg, progress=progress)
    if out.errors:
        return out

    kw_tuple = tuple(out.keyword_detected_industries)
    ex = run_email_extraction_phase(cfg, progress=progress, keyword_detected_industries=kw_tuple)
    out.crawl_stats = ex.crawl_stats
    out.emails_line_count = ex.emails_line_count
    out.qualified_leads = ex.qualified_leads
    out.qualified_count = ex.qualified_count
    out.high_quality_count = ex.high_quality_count
    out.errors.extend(ex.errors)
    return out


__all__ = [
    "FullPipelineConfig",
    "FullPipelineResult",
    "count_nonempty_lines",
    "run_domain_collection_only",
    "run_email_extraction_phase",
    "run_full_pipeline",
]
