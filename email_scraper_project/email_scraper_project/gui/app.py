"""
Lead generation dashboard (Streamlit): domain collection + email crawl + export preview.

Run from project root (folder containing scrapy.cfg):

    streamlit run email_scraper_project/gui/app.py
"""

from __future__ import annotations

import os
import json
import subprocess
import sys
import tempfile
import time
from pathlib import Path

import streamlit as st

GUI_FILE = Path(__file__).resolve()
PROJECT_ROOT = GUI_FILE.parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

os.chdir(PROJECT_ROOT)

from email_scraper_project.browser_search.query_builder import build_playwright_queries  # noqa: E402
from email_scraper_project.lead_qualifier import (  # noqa: E402
    KNOWN_INDUSTRY_KEYS,
    QualifiedLead,
    infer_industries_from_keywords_text,
    normalize_industry_selection,
    parse_domains_txt,
    parse_emails_txt,
    qualify_domains_only,
    qualify_email_rows,
    write_outreach_csv,
    write_qualified_csv,
)
from email_scraper_project.pipeline_runner import (  # noqa: E402
    FullPipelineConfig,
    count_nonempty_lines,
    run_domain_collection_only,
    run_email_extraction_phase,
    run_full_pipeline,
)
from email_scraper_project.browser_search.stop_collection_request import (  # noqa: E402
    clear_stop_collection_request,
    request_stop_collection,
    stop_collection_request_path,
)
from email_scraper_project.browser_search.skip_engine_request import (  # noqa: E402
    clear_skip_engine_request,
    request_skip_engine,
    skip_engine_request_path,
)
from email_scraper_project.config import (  # noqa: E402
    data_dir,
    domains_path,
    emails_csv_path,
    emails_txt_path,
    leads_json_path,
    logs_dir,
    main_log_txt_path,
    manual_leads_csv_path,
    manual_qualified_leads_csv_path,
    outreach_ready_csv_path,
    qualified_leads_csv_path,
)
from email_scraper_project.manual_serp_processor import (  # noqa: E402
    run_manual_serp,
    write_manual_leads_csv,
    write_manual_qualified_csv,
)


def _tail_text(path: Path, max_chars: int = 12000) -> str:
    if not path.exists():
        return ""
    try:
        text = path.read_text(encoding="utf-8", errors="ignore")
    except Exception:
        return ""
    if len(text) > max_chars:
        return text[-max_chars:]
    return text


def _preview_csv(path: Path, n: int = 30) -> str:
    if not path.exists():
        return "(file not found)"
    try:
        lines = path.read_text(encoding="utf-8", errors="ignore").splitlines()
    except Exception as e:
        return str(e)
    return "\n".join(lines[: n + 1])


def _tail_jsonl_events(path: Path, max_lines: int = 50) -> str:
    """Human-readable tail for jsonl event logs."""
    if not path.exists():
        return "(file not found)"
    try:
        lines = path.read_text(encoding="utf-8", errors="ignore").splitlines()
    except Exception as e:
        return str(e)
    out: list[str] = []
    for raw in lines[-max_lines:]:
        raw = raw.strip()
        if not raw:
            continue
        try:
            obj = json.loads(raw)
        except Exception:
            out.append(raw)
            continue
        ts = obj.get("ts", "")
        lvl = obj.get("level", "")
        logn = obj.get("logger", "")
        msg = obj.get("message", "")
        data = obj.get("data")
        row = f"{ts} | {lvl} | {logn} | {msg}"
        if data:
            row += f" | data={data}"
        out.append(row)
    return "\n".join(out) if out else "(log empty)"


def _line_count(path: Path) -> int:
    if not path.exists():
        return 0
    try:
        return len(path.read_text(encoding="utf-8", errors="ignore").splitlines())
    except Exception:
        return 0


def _terminate_proc_if_running(proc) -> None:
    if proc is None:
        return
    try:
        if proc.poll() is None:
            proc.terminate()
            time.sleep(0.15)
    except Exception:
        pass


def _clear_all_outputs_and_logs() -> tuple[int, int]:
    """
    Delete generated output and log files.
    Returns (files_deleted, dirs_deleted).
    """
    files_deleted = 0
    dirs_deleted = 0

    targets = [
        domains_path(),
        emails_txt_path(),
        emails_csv_path(),
        leads_json_path(),
        main_log_txt_path(),
        logs_dir() / "gui_collect.log",
        logs_dir() / "gui_scrapy.log",
        logs_dir() / "gui_playwright_collect.log",
        logs_dir() / "gui_playwright_crawl.log",
        logs_dir() / "domain_collection.jsonl",
        logs_dir() / "gui_playwright_crawl.log",
        qualified_leads_csv_path(),
        outreach_ready_csv_path(),
        manual_leads_csv_path(),
        manual_qualified_leads_csv_path(),
    ]
    for p in targets:
        try:
            if p.exists() and p.is_file():
                p.unlink()
                files_deleted += 1
        except Exception:
            continue

    # Optional cleanup for screenshot/debug directories.
    for d in (logs_dir() / "serp_screenshots",):
        try:
            if d.exists() and d.is_dir():
                for child in d.iterdir():
                    if child.is_file():
                        child.unlink(missing_ok=True)
                        files_deleted += 1
                d.rmdir()
                dirs_deleted += 1
        except Exception:
            continue

    return files_deleted, dirs_deleted


def _popen_kwargs(**extra) -> dict:
    kw: dict = {"cwd": str(PROJECT_ROOT), **extra}
    if sys.platform == "win32" and hasattr(subprocess, "CREATE_NO_WINDOW"):
        kw["creationflags"] = subprocess.CREATE_NO_WINDOW  # type: ignore[arg-type]
    return kw


# Inherited HTTP(S)_PROXY often points at a dead local proxy and breaks search requests.
_PROXY_ENV_KEYS = frozenset(
    {
        "HTTP_PROXY",
        "HTTPS_PROXY",
        "ALL_PROXY",
        "http_proxy",
        "https_proxy",
        "all_proxy",
    }
)


def _env_without_system_proxy(base: dict | None = None) -> dict:
    env = dict(base or os.environ)
    for k in _PROXY_ENV_KEYS:
        env.pop(k, None)
    return env


def main() -> None:
    st.set_page_config(page_title="Lead Gen", layout="wide")
    st.title("Automated lead generation")
    st.caption("Search → domains → crawl → emails. Free engines only; use responsibly.")

    for key, default in (
        ("collect_proc", None),
        ("collect_log", None),
        ("crawl_proc", None),
        ("crawl_log", None),
        ("crawl_done_msg", None),
        ("collect_done_msg", None),
        ("pw_collect_proc", None),
        ("pw_collect_log", None),
        ("pw_collect_log_fp", None),
        ("pw_crawl_proc", None),
        ("pw_crawl_log", None),
        ("pw_crawl_log_fp", None),
        ("pw_msg", None),
        ("flp_collect_proc", None),
        ("flp_collect_log", None),
        ("flp_collect_log_fp", None),
        ("flp_collect_started", None),
        ("flp_crawl_proc", None),
        ("flp_crawl_log", None),
        ("flp_crawl_log_fp", None),
        ("flp_crawl_started", None),
        ("pipe_mxnum", 500),
    ):
        if key not in st.session_state:
            st.session_state[key] = default

    with st.sidebar:
        st.header("Data folder")
        st.write(str(data_dir()))
        if st.button("Open logs folder"):
            try:
                os.startfile(str(logs_dir()))  # type: ignore[attr-defined]
            except Exception:
                st.info(str(logs_dir()))
        st.divider()
        clear_all = st.button(
            "Clear all outputs + logs",
            help="Stops running jobs and deletes domains/emails/csv/json + log files.",
            type="secondary",
        )
        if clear_all:
            _terminate_proc_if_running(st.session_state.get("collect_proc"))
            _terminate_proc_if_running(st.session_state.get("crawl_proc"))
            _terminate_proc_if_running(st.session_state.get("pw_collect_proc"))
            _terminate_proc_if_running(st.session_state.get("pw_crawl_proc"))
            _terminate_proc_if_running(st.session_state.get("flp_collect_proc"))
            _terminate_proc_if_running(st.session_state.get("flp_crawl_proc"))
            st.session_state.collect_proc = None
            st.session_state.crawl_proc = None
            st.session_state.pw_collect_proc = None
            st.session_state.pw_crawl_proc = None
            st.session_state.flp_collect_proc = None
            st.session_state.flp_crawl_proc = None
            for fp_key in ("collect_log_fp", "pw_collect_log_fp", "pw_crawl_log_fp", "flp_collect_log_fp", "flp_crawl_log_fp"):
                fp = st.session_state.pop(fp_key, None)
                if fp:
                    try:
                        fp.close()
                    except Exception:
                        pass
            qtmp = st.session_state.pop("_pw_queries_tmp", None)
            if qtmp:
                try:
                    os.unlink(qtmp)
                except OSError:
                    pass
            clear_skip_engine_request()
            clear_stop_collection_request()
            n_files, n_dirs = _clear_all_outputs_and_logs()
            st.session_state.collect_done_msg = "All outputs/logs cleared."
            st.session_state.crawl_done_msg = "All outputs/logs cleared."
            st.session_state.pw_msg = f"Cleared {n_files} file(s), {n_dirs} directorie(s)."
            st.rerun()

    c1, c2, c3, c4, c5, c6 = st.tabs(
        [
            "1) HTTP collect",
            "2) Scrapy crawl",
            "3) Playwright + emails.txt",
            "4) AI Lead Intelligence",
            "5) Full Lead Pipeline (One Click)",
            "6) Manual SERP Extractor",
        ]
    )

    with c1:
        st.subheader("Keyword + country search")
        kw = st.text_input("Keywords (comma-separated)", "dentist, lawyer")
        country = st.text_input("Country", "USA")
        state = st.text_input("State / region (optional)", "")
        city = st.text_input("City (optional)", "")
        industry = st.text_input("Industry filter (optional)", "")
        pages = st.slider("Pages per engine per query", 1, 10, 3)
        use_proxy = st.checkbox(
            "Use free HTTP proxies (often unstable; leave off if searches fail)",
            value=False,
        )
        append_domains = st.checkbox("Append to existing domains.txt", value=True)
        b2b_http = st.checkbox(
            "B2B-style search queries (company / services + location)",
            value=True,
            help="Adds queries like “keyword company Texas USA” before HTTP collection.",
        )

        b1, b2 = st.columns(2)
        with b1:
            start_c = st.button("Start", type="primary", key="collect_start")
        with b2:
            stop_c = st.button("Stop", key="collect_stop")

        if start_c and st.session_state.collect_proc is None:
            log_path = logs_dir() / "gui_collect.log"
            logs_dir().mkdir(parents=True, exist_ok=True)
            log_path.write_text("", encoding="utf-8")
            cmd = [
                sys.executable,
                "-m",
                "email_scraper_project.spiders.collect_domains",
                "--keywords",
                kw,
                "--country",
                country,
                "--pages",
                str(pages),
            ]
            if state.strip():
                cmd.extend(["--state", state.strip()])
            if city.strip():
                cmd.extend(["--city", city.strip()])
            if industry.strip():
                cmd.extend(["--industry", industry.strip()])
            if use_proxy:
                cmd.append("--proxies")
            if not append_domains:
                cmd.append("--no-append")
            if b2b_http:
                cmd.append("--b2b-queries")

            env = _env_without_system_proxy()
            env["PYTHONUTF8"] = "1"
            env["PYTHONUNBUFFERED"] = "1"
            if use_proxy:
                env["LEADGEN_USE_PROXIES"] = "1"

            logf = open(log_path, "w", encoding="utf-8")
            st.session_state.collect_proc = subprocess.Popen(
                cmd,
                stdout=logf,
                stderr=subprocess.STDOUT,
                env=env,
                **_popen_kwargs(),
            )
            st.session_state.collect_log = log_path
            st.session_state.collect_log_fp = logf  # noqa: keep handle open
            st.session_state.collect_done_msg = None
            st.rerun()

        if stop_c and st.session_state.collect_proc is not None:
            try:
                st.session_state.collect_proc.terminate()
            except Exception:
                pass
            st.session_state.collect_proc = None
            fp = st.session_state.pop("collect_log_fp", None)
            if fp:
                try:
                    fp.close()
                except Exception:
                    pass
            st.session_state.collect_done_msg = "Collection stopped by user."
            st.rerun()

        proc_c = st.session_state.collect_proc
        if proc_c is not None:
            poll = proc_c.poll()
            if poll is None:
                st.progress(0.5, text="Collecting…")
                lp = st.session_state.collect_log
                if lp:
                    st.code(_tail_text(Path(lp)))
                time.sleep(0.45)
                st.rerun()
            else:
                fp = st.session_state.pop("collect_log_fp", None)
                if fp:
                    try:
                        fp.close()
                    except Exception:
                        pass
                st.session_state.collect_proc = None
                lp = st.session_state.collect_log
                if lp:
                    st.code(_tail_text(Path(lp)))
                if poll == 0:
                    st.success("Domain collection finished.")
                else:
                    st.error(f"Process exited with code {poll}")
                st.session_state.collect_done_msg = None

        if st.session_state.collect_done_msg:
            st.warning(st.session_state.collect_done_msg)

        st.metric("domains.txt", str(domains_path()))

    with c2:
        st.subheader("Scrapy email spider")
        st.write(f"Reads: `{domains_path()}` → writes: `{emails_csv_path()}`")
        overwrite = st.checkbox("Overwrite emails.csv (fresh CSV)", value=False)
        json_out = st.checkbox("Append JSON lines to leads.json", value=True)
        max_pages = st.number_input("Max pages per domain", min_value=1, max_value=50, value=15)
        early = st.number_input("Stop crawling domain after N emails", min_value=1, max_value=100, value=5)

        b3, b4 = st.columns(2)
        with b3:
            start_r = st.button("Start", type="primary", key="crawl_start")
        with b4:
            stop_r = st.button("Stop", key="crawl_stop")

        if start_r and st.session_state.crawl_proc is None:
            slog = logs_dir() / "gui_scrapy.log"
            logs_dir().mkdir(parents=True, exist_ok=True)
            slog.write_text("", encoding="utf-8")
            cmd = [
                sys.executable,
                "-m",
                "scrapy",
                "crawl",
                "email_spider",
                "-s",
                f"LOG_FILE={slog}",
                "-s",
                f"EMAIL_FEED_OVERWRITE={'True' if overwrite else 'False'}",
                "-a",
                f"max_pages={int(max_pages)}",
                "-a",
                f"early_stop={int(early)}",
            ]
            env = _env_without_system_proxy()
            env["PYTHONUTF8"] = "1"
            env["PYTHONUNBUFFERED"] = "1"
            if not json_out:
                env["LEADGEN_JSON_EXPORT"] = "0"

            st.session_state.crawl_proc = subprocess.Popen(
                cmd,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                env=env,
                **_popen_kwargs(),
            )
            st.session_state.crawl_log = slog
            st.session_state.crawl_done_msg = None
            st.rerun()

        if stop_r and st.session_state.crawl_proc is not None:
            try:
                st.session_state.crawl_proc.terminate()
            except Exception:
                pass
            st.session_state.crawl_proc = None
            st.session_state.crawl_done_msg = "Crawl stopped by user."
            st.rerun()

        proc_r = st.session_state.crawl_proc
        if proc_r is not None:
            poll = proc_r.poll()
            if poll is None:
                st.progress(0.5, text="Scrapy running…")
                sl = st.session_state.crawl_log
                if sl:
                    st.code(_tail_text(Path(sl)))
                time.sleep(0.5)
                st.rerun()
            else:
                st.session_state.crawl_proc = None
                sl = st.session_state.crawl_log
                if sl:
                    st.code(_tail_text(Path(sl)))
                if poll == 0:
                    st.success("Crawl finished.")
                else:
                    st.error(f"Scrapy exited with code {poll}")

        if st.session_state.crawl_done_msg:
            st.warning(st.session_state.crawl_done_msg)

        st.subheader("Output preview")
        st.text_area("emails.csv (first lines)", _preview_csv(emails_csv_path()), height=220)
        if leads_json_path().exists():
            st.text_area("leads.json (first lines)", _preview_csv(leads_json_path()), height=160)

        dl1, dl2, dl3 = st.columns(3)
        p_csv = emails_csv_path()
        p_dom = domains_path()
        p_js = leads_json_path()
        if p_csv.exists():
            dl1.download_button("Download emails.csv", p_csv.read_bytes(), "emails.csv")
        if p_dom.exists():
            dl2.download_button("Download domains.txt", p_dom.read_bytes(), "domains.txt")
        if p_js.exists():
            dl3.download_button("Download leads.json", p_js.read_bytes(), "leads.json")

    with c3:
        st.subheader("Playwright browser search + threaded email crawl")
        st.caption(
            "Uses Chromium (visible by default). Install: `pip install playwright` then "
            "`python -m playwright install chromium`. "
            "Each enabled engine runs for every query (Bing cannot use the whole budget alone)."
        )
        pkw = st.text_area(
            "Keywords (comma, semicolon, or newline)",
            "motor rewinding services",
            height=88,
            key="pw_kw",
        )
        pcountry = st.text_area(
            "Countries / regions (comma or newline)",
            "USA",
            height=70,
            key="pw_country",
        )
        pstates = st.text_area(
            "States / provinces (optional; comma or newline)",
            "",
            height=70,
            key="pw_states",
            placeholder="e.g. Texas, Florida (leave empty to skip)",
        )
        presults = st.slider("Max total unique domains (stop when reached)", 20, 500, 80, key="pw_n")
        ppem = st.slider(
            "Max new domains per engine, per query",
            10,
            120,
            35,
            help="Each selected engine runs until it adds this many (or runs out of pages), then the next engine runs.",
            key="pw_pem",
        )
        st.caption(
            "Search engines: every checked engine runs, in order: Bing, then DuckDuckGo, then Yahoo, then Google."
        )
        pw_bing = st.checkbox("Bing", value=True, key="pw_bing")
        pw_ddg = st.checkbox(
            "DuckDuckGo (homepage search; HTML fallback if blocked)", value=True, key="pw_ddg"
        )
        pw_yahoo = st.checkbox("Yahoo", value=True, key="pw_yahoo")
        pw_google = st.checkbox(
            "Google (optional; solve CAPTCHA in browser if shown)", value=False, key="pw_g"
        )
        st.caption(
            "Searches use each engine’s **homepage + search box** (not raw `?q=` URLs). "
            "Domains are taken from result **links** in the page. Optional debug PNGs: set env "
            "`LEADGEN_SERP_SCREENSHOT=1` → `logs/serp_screenshots/` (OCR is not used; link harvest "
            "is more reliable)."
        )
        st.caption(
            "Google: run **non-headless**, long captcha wait, optional `LEADGEN_PW_USER_DATA_DIR` "
            "(persistent Chromium profile) and `LEADGEN_PW_CHANNEL=chrome`."
        )
        with st.expander("SERP page limits (per search engine)", expanded=True):
            st.caption(
                "Max **result pages** to open per engine for each query (page 1 counts as 1; "
                "then Next / pagination). Example: Bing 1, Google 2, DDG 3, Yahoo 1."
            )
            r1, r2 = st.columns(2)
            with r1:
                pw_pg_bing = st.number_input("Bing pages", min_value=1, max_value=100, value=5, key="pw_pgb")
                pw_pg_ddg = st.number_input("DuckDuckGo pages", min_value=1, max_value=100, value=5, key="pw_pgddg")
            with r2:
                pw_pg_yahoo = st.number_input("Yahoo pages", min_value=1, max_value=100, value=5, key="pw_pgy")
                pw_pg_google = st.number_input("Google pages", min_value=1, max_value=100, value=5, key="pw_pgg")
        pw_headless = st.checkbox("Headless browser (no window)", value=False, key="pw_h")
        pw_append_dom = st.checkbox("Append domains.txt", value=True, key="pw_ad")
        pw_workers = st.slider("Email crawl threads", 1, 16, 6, key="pw_w")
        pw_append_em = st.checkbox("Append emails.txt", value=True, key="pw_ae")
        pw_fb_headful = st.checkbox(
            "Headful Playwright fallback when HTTP is blocked", value=False, key="pw_fb"
        )
        pw_serp_png = st.checkbox(
            "Save full-page SERP screenshots (logs/serp_screenshots/) for debugging",
            value=False,
            key="pw_png",
        )
        pw_b2b = st.checkbox(
            "B2B-style browser queries (company / services + region)",
            value=True,
            key="pw_b2b",
            help="Expands each keyword × region into additional high-intent searches.",
        )

        pc1, pc2, pc3, pc4 = st.columns(4)
        with pc1:
            pw_start_collect = st.button(
                "Collect (browser)", type="primary", key="pw_sc"
            )
        with pc2:
            pw_stop_collect = st.button("Stop collect", key="pw_xc")
        with pc3:
            pw_start_crawl = st.button("Crawl → emails.txt", type="primary", key="pw_sl")
        with pc4:
            pw_stop_crawl = st.button("Stop crawl", key="pw_xl")

        if pw_start_collect and st.session_state.pw_collect_proc is None:
            clear_skip_engine_request()
            clear_stop_collection_request()
            lg = logs_dir() / "gui_playwright_collect.log"
            logs_dir().mkdir(parents=True, exist_ok=True)
            lg.write_text("", encoding="utf-8")
            qlist = build_playwright_queries(pkw, pcountry, pstates, b2b_enrich=bool(pw_b2b))
            fd, qpath = tempfile.mkstemp(suffix="_queries.txt", text=True)
            os.close(fd)
            Path(qpath).write_text("\n".join(qlist), encoding="utf-8")
            st.session_state["_pw_queries_tmp"] = qpath
            cmd = [
                sys.executable,
                "-u",
                "-m",
                "email_scraper_project.playwright_cli",
                "collect",
                "--queries-file",
                qpath,
                "--results",
                str(int(presults)),
                "--per-engine-max",
                str(int(ppem)),
                "--bing-pages",
                str(int(pw_pg_bing)),
                "--ddg-pages",
                str(int(pw_pg_ddg)),
                "--yahoo-pages",
                str(int(pw_pg_yahoo)),
                "--google-pages",
                str(int(pw_pg_google)),
                "--captcha-mode",
                "wait",
                "--captcha-wait-ms",
                "600000",
            ]
            if not pw_bing:
                cmd.append("--no-bing")
            if not pw_ddg:
                cmd.append("--no-ddg")
            if not pw_yahoo:
                cmd.append("--no-yahoo")
            if pw_google:
                cmd.append("--google")
            if not pw_append_dom:
                cmd.append("--no-append")
            env = _env_without_system_proxy()
            env["PYTHONUTF8"] = "1"
            env["PYTHONUNBUFFERED"] = "1"
            env["LEADGEN_CAPTCHA_MODE"] = "wait"
            if pw_serp_png:
                env["LEADGEN_SERP_SCREENSHOT"] = "1"
            else:
                env.pop("LEADGEN_SERP_SCREENSHOT", None)
            if pw_headless:
                env["LEADGEN_PLAYWRIGHT_HEADLESS"] = "1"
            else:
                env.pop("LEADGEN_PLAYWRIGHT_HEADLESS", None)
            fp = open(lg, "w", encoding="utf-8")
            st.session_state.pw_collect_proc = subprocess.Popen(
                cmd,
                stdout=fp,
                stderr=subprocess.STDOUT,
                env=env,
                **_popen_kwargs(),
            )
            st.session_state.pw_collect_log = lg
            st.session_state.pw_collect_log_fp = fp
            st.session_state.pw_msg = None
            st.rerun()

        if pw_stop_collect and st.session_state.pw_collect_proc is not None:
            request_stop_collection()
            st.session_state.pw_msg = "Stop requested; browser collection exits after the current step (see leadgen_stop_collection.txt)."
            st.rerun()

        pwc = st.session_state.pw_collect_proc
        if pwc is not None:
            poll = pwc.poll()
            if poll is None:
                st.progress(0.45, text="Playwright collecting… (watch Chromium window)")
                with st.expander("Skip a search engine (if one hangs or CAPTCHA stalls)", expanded=False):
                    st.caption(
                        f"Writes `{skip_engine_request_path().name}` under your data folder. "
                        "Checked between engines and every ~2s during CAPTCHA wait."
                    )
                    s1, s2, s3, s4, s5 = st.columns(5)
                    if s1.button("Skip Bing", key="sk_bing"):
                        request_skip_engine("bing")
                    if s2.button("Skip DDG", key="sk_ddg"):
                        request_skip_engine("duckduckgo")
                    if s3.button("Skip Yahoo", key="sk_yahoo"):
                        request_skip_engine("yahoo")
                    if s4.button("Skip Google", key="sk_google"):
                        request_skip_engine("google")
                    if s5.button("Skip ALL", key="sk_all"):
                        request_skip_engine("all")
                plg = st.session_state.pw_collect_log
                if plg:
                    st.code(_tail_text(Path(plg)))
                time.sleep(0.45)
                st.rerun()
            else:
                fp = st.session_state.pop("pw_collect_log_fp", None)
                if fp:
                    try:
                        fp.close()
                    except Exception:
                        pass
                st.session_state.pw_collect_proc = None
                plg = st.session_state.pw_collect_log
                if plg:
                    st.code(_tail_text(Path(plg)))
                if poll == 0:
                    st.success("Browser collection finished.")
                else:
                    st.error(f"Collect exited with code {poll}")
                qtmp = st.session_state.pop("_pw_queries_tmp", None)
                if qtmp:
                    try:
                        os.unlink(qtmp)
                    except OSError:
                        pass

        if pw_start_crawl and st.session_state.pw_crawl_proc is None:
            lg = logs_dir() / "gui_playwright_crawl.log"
            logs_dir().mkdir(parents=True, exist_ok=True)
            lg.write_text("", encoding="utf-8")
            cmd = [
                sys.executable,
                "-u",
                "-m",
                "email_scraper_project.playwright_cli",
                "crawl",
                "--workers",
                str(int(pw_workers)),
            ]
            if not pw_append_em:
                cmd.append("--no-append")
            if pw_fb_headful:
                cmd.append("--fallback-headful")
            env = _env_without_system_proxy()
            env["PYTHONUTF8"] = "1"
            env["PYTHONUNBUFFERED"] = "1"
            if pw_headless:
                env["LEADGEN_PLAYWRIGHT_HEADLESS"] = "1"
            fpw = open(lg, "w", encoding="utf-8")
            st.session_state.pw_crawl_proc = subprocess.Popen(
                cmd,
                stdout=fpw,
                stderr=subprocess.STDOUT,
                env=env,
                **_popen_kwargs(),
            )
            st.session_state.pw_crawl_log = lg
            st.session_state.pw_crawl_log_fp = fpw
            st.session_state.pw_msg = None
            st.rerun()

        if pw_stop_crawl and st.session_state.pw_crawl_proc is not None:
            try:
                st.session_state.pw_crawl_proc.terminate()
            except Exception:
                pass
            st.session_state.pw_crawl_proc = None
            fpw = st.session_state.pop("pw_crawl_log_fp", None)
            if fpw:
                try:
                    fpw.close()
                except Exception:
                    pass
            st.session_state.pw_msg = "Email crawl stopped."
            st.rerun()

        pwr = st.session_state.pw_crawl_proc
        if pwr is not None:
            poll = pwr.poll()
            if poll is None:
                st.progress(0.45, text="Threaded email crawl…")
                clg = st.session_state.pw_crawl_log
                if clg:
                    st.code(_tail_text(Path(clg)))
                time.sleep(0.5)
                st.rerun()
            else:
                fpw = st.session_state.pop("pw_crawl_log_fp", None)
                if fpw:
                    try:
                        fpw.close()
                    except Exception:
                        pass
                st.session_state.pw_crawl_proc = None
                clg = st.session_state.pw_crawl_log
                if clg:
                    st.code(_tail_text(Path(clg)))
                if poll == 0:
                    st.success("emails.txt crawl finished.")
                else:
                    st.error(f"Crawl exited with code {poll}")

        if st.session_state.pw_msg:
            st.warning(st.session_state.pw_msg)

        dp = domains_path()
        et = emails_txt_path()
        main_log = main_log_txt_path()
        pw_collect_log = logs_dir() / "gui_playwright_collect.log"
        pw_crawl_log = logs_dir() / "gui_playwright_crawl.log"
        domain_jsonl = logs_dir() / "domain_collection.jsonl"

        st.subheader("Output preview")
        c_meta1, c_meta2, c_meta3 = st.columns(3)
        c_meta1.metric("domains.txt lines", _line_count(dp))
        c_meta2.metric("emails.txt lines", _line_count(et))
        c_meta3.metric("domain_collection.jsonl events", _line_count(domain_jsonl))

        st.text_area(
            "domains.txt (preview)",
            _preview_csv(dp),
            height=120,
            key="pw_preview_domains",
        )
        if not et.exists():
            st.info("emails.txt not created yet. Run `Crawl → emails.txt` after collection.")
        st.text_area(
            "emails.txt (preview)",
            _preview_csv(et),
            height=160,
        )

        with st.expander("Backend logs (collector / crawler / events)", expanded=True):
            st.caption("These are the direct backend logs used by the dashboard subprocesses.")
            st.text_area(
                "Playwright collect log (gui_playwright_collect.log)",
                _tail_text(pw_collect_log, 14000),
                height=180,
            )
            st.text_area(
                "Playwright crawl log (gui_playwright_crawl.log)",
                _tail_text(pw_crawl_log, 14000),
                height=150,
            )
            st.text_area(
                "Domain collection events (domain_collection.jsonl)",
                _tail_jsonl_events(domain_jsonl, 60),
                height=170,
            )
            st.text_area(
                "Global logs.txt (tail)",
                _tail_text(main_log, 8000),
                height=130,
            )

        d1, d2, d3 = st.columns(3)
        lt = main_log
        if et.exists():
            d1.download_button("Download emails.txt", et.read_bytes(), "emails.txt", key="dl_et")
        if lt.exists():
            d2.download_button("Download logs.txt", lt.read_bytes(), "logs.txt", key="dl_lt")
        if dp.exists():
            d3.download_button("Download domains.txt", dp.read_bytes(), "domains.txt", key="dl_dt")

    with c4:
        st.subheader("AI Lead Intelligence")
        st.caption(
            "Classifies domains, filters low-intent emails, detects target industries, scores 0–10, "
            "and writes `qualified_leads.csv`. Optional: set `OPENAI_API_KEY` for one-line summaries "
            "(appended into the notes column)."
        )
        if "qualified_leads_cache" not in st.session_state:
            st.session_state.qualified_leads_cache = []

        src = st.radio(
            "Input source",
            ("emails.txt (recommended)", "domains.txt (homepage only)"),
            horizontal=True,
            key="intel_src",
        )
        use_ai = st.checkbox("Request AI summaries (needs OPENAI_API_KEY)", value=False, key="intel_ai")
        intel_industries = st.multiselect(
            "Target industries (boosts score when homepage matches; optional)",
            sorted(KNOWN_INDUSTRY_KEYS),
            default=[],
            key="intel_msel",
        )
        intel_strict = st.checkbox(
            "Strict industry filtering (keep only leads whose homepage industry is one of the selections above)",
            value=False,
            key="intel_strict",
        )
        intel_kw_hints = st.text_area(
            "Campaign keywords (optional; improves keyword–industry scoring)",
            "",
            height=70,
            key="intel_kw",
            help="Separate with comma, semicolon, or newline. Example: construction company, warehouse company, hvac service.",
        )
        _intel_inferred = infer_industries_from_keywords_text(intel_kw_hints)
        if _intel_inferred:
            st.info("Detected industries from keyword hints: **" + "**, **".join(_intel_inferred) + "**")

        def _intel_progress(msg: str) -> None:
            st.session_state["_intel_last"] = msg

        b_run = st.button("Run qualification", type="primary", key="intel_run")
        b_clean = st.button("Clean & Qualify Current List", key="intel_clean")

        _intel_sel_fz = normalize_industry_selection(intel_industries)
        _intel_kw_fz = frozenset(infer_industries_from_keywords_text(intel_kw_hints))

        if b_run:
            st.session_state["_intel_last"] = ""
            ran_ok = False
            with st.spinner("Qualifying… (fetches homepages; may take a while)"):
                if src.startswith("emails"):
                    rows = parse_emails_txt(emails_txt_path())
                    if not rows:
                        st.warning("emails.txt is empty or missing. Crawl first or pick domains.txt.")
                    else:
                        st.session_state.qualified_leads_cache = qualify_email_rows(
                            rows,
                            use_openai=use_ai,
                            require_target_industry=False,
                            strict_industry_filter=bool(intel_strict and _intel_sel_fz),
                            selected_industries=_intel_sel_fz or None,
                            keyword_inferred_industries=_intel_kw_fz or None,
                            progress=_intel_progress,
                        )
                        ran_ok = True
                else:
                    doms = parse_domains_txt(domains_path())
                    if not doms:
                        st.warning("domains.txt is empty or missing.")
                    else:
                        st.session_state.qualified_leads_cache = qualify_domains_only(
                            doms,
                            require_target_industry=False,
                            strict_industry_filter=bool(intel_strict and _intel_sel_fz),
                            selected_industries=_intel_sel_fz or None,
                            keyword_inferred_industries=_intel_kw_fz or None,
                            progress=_intel_progress,
                        )
                        ran_ok = True
            if ran_ok:
                write_qualified_csv(
                    st.session_state.qualified_leads_cache,
                    qualified_leads_csv_path(),
                )
                write_outreach_csv(
                    st.session_state.qualified_leads_cache,
                    outreach_ready_csv_path(),
                )
                st.success(
                    f"Wrote {qualified_leads_csv_path().name} ({len(st.session_state.qualified_leads_cache)} rows) "
                    f"and {outreach_ready_csv_path().name}."
                )
            st.rerun()

        if b_clean:
            st.session_state["_intel_last"] = ""
            rows = parse_emails_txt(emails_txt_path())
            with st.spinner("Cleaning emails.txt → qualified leads…"):
                st.session_state.qualified_leads_cache = qualify_email_rows(
                    rows,
                    use_openai=use_ai,
                    require_target_industry=False,
                    strict_industry_filter=bool(intel_strict and _intel_sel_fz),
                    selected_industries=_intel_sel_fz or None,
                    keyword_inferred_industries=_intel_kw_fz or None,
                    progress=_intel_progress,
                )
            if st.session_state.qualified_leads_cache:
                write_qualified_csv(
                    st.session_state.qualified_leads_cache,
                    qualified_leads_csv_path(),
                )
                write_outreach_csv(
                    st.session_state.qualified_leads_cache,
                    outreach_ready_csv_path(),
                )
                st.success(
                    f"Qualified {len(st.session_state.qualified_leads_cache)} lead(s); CSVs updated."
                )
            else:
                st.warning("No rows passed filters, or emails.txt is empty.")
            st.rerun()

        if st.session_state.get("_intel_last"):
            st.caption(st.session_state["_intel_last"])

        leads_all: list[QualifiedLead] = list(st.session_state.get("qualified_leads_cache") or [])
        st.metric("Cached qualified rows (this session)", len(leads_all))

        f1, f2, f3, f4 = st.columns(4)
        with f1:
            min_score = st.number_input("Min score", min_value=0, max_value=10, value=0, key="intel_min_sc")
        with f2:
            only_business = st.checkbox('Only classification "business"', value=False, key="intel_bus")
        with f3:
            ind_opts = [
                "",
                "construction",
                "hvac",
                "logistics",
                "warehouse",
                "manufacturing",
                "repair",
            ]
            ind_pick = st.selectbox("Industry filter", ind_opts, key="intel_indpick")
        with f4:
            dedupe = st.checkbox("Remove duplicate domain+email", value=True, key="intel_dedupe")

        def _apply_filters(leads: list[QualifiedLead]) -> list[QualifiedLead]:
            out = []
            seen_k: set[tuple[str, str]] = set()
            for L in leads:
                if int(L.score) < int(min_score):
                    continue
                if only_business and L.classification != "business":
                    continue
                if ind_pick and L.industry != ind_pick:
                    continue
                k = (L.domain.lower(), L.email.lower())
                if dedupe and k in seen_k:
                    continue
                if dedupe:
                    seen_k.add(k)
                out.append(L)
            return out

        leads_view = _apply_filters(leads_all)
        st.write(f"Showing **{len(leads_view)}** of {len(leads_all)} after filters.")
        if leads_view:
            st.dataframe(
                [
                    {
                        "domain": x.domain,
                        "email": x.email,
                        "industry": x.industry,
                        "detected_industry": x.detected_industry or x.industry,
                        "matched_industry": x.matched_industry,
                        "classification": x.classification,
                        "score": x.score,
                        "contact_page_url": x.contact_page_url,
                        "notes": (x.notes + (f" | ai: {x.ai_summary}" if x.ai_summary else ""))[:500],
                    }
                    for x in leads_view
                ],
                use_container_width=True,
                hide_index=True,
            )

        q_path = qualified_leads_csv_path()
        o_path = outreach_ready_csv_path()
        dqa, dqb = st.columns(2)
        if q_path.exists():
            dqa.download_button(
                "Download qualified_leads.csv",
                q_path.read_bytes(),
                "qualified_leads.csv",
                key="dl_ql",
            )
        if o_path.exists():
            dqb.download_button(
                "Download outreach_ready.csv",
                o_path.read_bytes(),
                "outreach_ready.csv",
                key="dl_or",
            )

    with c5:
        st.subheader("Full Lead Pipeline (One Click)")
        st.caption(
            "**Run Full Pipeline** runs in this Streamlit process: collect → crawl → qualify → CSV. "
            "Use **Decoupled run** for long collects (subprocess + graceful stop file). "
            "Do not start tab 3 collect/crawl at the same time as a pipeline collect. "
            "CAPTCHA handling uses **wait** mode (same as tab 3 collect)."
        )
        if "pipeline_last" not in st.session_state:
            st.session_state.pipeline_last = None

        pipe_kw = st.text_area(
            "Keywords (comma, semicolon, or newline)",
            "hvac services\nwarehouse logistics",
            height=88,
            key="pipe_kw",
        )
        _pipe_kw_inferred = infer_industries_from_keywords_text(pipe_kw)
        if _pipe_kw_inferred:
            st.info(
                "Detected industries from keywords (used for scoring): **"
                + "**, **".join(_pipe_kw_inferred)
                + "**"
            )
        pipe_country = st.text_input("Country / region", "USA", key="pipe_country")
        pipe_state = st.text_area(
            "States / provinces (optional; comma, semicolon, or newline)",
            "",
            height=56,
            key="pipe_state",
            placeholder="e.g. Texas, California, Florida",
        )
        pipe_industries = st.multiselect(
            "Target industries (optional; boosts score when homepage matches)",
            sorted(KNOWN_INDUSTRY_KEYS),
            default=[],
            key="pipe_ind",
        )
        pipe_strict = st.checkbox(
            "Strict industry filtering (keep only leads in selected industries above)",
            value=False,
            key="pipe_strict",
        )
        pipe_unlimited = st.checkbox(
            "Unlimited mode (run until manually stopped; ignores max domains cap)",
            value=False,
            key="pipe_unl",
        )

        def _pipe_slider_to_num() -> None:
            st.session_state.pipe_mxnum = int(st.session_state.pipe_mxsl)

        c5a, c5b = st.columns(2)
        with c5a:
            _mx = max(100, min(10_000, int(st.session_state.get("pipe_mxnum", 500))))
            st.slider(
                "Max domains (quick preset, 100–10,000)",
                100,
                10_000,
                _mx,
                key="pipe_mxsl",
                on_change=_pipe_slider_to_num,
            )
            st.number_input(
                "Max domains (exact cap; up to 2,000,000)",
                min_value=100,
                max_value=2_000_000,
                step=50,
                key="pipe_mxnum",
            )
            pipe_pem = st.slider(
                "Max new domains per engine, per query",
                10,
                120,
                35,
                key="pipe_pem",
            )
            pipe_workers = st.slider("Email crawl threads", 1, 16, 6, key="pipe_workers")
        with c5b:
            pipe_b2b = st.checkbox("B2B query expansion (company / services + region)", value=True, key="pipe_b2b")
            pipe_append_dom = st.checkbox("Append domains.txt (off = fresh file for this run)", value=False, key="pipe_ad")
            pipe_append_em = st.checkbox("Append emails.txt (off = fresh file for this run)", value=False, key="pipe_ae")
            pipe_headless = st.checkbox("Headless domain collection (no Chromium window)", value=False, key="pipe_hl")
            pipe_pw_headless_fb = st.checkbox(
                "Headless Playwright for email crawl fallback",
                value=True,
                key="pipe_pwhl",
                help="When HTTP crawl is blocked, use a headless Chromium fetch. Uncheck to show a browser window for fallback.",
            )
            pipe_openai = st.checkbox("AI summaries during qualification (OPENAI_API_KEY)", value=False, key="pipe_ai")
            pipe_png = st.checkbox("SERP debug screenshots (logs/serp_screenshots/)", value=False, key="pipe_png")

        st.caption("Search engines (order: Bing → DuckDuckGo → Yahoo → Google)")
        g5_1, g5_2, g5_3, g5_4 = st.columns(4)
        with g5_1:
            pipe_bing = st.checkbox("Bing", value=True, key="pipe_bing")
        with g5_2:
            pipe_ddg = st.checkbox("DuckDuckGo", value=True, key="pipe_ddg")
        with g5_3:
            pipe_yahoo = st.checkbox("Yahoo", value=True, key="pipe_yahoo")
        with g5_4:
            pipe_google = st.checkbox("Google (CAPTCHA possible)", value=False, key="pipe_google")

        with st.expander("SERP pages per engine", expanded=False):
            r5a, r5b = st.columns(2)
            with r5a:
                pipe_pg_bing = st.number_input("Bing pages", min_value=1, max_value=100, value=5, key="pipe_pgb")
                pipe_pg_ddg = st.number_input("DuckDuckGo pages", min_value=1, max_value=100, value=5, key="pipe_pgddg")
            with r5b:
                pipe_pg_yahoo = st.number_input("Yahoo pages", min_value=1, max_value=100, value=5, key="pipe_pgy")
                pipe_pg_google = st.number_input("Google pages", min_value=1, max_value=100, value=5, key="pipe_pgg")

        pipe_captcha_ms = st.number_input(
            "CAPTCHA max wait (ms) for Google/Bing if needed",
            min_value=60_000,
            max_value=1_800_000,
            value=600_000,
            step=60_000,
            key="pipe_cap_ms",
        )

        st.divider()
        st.subheader("Decoupled run (subprocess)")
        flp_b1, flp_b2, flp_b3 = st.columns(3)
        with flp_b1:
            flp_start_collect = st.button("Start Domain Collection", type="primary", key="flp_sc")
        with flp_b2:
            flp_stop_collect = st.button("Stop Collection", key="flp_xc")
        with flp_b3:
            flp_start_crawl = st.button("Start Email Extraction", type="primary", key="flp_sl")
        st.caption(
            f"Stop writes `{stop_collection_request_path().name}` under the data folder for a graceful exit "
            "(same mechanism as tab 3 Stop collect). Domains append with flush after each line."
        )

        if flp_start_collect and st.session_state.flp_collect_proc is None:
            clear_skip_engine_request()
            clear_stop_collection_request()
            lg = logs_dir() / "gui_pipeline_collect.log"
            logs_dir().mkdir(parents=True, exist_ok=True)
            lg.write_text("", encoding="utf-8")
            qlist = build_playwright_queries(
                pipe_kw,
                pipe_country.strip() or "USA",
                pipe_state.strip(),
                b2b_enrich=bool(pipe_b2b),
            )
            fd, qpath = tempfile.mkstemp(suffix="_flp_queries.txt", text=True)
            os.close(fd)
            Path(qpath).write_text("\n".join(qlist), encoding="utf-8")
            st.session_state["_flp_queries_tmp"] = qpath
            cmd = [
                sys.executable,
                "-u",
                "-m",
                "email_scraper_project.playwright_cli",
                "collect",
                "--queries-file",
                qpath,
                "--results",
                str(int(st.session_state.pipe_mxnum)),
                "--per-engine-max",
                str(int(pipe_pem)),
                "--bing-pages",
                str(int(pipe_pg_bing)),
                "--ddg-pages",
                str(int(pipe_pg_ddg)),
                "--yahoo-pages",
                str(int(pipe_pg_yahoo)),
                "--google-pages",
                str(int(pipe_pg_google)),
                "--captcha-mode",
                "wait",
                "--captcha-wait-ms",
                str(int(pipe_captcha_ms)),
            ]
            if pipe_unlimited:
                cmd.append("--unlimited")
            if not pipe_bing:
                cmd.append("--no-bing")
            if not pipe_ddg:
                cmd.append("--no-ddg")
            if not pipe_yahoo:
                cmd.append("--no-yahoo")
            if pipe_google:
                cmd.append("--google")
            if not pipe_append_dom:
                cmd.append("--no-append")
            env = _env_without_system_proxy()
            env["PYTHONUTF8"] = "1"
            env["PYTHONUNBUFFERED"] = "1"
            env["LEADGEN_CAPTCHA_MODE"] = "wait"
            if pipe_png:
                env["LEADGEN_SERP_SCREENSHOT"] = "1"
            else:
                env.pop("LEADGEN_SERP_SCREENSHOT", None)
            if pipe_headless:
                env["LEADGEN_PLAYWRIGHT_HEADLESS"] = "1"
            else:
                env.pop("LEADGEN_PLAYWRIGHT_HEADLESS", None)
            fp = open(lg, "w", encoding="utf-8")
            st.session_state.flp_collect_proc = subprocess.Popen(
                cmd,
                stdout=fp,
                stderr=subprocess.STDOUT,
                env=env,
                **_popen_kwargs(),
            )
            st.session_state.flp_collect_log = lg
            st.session_state.flp_collect_log_fp = fp
            st.session_state.flp_collect_started = time.time()
            st.rerun()

        if flp_stop_collect and st.session_state.flp_collect_proc is not None:
            request_stop_collection()
            st.rerun()

        flc = st.session_state.flp_collect_proc
        if flc is not None:
            pol = flc.poll()
            if pol is None:
                st.progress(0.4, text="Pipeline domain collection (subprocess)…")
                cm1, cm2, cm3 = st.columns(3)
                with cm1:
                    st.metric("domains.txt data lines", count_nonempty_lines(domains_path()))
                with cm2:
                    st.metric("emails.txt data lines", count_nonempty_lines(emails_txt_path()))
                with cm3:
                    t0 = float(st.session_state.get("flp_collect_started") or time.time())
                    st.metric("Collection runtime (s)", max(0, int(time.time() - t0)))
                plg = st.session_state.flp_collect_log
                if plg:
                    st.code(_tail_text(Path(plg)))
                time.sleep(0.45)
                st.rerun()
            else:
                fp = st.session_state.pop("flp_collect_log_fp", None)
                if fp:
                    try:
                        fp.close()
                    except Exception:
                        pass
                st.session_state.flp_collect_proc = None
                st.session_state.flp_collect_started = None
                plg = st.session_state.flp_collect_log
                if plg:
                    st.code(_tail_text(Path(plg)))
                if pol == 0:
                    st.success("Domain collection subprocess finished.")
                else:
                    st.error(f"Domain collection exited with code {pol}")
                qtmp = st.session_state.pop("_flp_queries_tmp", None)
                if qtmp:
                    try:
                        os.unlink(qtmp)
                    except OSError:
                        pass

        if flp_start_crawl and st.session_state.flp_crawl_proc is None:
            lg = logs_dir() / "gui_pipeline_crawl.log"
            logs_dir().mkdir(parents=True, exist_ok=True)
            lg.write_text("", encoding="utf-8")
            cmd = [
                sys.executable,
                "-u",
                "-m",
                "email_scraper_project.playwright_cli",
                "crawl",
                "--workers",
                str(int(pipe_workers)),
            ]
            if not pipe_append_em:
                cmd.append("--no-append")
            if not pipe_pw_headless_fb:
                cmd.append("--fallback-headful")
            env = _env_without_system_proxy()
            env["PYTHONUTF8"] = "1"
            env["PYTHONUNBUFFERED"] = "1"
            if pipe_headless:
                env["LEADGEN_PLAYWRIGHT_HEADLESS"] = "1"
            else:
                env.pop("LEADGEN_PLAYWRIGHT_HEADLESS", None)
            fpw = open(lg, "w", encoding="utf-8")
            st.session_state.flp_crawl_proc = subprocess.Popen(
                cmd,
                stdout=fpw,
                stderr=subprocess.STDOUT,
                env=env,
                **_popen_kwargs(),
            )
            st.session_state.flp_crawl_log = lg
            st.session_state.flp_crawl_log_fp = fpw
            st.session_state.flp_crawl_started = time.time()
            st.rerun()

        flr = st.session_state.flp_crawl_proc
        if flr is not None:
            pol = flr.poll()
            if pol is None:
                st.progress(0.65, text="Pipeline email extraction (subprocess)…")
                em1, em2, em3 = st.columns(3)
                with em1:
                    st.metric("domains.txt data lines", count_nonempty_lines(domains_path()))
                with em2:
                    st.metric("emails.txt data lines", count_nonempty_lines(emails_txt_path()))
                with em3:
                    t0 = float(st.session_state.get("flp_crawl_started") or time.time())
                    st.metric("Extraction runtime (s)", max(0, int(time.time() - t0)))
                clg = st.session_state.flp_crawl_log
                if clg:
                    st.code(_tail_text(Path(clg)))
                time.sleep(0.5)
                st.rerun()
            else:
                fpw = st.session_state.pop("flp_crawl_log_fp", None)
                if fpw:
                    try:
                        fpw.close()
                    except Exception:
                        pass
                st.session_state.flp_crawl_proc = None
                st.session_state.flp_crawl_started = None
                clg = st.session_state.flp_crawl_log
                if clg:
                    st.code(_tail_text(Path(clg)))
                if pol == 0:
                    st.success("Email extraction subprocess finished (emails.txt updated).")
                else:
                    st.error(f"Email extraction exited with code {pol}")

        run_pipe = st.button("Run Full Pipeline", type="primary", key="pipe_run")

        if run_pipe:
            status_ph = st.empty()
            prog = st.progress(0, text="Starting…")

            def _pipe_progress(msg: str, pct: float | None) -> None:
                status_ph.markdown(f"**{msg}**")
                if pct is not None:
                    prog.progress(min(1.0, max(0.0, float(pct))), text=msg[:80])

            cfg = FullPipelineConfig(
                keywords=pipe_kw,
                country=pipe_country.strip() or "USA",
                state=pipe_state.strip(),
                selected_industries=tuple(pipe_industries),
                strict_industry_filter=bool(pipe_strict),
                max_domains=int(st.session_state.pipe_mxnum),
                unlimited_domains=bool(pipe_unlimited),
                per_engine_max=int(pipe_pem),
                serp_bing=int(pipe_pg_bing),
                serp_ddg=int(pipe_pg_ddg),
                serp_yahoo=int(pipe_pg_yahoo),
                serp_google=int(pipe_pg_google),
                email_workers=int(pipe_workers),
                use_bing=pipe_bing,
                use_ddg=pipe_ddg,
                use_yahoo=pipe_yahoo,
                use_google=pipe_google,
                b2b_query_expansion=pipe_b2b,
                append_domains=pipe_append_dom,
                append_emails=pipe_append_em,
                headless_browser=pipe_headless,
                captcha_mode="wait",
                captcha_wait_ms=int(pipe_captcha_ms),
                headless_playwright_email_fallback=pipe_pw_headless_fb,
                use_openai_summaries=pipe_openai,
                serp_screenshots=pipe_png,
            )
            try:
                st.session_state.pipeline_last = run_full_pipeline(cfg, progress=_pipe_progress)
            except Exception as e:
                st.session_state.pipeline_last = None
                st.error(f"Pipeline crashed: {e}")
            finally:
                prog.empty()
                status_ph.empty()

            res = st.session_state.pipeline_last
            if res is not None:
                if res.errors:
                    for err in res.errors:
                        st.error(err)
                else:
                    st.success("Pipeline finished successfully.")

        pl = st.session_state.pipeline_last
        if pl is not None:
            st.divider()
            st.subheader("Last run summary")
            if getattr(pl, "keyword_detected_industries", None):
                st.write(
                    "**Detected industries from this run’s keywords:** "
                    + ", ".join(pl.keyword_detected_industries)
                )
            m1, m2, m3, m4 = st.columns(4)
            m1.metric("Domains (lines in domains.txt)", pl.domains_line_count)
            m2.metric("Emails (data lines in emails.txt)", pl.emails_line_count)
            m3.metric("Qualified leads", pl.qualified_count)
            m4.metric("High quality (score > 6)", pl.high_quality_count)
            if pl.collect_stats:
                st.caption(
                    f"Collect: +{pl.collect_stats.get('new_domains', 0)} new domains written | "
                    f"queries: {pl.collect_stats.get('queries_run', 0)}"
                )
            if pl.crawl_stats:
                st.caption(
                    f"Crawl: +{pl.crawl_stats.get('emails_new', 0)} new emails | "
                    f"domains processed: {pl.crawl_stats.get('domains', 0)}"
                )
            if pl.errors:
                st.warning("Completed with issues; see errors above.")

        st.divider()
        st.subheader("Downloads")
        dq1, dq2 = st.columns(2)
        q_csv = qualified_leads_csv_path()
        o_csv = outreach_ready_csv_path()
        if q_csv.exists():
            dq1.download_button(
                "Download qualified_leads.csv",
                q_csv.read_bytes(),
                "qualified_leads.csv",
                key="pipe_dl_ql",
            )
        else:
            dq1.caption("Run the pipeline to create qualified_leads.csv")
        if o_csv.exists():
            dq2.download_button(
                "Download outreach_ready.csv",
                o_csv.read_bytes(),
                "outreach_ready.csv",
                key="pipe_dl_or",
            )
        else:
            dq2.caption("Run the pipeline to create outreach_ready.csv")

    with c6:
        st.subheader("Manual SERP Extractor (High-Quality Mode)")
        st.caption(
            "Paste search-result snippets, Bing/DDG/Yahoo copy, or any text containing URLs and emails. "
            "Optional file upload merges with the text area. Outputs `manual_leads.csv` and optionally "
            "`manual_qualified_leads.csv` in your data folder."
        )

        serp_paste = st.text_area(
            "Paste SERP results / copied content",
            height=300,
            key="serp_paste",
            placeholder="Paste HTML or plain text from search results, SERPs, or email lists…",
        )
        serp_file = st.file_uploader(
            "Or upload a .txt / .csv file (optional)",
            type=["txt", "csv"],
            key="serp_file",
        )

        o1, o2 = st.columns(2)
        with o1:
            opt_fix = st.checkbox("Fix broken / obfuscated emails", value=True, key="serp_fix")
            opt_crawl = st.checkbox("Crawl domain if email missing (/contact, /about)", value=True, key="serp_crawl")
            opt_qual = st.checkbox("Run lead qualification", value=True, key="serp_qual")
        with o2:
            opt_low = st.checkbox("Remove low-intent emails (noreply, support, …)", value=True, key="serp_low")
            opt_company = st.checkbox("Extract company names", value=True, key="serp_co")

        if "manual_serp_last" not in st.session_state:
            st.session_state.manual_serp_last = None

        run_serp = st.button("Extract & Enrich Leads", type="primary", key="serp_run")

        if run_serp:
            parts: list[str] = []
            if serp_paste and str(serp_paste).strip():
                parts.append(str(serp_paste))
            if serp_file is not None:
                try:
                    raw_u = serp_file.read()
                    parts.append(raw_u.decode("utf-8", errors="replace"))
                except Exception as e:
                    st.warning(f"Could not read uploaded file: {e}")
            combined = "\n\n".join(parts).strip()
            if not combined:
                st.warning("Add pasted text or upload a file.")
            else:
                with st.spinner("Processing manual SERP…"):
                    try:
                        res = run_manual_serp(
                            combined,
                            fix_broken_emails=opt_fix,
                            crawl_if_no_email=opt_crawl,
                            run_qualification=opt_qual,
                            remove_low_intent=opt_low,
                            extract_companies=opt_company,
                        )
                        st.session_state.manual_serp_last = res
                        mlp = manual_leads_csv_path()
                        write_manual_leads_csv(res.manual_leads, mlp)
                        mq = manual_qualified_leads_csv_path()
                        if opt_qual:
                            write_manual_qualified_csv(res.qualified_leads, mq)
                        elif mq.exists():
                            try:
                                mq.unlink()
                            except OSError:
                                pass
                        if res.errors:
                            for err in res.errors:
                                st.error(err)
                        else:
                            st.success("Done. CSVs written.")
                    except Exception as e:
                        st.session_state.manual_serp_last = None
                        st.error(f"Processing failed: {e}")

        ms = st.session_state.manual_serp_last
        if ms is not None:
            st.divider()
            m1, m2, m3, m4, m5 = st.columns(5)
            m1.metric("Emails extracted (raw)", ms.raw_emails_found)
            m2.metric("Obfuscation fixes", ms.fixed_obfuscation_count)
            m3.metric("Domains crawled (emails found)", ms.crawled_domains_count)
            m4.metric("Final lead rows", ms.final_lead_rows)
            m5.metric("Qualified rows", ms.qualified_rows)
            if ms.manual_leads:
                st.dataframe(ms.manual_leads[:200], use_container_width=True, hide_index=True)
            dl1, dl2 = st.columns(2)
            p1 = manual_leads_csv_path()
            p2 = manual_qualified_leads_csv_path()
            if p1.exists():
                dl1.download_button(
                    "Download manual_leads.csv",
                    p1.read_bytes(),
                    "manual_leads.csv",
                    key="dl_mserp1",
                )
            if p2.exists():
                dl2.download_button(
                    "Download manual_qualified_leads.csv",
                    p2.read_bytes(),
                    "manual_qualified_leads.csv",
                    key="dl_mserp2",
                )


if __name__ == "__main__":
    main()
