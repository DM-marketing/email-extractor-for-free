"""
Lead generation dashboard (Streamlit): domain collection + email crawl + export preview.

Run from project root (folder containing scrapy.cfg):

    streamlit run email_scraper_project/gui/app.py
"""

from __future__ import annotations

import os
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
from email_scraper_project.config import (  # noqa: E402
    data_dir,
    domains_path,
    emails_csv_path,
    emails_txt_path,
    leads_json_path,
    logs_dir,
    main_log_txt_path,
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

    c1, c2, c3 = st.tabs(
        ["1) HTTP collect", "2) Scrapy crawl", "3) Playwright + emails.txt"]
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
        pw_ddg = st.checkbox("DuckDuckGo (HTML)", value=True, key="pw_ddg")
        pw_yahoo = st.checkbox("Yahoo", value=True, key="pw_yahoo")
        pw_google = st.checkbox(
            "Google (optional; solve CAPTCHA in browser if shown)", value=False, key="pw_g"
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
            lg = logs_dir() / "gui_playwright_collect.log"
            logs_dir().mkdir(parents=True, exist_ok=True)
            lg.write_text("", encoding="utf-8")
            qlist = build_playwright_queries(pkw, pcountry, pstates)
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
            try:
                st.session_state.pw_collect_proc.terminate()
            except Exception:
                pass
            st.session_state.pw_collect_proc = None
            fp = st.session_state.pop("pw_collect_log_fp", None)
            if fp:
                try:
                    fp.close()
                except Exception:
                    pass
            st.session_state.pw_msg = "Playwright collect stopped."
            qtmp = st.session_state.pop("_pw_queries_tmp", None)
            if qtmp:
                try:
                    os.unlink(qtmp)
                except OSError:
                    pass
            st.rerun()

        pwc = st.session_state.pw_collect_proc
        if pwc is not None:
            poll = pwc.poll()
            if poll is None:
                st.progress(0.45, text="Playwright collecting… (watch Chromium window)")
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

        st.text_area(
            "emails.txt (preview)",
            _preview_csv(emails_txt_path()),
            height=160,
            key="pw_preview_em",
        )
        st.text_area(
            "logs.txt (tail)",
            _tail_text(main_log_txt_path(), 8000),
            height=140,
            key="pw_preview_log",
        )
        d1, d2, d3 = st.columns(3)
        et = emails_txt_path()
        lt = main_log_txt_path()
        if et.exists():
            d1.download_button("Download emails.txt", et.read_bytes(), "emails.txt", key="dl_et")
        if lt.exists():
            d2.download_button("Download logs.txt", lt.read_bytes(), "logs.txt", key="dl_lt")
        dp = domains_path()
        if dp.exists():
            d3.download_button("Download domains.txt", dp.read_bytes(), "domains.txt", key="dl_dt")


if __name__ == "__main__":
    main()
