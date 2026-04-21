# Email Scraper Project

Automated lead generation pipeline that:

1. collects business domains from search engines, then
2. crawls those domains to extract emails, then
3. optionally qualifies and scores leads (B2B-focused filtering and exports).

The project includes a Streamlit dashboard and CLI workflows.

---

## What This Project Does

- Builds search queries from your keywords + region inputs (optional B2B-style query expansion).
- Drops many directory, aggregator, and news hosts at collection time.
- Collects domains from multiple engines (Bing / DuckDuckGo / Yahoo / optional Google).
- Saves domains to `domains.txt`.
- Crawls discovered domains and extracts emails (contact / about / team / services pages first in the threaded crawler).
- Saves emails to `emails.txt` (Playwright threaded crawler) or `emails.csv` (Scrapy mode).
- Qualifies leads into `qualified_leads.csv` and optional `outreach_ready.csv` (tabs 4–5).
- Writes detailed runtime logs for troubleshooting.

---

## Project Structure

- `run_dashboard.py`  
  Launcher for the Streamlit UI.

- `email_scraper_project/gui/app.py`  
  Main dashboard app (5 tabs: HTTP collect, Scrapy crawl, Playwright + emails.txt, AI Lead Intelligence, Full Lead Pipeline).

- `email_scraper_project/lead_qualifier.py`  
  Domain classification, email filtering, keyword→industry inference, multi-industry scoring, strict optional filtering, and CSV writers (tabs 4–5, collection hooks).

- `email_scraper_project/pipeline_runner.py`  
  One-click orchestration: queries → Playwright collect → threaded email crawl → qualification (used by tab 5).

- `email_scraper_project/browser_search/playwright_collector.py`  
  Browser-based search collector (engine flow, pagination, CAPTCHA waiting, skip engine signal). On Windows, applies a Proactor-friendly asyncio policy when needed so Playwright can spawn Chromium from Streamlit.

- `email_scraper_project/playwright_cli.py`  
  CLI interface for Playwright collect/crawl/all.

- `email_scraper_project/email_txt_crawler/threaded_crawler.py`  
  Threaded email crawler for `emails.txt` with Playwright fallback on blocked domains.

- `email_scraper_project/spiders/`  
  Scrapy-based domain and email crawling flow.

- `email_scraper_project/config.py`  
  Central path configuration (`domains.txt`, `emails.txt`, `logs.txt`, etc.).

---

## Data & Output Files

Default output folder is project root (override with `LEADGEN_DATA_DIR`):

- `domains.txt` -> collected domains
- `emails.txt` -> threaded crawler email output (tab 3 / CLI crawl)
- `emails.csv` -> Scrapy email output (tab 2)
- `leads.json` -> optional JSON lines export
- `logs.txt` -> global aggregated log
- `logs/gui_playwright_collect.log` -> Playwright collector log
- `logs/gui_playwright_crawl.log` -> Playwright crawl log
- `logs/gui_collect.log` -> HTTP collect tab log
- `logs/gui_scrapy.log` -> Scrapy tab log
- `logs/domain_collection.jsonl` -> structured domain collection events
- `logs/serp_screenshots/` -> optional SERP screenshots
- `qualified_leads.csv` -> scored leads (domain, email, industry, classification, score, contact URL, notes, **detected_industry**, **matched_industry** `true`/`false`)
- `outreach_ready.csv` -> optional export (name guess, email, company guess, industry, pitch angle)

---

## Installation

From project root:

```bash
pip install -r requirements.txt
python -m playwright install chromium
```

If you run Scrapy paths, make sure Scrapy dependencies are installed from `requirements.txt`.

---

## Running the Dashboard

```bash
python run_dashboard.py
```

Open the local Streamlit URL shown in terminal.

---

## Dashboard Modes (5 Methods)

## 1) HTTP collect

Purpose: faster, lighter domain collection via HTTP requests + parsing.

How it operates:
- Builds query set from keyword/country/state/city/industry.
- Optional **B2B-style search queries**: adds phrases such as `{keyword} company {location}` and `{keyword} services {location}` (CLI: `--b2b-queries` on `collect_domains`).
- Calls engines in HTTP mode.
- Normalizes URLs and writes unique domains to `domains.txt`.
- Logs to `logs/gui_collect.log`.

Use when:
- You want fast collection and engines are not strongly blocking.

Limitations:
- More block-prone than browser automation.

---

## 2) Scrapy crawl

Purpose: crawl domain pages and export structured email data.

How it operates:
- Reads `domains.txt`.
- Runs Scrapy spider (`email_spider`) with configurable max pages / early stop.
- Writes:
  - `emails.csv`
  - optional `leads.json`
- Logs to `logs/gui_scrapy.log`.

Use when:
- You need CSV/JSON output and broader spider-style crawling.

Limitations:
- Some sites block HTTP crawlers or hide content behind JS.

---

## 3) Playwright + emails.txt (Recommended)

Purpose: highest reliability with browser search + threaded email extraction.

How it operates:

### A. Browser domain collection
- Opens Chromium.
- Uses homepage + search box flow for each enabled engine.
- Navigates SERPs and extracts domains from links.
- Handles redirect decoding (Bing/Yahoo).
- Optional Google (can trigger CAPTCHA).
- Writes domains to `domains.txt`.
- Logs to `logs/gui_playwright_collect.log` and `logs.txt`.

### B. Threaded email crawl
- Reads `domains.txt`.
- Tries HTTP crawl first using thread pool.
- If HTTP blocked/empty for domain, queues Playwright fallback.
- Deduplicates globally and appends to `emails.txt`.
- Logs to `logs/gui_playwright_crawl.log` and `logs.txt`.

Use when:
- You want best real-world results with fallback behavior.

---

## 4) AI Lead Intelligence

Purpose: turn raw `emails.txt` / `domains.txt` into scored B2B-style leads (with optional strict filtering).

How it operates:

- **Input**: choose `emails.txt` (recommended, uses email + domain + source URL from the crawl) or `domains.txt` (homepage-only signals; uses a synthetic `info@` row per domain for filtering/scoring).
- **Target industries**: multi-select (construction, HVAC, logistics, warehouse, manufacturing, repair). Used for **scoring** by default.
- **Strict industry filtering** (optional): when enabled *and* at least one industry is selected, only leads whose **homepage-detected** industry is in that set are kept. When off, non-matching verticals are **not** dropped by default; they may get lower scores instead.
- **Campaign keywords** (optional): free-text hints (comma/line-separated). The tool infers industries from phrases (e.g. “construction company” → construction, “warehouse company” → logistics) and uses that for **keyword–industry scoring** alongside homepage detection.
- **Domain classification**: hostname + homepage fetch (title + text), and keyword rules (`business`, `saas/tool`, `media/blog`, `directory/listing`, `government`, `unknown`).
- **Email filtering**: removes obvious test/fake locals and common low-intent addresses (`noreply`, `support`, `newsletter`, `press`, etc.); keeps `info`, `contact`, `sales`, `hello`, and simple name-like locals.
- **Industry on page**: homepage text is matched to the same verticals as above.
- **Scoring**: base signals (services copy, HTML heuristics, process/SMB/enterprise hints, contact URL, classification) plus industry alignment: boosts when the page industry matches your selections and/or keyword-inferred industries; a small penalty when the page industry is outside your campaign selections/inference (empty homepage industry is not penalized).
- **Output**: writes `qualified_leads.csv` (including **detected_industry** and **matched_industry**) and `outreach_ready.csv`. **Clean & Qualify Current List** reads `emails.txt` and refreshes those CSVs.
- **UI filters** (table preview): minimum score, business-only classification, industry filter, optional dedupe by domain + email.
- **Optional AI summaries**: if `OPENAI_API_KEY` is set, enable **Request AI summaries**; one-line summaries are merged into the **notes** column.

Use when:
- You care more about lead quality than raw email count, including **mixed-keyword / multi-vertical** campaigns.

---

## 5) Full Lead Pipeline (One Click)

Purpose: single run from keywords through to qualified CSVs (no manual tab 3 then tab 4).

How it operates:

- **Inputs**: multi-line keywords, country, **multi-value states** (comma/semicolon/newline), optional **multi-select industries**, optional **strict industry filtering**, B2B query expansion, caps (max domains, per-engine max, SERP pages), email crawl threads, engine toggles, headless/captcha options.
- **Step 1**: builds Playwright queries (with B2B expansion when enabled). Shows **detected industries inferred from keywords** in the UI.
- **Step 2**: runs **Playwright domain collection** in-process (same collector as tab 3). Writes `domains.txt` with existing directory/host filtering.
- **Step 3**: runs the **threaded email crawler** → `emails.txt`.
- **Step 4**: runs **lead qualification** on `emails.txt` with the same industry/scoring rules as tab 4 (strict + selections from this tab).
- **Step 5**: UI shows domain/email/qualified counts, high-quality count (score > 6), last-run summary, and download buttons for `qualified_leads.csv` / `outreach_ready.csv`.

**Note:** Do not start tab 3 collect/crawl at the same time as a full-pipeline run. On **Windows**, the collector uses an asyncio policy compatible with spawning Chromium from Streamlit; if you ever run Playwright-only code elsewhere in a threaded host, use the same pattern as `playwright_collector.windows_playwright_asyncio_guard`.

Use when:
- You want an end-to-end **one-click** lead run from the dashboard.

---

## Playwright Tab Options (What Each Setting Does)

- **B2B-style browser queries**  
  When enabled, expands each keyword × region with extra searches (for example `{keyword} company {region}`). When you use a queries file from the dashboard, expansion is applied when that file is generated (not double-applied in the subprocess).

- **Keywords**  
  Comma/semicolon/newline list. Each item becomes part of generated queries.

- **Countries / regions**  
  Multi-value region input; used in query construction.

- **States / provinces**  
  Optional refiners for query expansion.

- **Max total unique domains**  
  Hard stop when this many unique domains are collected in current run.

- **Max new domains per engine, per query**  
  Per-engine cap before moving to next engine.

- **Engine toggles** (`Bing`, `DuckDuckGo`, `Yahoo`, `Google`)  
  Enable/disable each engine.

- **SERP pages per engine**  
  Max pages to visit for each engine per query.

- **Headless browser**  
  Runs browser without window. Usually less stable for anti-bot heavy engines.

- **Append domains.txt**  
  If off, current run overwrites domain output.

- **Email crawl threads**  
  Worker threads for email extraction phase.

- **Append emails.txt**  
  If off, crawler starts fresh output.

- **Headful Playwright fallback when HTTP is blocked**  
  Uses visible browser for fallback fetches.

- **Save full-page SERP screenshots**  
  Saves screenshots to `logs/serp_screenshots/` (debugging).

- **Skip engine buttons (while collecting)**  
  Writes `leadgen_skip_engine.txt` signal to skip active/next engine.

- **Clear all outputs + logs (sidebar)**  
  Stops running jobs and removes generated outputs/logs (including `qualified_leads.csv` and `outreach_ready.csv` when present).

---

## CLI Usage

From project root:

```bash
python -m email_scraper_project.playwright_cli collect --keyword "motor rewinding" --country USA --results 120
python -m email_scraper_project.playwright_cli crawl --workers 8
python -m email_scraper_project.playwright_cli all --keyword "electrical contractors" --country USA --results 100
```

### Important CLI flags

- `collect`
  - `--keyword`
  - `--country`
  - `--states`
  - `--queries-file`
  - `--results`
  - `--per-engine-max`
  - `--engine-order`
  - `--no-bing`, `--no-ddg`, `--no-yahoo`, `--google`
  - `--bing-pages`, `--ddg-pages`, `--yahoo-pages`, `--google-pages`
  - `--no-append`
  - `--captcha-mode stdin|wait`
  - `--captcha-wait-ms`
  - `--b2b-queries` (adds company/services style queries when building queries from `--keyword` / `--country` / `--states`; ignored if `--queries-file` is used)

- `crawl`
  - `--workers`
  - `--no-append`
  - `--fallback-headful`

- `all`  
  Runs collect then crawl in one command (supports `--b2b-queries` on the collect phase the same way as `collect`).

HTTP domain collection (separate CLI):

```bash
python -m email_scraper_project.spiders.collect_domains --keywords "hvac,warehouse" --country USA --state Texas --pages 3 --b2b-queries
```

---

## Environment Variables

- `LEADGEN_DATA_DIR`  
  Override output/log directory.

- `LEADGEN_PLAYWRIGHT_HEADLESS=1`  
  Force headless browser.

- `LEADGEN_CAPTCHA_MODE=wait|stdin`  
  CAPTCHA handling mode.

- `LEADGEN_SERP_SCREENSHOT=1`  
  Save full-page SERP screenshots.

- `LEADGEN_PW_USER_DATA_DIR=/path/to/profile`  
  Persistent browser profile (helps with consent/session continuity).

- `LEADGEN_PW_CHANNEL=chrome|msedge`  
  Use installed browser channel.

- `OPENAI_API_KEY`  
  Optional. Enables one-line AI summaries in **AI Lead Intelligence** when the dashboard checkbox is on; summaries are merged into the `qualified_leads.csv` **notes** field.

---

## Logging & Troubleshooting

If something looks wrong, check logs in this order:

1. `logs/gui_playwright_collect.log` (collector details)
2. `logs/gui_playwright_crawl.log` (email crawl + fallback details)
3. `logs/domain_collection.jsonl` (structured query/engine events)
4. `logs.txt` (global combined context)

Common situations:

- **Google CAPTCHA loops**  
  Use non-headless mode, enable longer wait, solve manually in browser, optionally disable Google for bulk runs.

- **Low DDG results**  
  DDG can return block/empty pages for some IPs; Bing/Yahoo usually carry collection.

- **`emails.txt` not populated quickly**  
  Collection and crawl are separate steps; run `Collect` first, then `Crawl` (or use tab 5 for both).

- **Playwright `NotImplementedError` / subprocess on Windows (Streamlit)**  
  The collector applies a Windows **Proactor** asyncio policy around Playwright so the browser subprocess can start. If you embed Playwright elsewhere, reuse `windows_playwright_asyncio_guard` from `playwright_collector.py`.

---

## Recommended Workflow

**Option A — One click**

1. Run dashboard.
2. Open tab **5) Full Lead Pipeline**: set keywords, regions, industries, and **Run Full Pipeline**.
3. Download `qualified_leads.csv` / `outreach_ready.csv` when finished.

**Option B — Step by step**

1. Run dashboard.
2. Use tab 3 (Playwright + emails.txt) for reliable collection.
3. Leave **B2B-style browser queries** on (or use **B2B-style search queries** on tab 1 for HTTP collect) for higher-intent search phrases.
4. Collect domains with Bing/DDG/Yahoo first (Google optional).
5. Start `Crawl → emails.txt`.
6. Open tab **4) AI Lead Intelligence**: set industries / keyword hints / strict mode as needed; run qualification or **Clean & Qualify Current List**; download CSVs.
7. Review backend logs in dashboard.
8. Download outputs (`domains.txt`, `emails.txt`, `logs.txt`, CSVs as needed).

---

## Notes

- This project relies on public search pages and may be affected by anti-bot changes.
- Query quality strongly impacts lead quality; B2B-style expansion, tab 4/5 qualification, and multi-industry keyword hints are meant to improve signal over raw scraping.
- Classification and scoring are heuristic (not a guarantee of fit); review exports before outreach.
- **Mixed campaigns**: use multi-select industries + keyword hints with **strict** off unless you intentionally want to drop every lead outside the selected verticals.
- Respect website terms and applicable laws for data collection.

