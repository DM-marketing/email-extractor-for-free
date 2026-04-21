# Lead Generation Flow (All 3 Methods)

This file explains how each method in the dashboard works end-to-end.

## 1) HTTP collect

Purpose: collect business domains quickly using HTTP requests (lighter/faster path).

Flow:
1. User enters keywords/country (optional state/city/industry).
2. Dashboard starts `email_scraper_project.spiders.collect_domains`.
3. Collector builds multiple search queries from inputs.
4. For each query, it calls enabled engines (HTTP requests + parsing).
5. Candidate URLs are normalized and cleaned to root domains.
6. New domains are appended to `domains.txt` (or overwrite if selected).
7. Runtime logs are written to `logs/gui_collect.log`.
8. Structured events are written to `logs/domain_collection.jsonl` (if available in this path).

Outputs:
- `domains.txt`
- `logs/gui_collect.log`
- `logs/domain_collection.jsonl` (event stream)

Best for:
- Fast domain discovery when engines are not heavily blocking.

Limitations:
- More likely to be blocked than browser automation.

---

## 2) Scrapy crawl

Purpose: crawl domains and extract emails into CSV/JSON outputs.

Flow:
1. Reads `domains.txt`.
2. Starts Scrapy spider `email_spider`.
3. Crawls pages per domain up to configured limits.
4. Extracts emails from page text/links.
5. Writes results to:
   - `emails.csv`
   - `leads.json` (if enabled)
6. Logs go to `logs/gui_scrapy.log`.

Outputs:
- `emails.csv`
- `leads.json` (optional)
- `logs/gui_scrapy.log`

Best for:
- Structured output and broad crawl where Scrapy can fetch pages.

Limitations:
- Some sites block HTTP crawlers; misses JS-rendered pages.

---

## 3) Playwright + emails.txt

Purpose: high-resilience collection + threaded email extraction with browser fallback.

Flow A - Domain collection:
1. Dashboard builds query list from keywords/countries/states.
2. Starts: `python -m email_scraper_project.playwright_cli collect`.
3. Opens Chromium and runs enabled engines in order:
   - Bing
   - DuckDuckGo
   - Yahoo
   - Google (optional)
4. For each engine:
   - Open homepage
   - Type query in search box
   - Navigate SERP pages
   - Extract result links/domains
   - Handle redirects (Bing/Yahoo decoding logic)
5. CAPTCHA/consent handling:
   - Wait mode or manual solve
   - Optional Skip Engine from dashboard via `leadgen_skip_engine.txt`
6. Writes discovered domains to `domains.txt`.
7. Logs to `logs/gui_playwright_collect.log` and `logs.txt`.
8. Optional screenshots to `logs/serp_screenshots/` when enabled.

Flow B - Email crawl:
1. Starts: `python -m email_scraper_project.playwright_cli crawl`.
2. Reads domains from `domains.txt`.
3. Threaded HTTP extraction attempts email collection.
4. If HTTP blocked/empty, queues Playwright fallback for that domain.
5. Deduplicates emails and writes to `emails.txt`.
6. Logs to `logs/gui_playwright_crawl.log` and `logs.txt`.

Outputs:
- `domains.txt`
- `emails.txt`
- `logs/gui_playwright_collect.log`
- `logs/gui_playwright_crawl.log`
- `logs.txt`
- `logs/serp_screenshots/` (optional)

Best for:
- Most reliable real-world mode with engine UI flow + fallback.

Limitations:
- Slower than HTTP mode; Google can still challenge with CAPTCHA.

---

## Dashboard log mapping

In Playwright tab, backend visibility should come from:
- `gui_playwright_collect.log` -> live domain collection details
- `gui_playwright_crawl.log` -> live threaded/fallback crawl details
- `domain_collection.jsonl` -> structured event timeline
- `logs.txt` -> global aggregate log

---

## Clear All button behavior

`Clear all outputs + logs` in sidebar:
1. Stops any running collect/crawl process (all tabs).
2. Closes active log file handles.
3. Removes temporary query file if present.
4. Deletes generated outputs:
   - `domains.txt`, `emails.txt`, `emails.csv`, `leads.json`
5. Deletes log files:
   - `logs.txt`
   - `logs/gui_collect.log`
   - `logs/gui_scrapy.log`
   - `logs/gui_playwright_collect.log`
   - `logs/gui_playwright_crawl.log`
   - `logs/domain_collection.jsonl`
6. Clears screenshot folder `logs/serp_screenshots/` if present.

