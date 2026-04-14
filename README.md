# Automated Lead Generation System (Scrapy + Playwright)
## free email extractor from sreach engine like (google, Bings, yahoo and DDG (DuckDuckGo)), 100% free

A modular, free, end-to-end lead generation project that:

- collects business domains from multiple search engines,
- crawls domains for emails,
- exports clean outputs,
- provides a Streamlit dashboard for non-technical usage.

This project keeps your original Scrapy workflow and adds a Playwright-based browser pipeline for stronger search extraction and anti-bot resilience.

## Features

- Multi-engine domain collection:
  - Bing
  - DuckDuckGo (HTML)
  - Yahoo
  - Google (optional/manual CAPTCHA handling)
- Multi-input query generation:
  - multiple keywords
  - multiple countries
  - optional states/provinces
- Per-engine controls:
  - enable/disable each engine
  - custom page limits per engine (SERP page count)
  - per-engine domain cap per query
- Email extraction:
  - regex extraction
  - `mailto:` extraction
  - obfuscation handling in Scrapy path
- Crawling outputs:
  - `domains.txt`
  - `emails.csv` (Scrapy path)
  - `emails.txt` (threaded crawler path)
  - `logs.txt` + JSON logs
- GUI dashboard (Streamlit):
  - Start/Stop controls
  - live logs
  - output preview and download

## Tech Stack

- Python
- Scrapy
- Playwright (Chromium automation)
- Requests + BeautifulSoup
- Streamlit

## Project Structure

```
email_scraper_project/
├─ email_scraper_project/
│  ├─ spiders/
│  │  ├─ collect_domains.py
│  │  └─ email_spider.py
│  ├─ browser_search/
│  │  ├─ playwright_collector.py
│  │  ├─ bing_url_decode.py
│  │  └─ query_builder.py
│  ├─ email_txt_crawler/
│  │  ├─ threaded_crawler.py
│  │  └─ extract.py
│  ├─ gui/
│  │  └─ app.py
│  ├─ domain_cleaner/
│  ├─ search_engine/
│  ├─ proxy_manager/
│  ├─ email_extractor/
│  ├─ crawler/
│  ├─ config.py
│  ├─ logging_config.py
│  └─ playwright_cli.py
├─ run_dashboard.py
├─ requirements.txt
├─ scrapy.cfg
├─ domains.txt
├─ emails.csv
├─ emails.txt
└─ logs.txt
```

## Installation

From project root (`email_scraper_project`):

```bash
python -m pip install -r requirements.txt
python -m playwright install chromium
```

Notes:

- On Windows, use `python -m playwright install chromium` (not `playwright install chromium`) if Playwright CLI is not on PATH.
- For GUI usage, keep the same Python environment for install and run.

## Quick Start

### 1) Run Dashboard

```bash
python run_dashboard.py
```

Open the Streamlit URL shown in terminal.

Tabs:

- `1) HTTP collect` - requests-based domain collection
- `2) Scrapy crawl` - Scrapy email crawler (`emails.csv`)
- `3) Playwright + emails.txt` - browser collection + threaded email crawl (`emails.txt`)

### 2) Run Playwright CLI (recommended for robust search)

Collect domains:

```bash
python -m email_scraper_project.playwright_cli collect --keyword "motor rewinding services" --country USA --results 80
```

Crawl domains to `emails.txt`:

```bash
python -m email_scraper_project.playwright_cli crawl --workers 8
```

Run both:

```bash
python -m email_scraper_project.playwright_cli all --keyword "dentist" --country UK --results 50
```

## Advanced Playwright Usage

### Multi-country/state/keyword

```bash
python -m email_scraper_project.playwright_cli collect \
  --keyword "motor rewinding services,electric motor repair" \
  --country "USA,UK" \
  --states "Texas,California" \
  --results 200
```

### Per-engine page limits (custom)

Example: Bing 1 page, Google 2, DDG 3, Yahoo 1:

```bash
python -m email_scraper_project.playwright_cli collect \
  --keyword "industrial supplier" \
  --country "USA" \
  --results 120 \
  --per-engine-max 40 \
  --bing-pages 1 \
  --google-pages 2 \
  --ddg-pages 3 \
  --yahoo-pages 1 \
  --google
```

### Engine selection

- Disable Bing: `--no-bing`
- Disable DuckDuckGo: `--no-ddg`
- Disable Yahoo: `--no-yahoo`
- Enable Google: `--google`

### Custom query file

One query per line:

```bash
python -m email_scraper_project.playwright_cli collect --queries-file queries.txt --results 300
```

## Outputs

Default output location is project root (same folder as `scrapy.cfg`):

- `domains.txt` - collected domains
- `emails.csv` - Scrapy spider output
- `emails.txt` - threaded crawler output
- `logs.txt` - main leadgen logs
- `logs/` - additional run-specific logs (`gui_*.log`, `domain_collection.jsonl`, etc.)

## Environment Variables

- `LEADGEN_DATA_DIR` - custom output directory
- `LEADGEN_PLAYWRIGHT_HEADLESS=1` - run Playwright without opening browser window
- `LEADGEN_USE_PROXIES=1` - optional free proxy usage (unreliable)
- `LEADGEN_JSON_EXPORT=0` - disable JSONL export from Scrapy pipeline

## Notes and Limitations

- Free search scraping can hit anti-bot checks/CAPTCHA; manual solve may be required.
- Google support is optional and may be slower due to manual verification.
- Free proxies are unstable; direct connection is generally more reliable.
- Respect target websites' terms of service and legal compliance requirements.

## License

Use your preferred license before publishing (for example, MIT).

Developed by https://www.linkedin.com/in/madhav-digitalmarketing/

