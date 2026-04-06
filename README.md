# 📧 Email Extractor (Scrapy + Search Engine Powered)

A powerful Python-based email extraction tool that:

1. **Finds business domains** using search engines (DuckDuckGo, Yahoo, Bing)
2. **Crawls websites** using Scrapy
3. **Extracts valid emails** efficiently

Perfect for **lead generation, outreach, and research**.

---

## 🚀 Features

* 🔍 Multi-source domain collection (DDG, Yahoo, Bing)
* 🌐 Smart website crawling with Scrapy
* 📧 Accurate email extraction using regex
* 🚫 Filters junk domains (Indeed, Yelp, etc.)
* ⚡ Priority crawling (contact, about pages)
* 🔁 Avoids duplicate emails
* 📁 CSV export (emails + source URL)
* 🧠 Optimized crawl limits (fast + efficient)

---

## 📁 Project Structure

```
email_scraper_project/
│
├── email_scraper_project/
│   ├── spiders/
│   │   ├── email_spider.py        # Main email crawler
│   │   ├── collect_domains.py     # Domain generator (search engines)
│   │   ├── __init__.py
│   ├── items.py
│   ├── pipelines.py
│   ├── settings.py
│
├── domains.txt                    # Input domains list
├── emails.csv                     # Output file
├── scrapy.cfg
```

---

## ⚙️ Installation

### 1. Clone Repository

```
cd email-extractor
```

### 2. Install Dependencies

```
python -m pip install scrapy requests beautifulsoup4
```

---

## 📌 How It Works

### Step 1: Collect Domains

Run the domain generator:

```
python email_scraper_project/spiders/collect_domains.py
```

👉 This will:

* Search business queries
* Extract real company websites
* Save them into `domains.txt`
<img width="1441" height="997" alt="image" src="https://github.com/user-attachments/assets/64e51888-6c6b-46db-b6a7-3012cb1f8bdf" />


**Note**: Dont forgot to change the state and the services (Keyword or sreach term as per your needs) inside the **email_scraper_project/spiders/collect_domains.py**
---

### Step 2: Run Email Scraper

Go to project root:

```
cd email_scraper_project
```

Run:

```
python -m scrapy crawl email_spider
```
<img width="1429" height="1000" alt="image" src="https://github.com/user-attachments/assets/4be69325-82c2-43f3-a84a-53f282a5c0ec" />

---

## 📤 Output

### `emails.csv`

```
email,source
info@company.com,https://company.com/contact
support@business.co.uk,https://business.co.uk
```


---

## 🔧 Configuration

### In `collect_domains.py`

* `SERVICES` → Change business types
* `STATES` → Change location targeting
* `BAD_DOMAINS` → Block unwanted sites
<img width="1884" height="1043" alt="image" src="https://github.com/user-attachments/assets/974687fb-3334-4af6-aec2-aad35cf9f5f8" />

---

### In `email_spider.py`

| Setting                | Description             |
| ---------------------- | ----------------------- |
| `MAX_PAGES_PER_DOMAIN` | Limit crawl depth       |
| `PRIORITY_KEYWORDS`    | Pages to prioritize     |
| `BAD_EXTENSIONS`       | Skip non-HTML resources |
| `RETRY_TIMES`          | Retry failed requests   |

---

## ⚠️ Important Notes

* Only extracts **publicly available emails**
* Some sites may block scraping (403 / 429)
* Results depend on website structure
* Use responsibly and follow legal guidelines

---

## ❌ Common Mistakes

### ❌ Running spider like this:

```
python email_spider.py
```

👉 This will NOT work

### ✅ Correct way:

```
python -m scrapy crawl email_spider
```

---

## 🧠 Workflow Summary

1. Generate domains → `collect_domains.py`
2. Store domains → `domains.txt`
3. Crawl websites → `email_spider.py`
4. Extract emails → `emails.csv`

---

## 🚀 Future Improvements

* Proxy rotation support
* Email validation (SMTP check)
* GUI dashboard
* API integration
* Multi-threading

---

## 🤝 Contributing

**Contributions are welcome!
Feel free to open issues or submit pull requests.**

---

## 📜 License

Free and open-source.

---

## ⭐ Support

If this helped you:

* Star the repo ⭐
* Share with others 🚀

---

## 👨‍💻 Author

Developed by **Madhav Khurana**
https://www.linkedin.com/in/madhav-digitalmarketing/
---
