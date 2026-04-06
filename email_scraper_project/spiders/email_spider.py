import scrapy
import re
from urllib.parse import urlparse
import os


BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
file_path = os.path.join(BASE_DIR, "..", "domains.txt")


class EmailSpider(scrapy.Spider):
    name = "email_spider"

    custom_settings = {
        "ROBOTSTXT_OBEY": False,
        "USER_AGENT": "Mozilla/5.0",
        "DOWNLOAD_TIMEOUT": 10,
        "RETRY_TIMES": 1,
        "FEEDS": {
            "emails.csv": {
                "format": "csv",
                "overwrite": False
            }
        }
    }

    email_regex = r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}"

    BAD_EXTENSIONS = (
        ".png", ".jpg", ".jpeg", ".gif", ".webp",
        ".svg", ".css", ".js", ".ico"
    )

    PRIORITY_KEYWORDS = [
        "contact", "about", "support", "help", "company"
    ]

    collected_emails = set()
    visited_urls = set()

    MAX_PAGES_PER_DOMAIN = 10  # 🔥 reduce crawl

    domain_counts = {}

    # ---------------------------
    # LOAD DOMAINS
    # ---------------------------
    def start_requests(self):
        with open("domains.txt", "r") as f:
            for line in f:
                url = line.strip()

                if not url.startswith("http"):
                    url = "http://" + url

                domain = urlparse(url).netloc
                self.domain_counts[domain] = 0

                yield scrapy.Request(
                    url,
                    callback=self.parse,
                    errback=self.handle_error,
                    dont_filter=True
                )

    # ---------------------------
    # ERROR HANDLING
    # ---------------------------
    def handle_error(self, failure):
        self.logger.info(f"Skipping failed: {failure.request.url}")

    # ---------------------------
    # MAIN PARSER
    # ---------------------------
    def parse(self, response):

        if response.status in [403, 404, 429]:
            return

        domain = urlparse(response.url).netloc

        if self.domain_counts.get(domain, 0) >= self.MAX_PAGES_PER_DOMAIN:
            return

        self.domain_counts[domain] += 1
        self.visited_urls.add(response.url)

        # ---------------------------
        # EMAIL EXTRACTION
        # ---------------------------
        text = response.text.lower()
        text = text.replace("[at]", "@").replace("[dot]", ".")

        emails = re.findall(self.email_regex, text)

        for email in emails:

            if any(email.endswith(ext) for ext in self.BAD_EXTENSIONS):
                continue

            if any(x in email for x in ["example", "test", "sample"]):
                continue

            if email not in self.collected_emails:
                self.collected_emails.add(email)

                yield {
                    "email": email,
                    "source": response.url
                }

        # ---------------------------
        # PRIORITY LINK EXTRACTION
        # ---------------------------
        priority_links = []
        normal_links = []

        for link in response.css("a::attr(href)").getall():

            url = response.urljoin(link).lower()

            if any(ext in url for ext in self.BAD_EXTENSIONS):
                continue

            if urlparse(url).netloc != domain:
                continue

            if url in self.visited_urls:
                continue

            # classify links
            if any(word in url for word in self.PRIORITY_KEYWORDS):
                priority_links.append(url)
            else:
                normal_links.append(url)

        # ---------------------------
        # FOLLOW PRIORITY FIRST
        # ---------------------------
        for url in priority_links[:5]:  # 🔥 only top priority
            yield scrapy.Request(url, callback=self.parse, errback=self.handle_error)

        # ---------------------------
        # THEN LIMITED NORMAL LINKS
        # ---------------------------
        for url in normal_links[:3]:  # 🔥 limit normal crawl
            yield scrapy.Request(url, callback=self.parse, errback=self.handle_error)
