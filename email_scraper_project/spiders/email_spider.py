# import scrapy
# import re


# class EmailSpider(scrapy.Spider):
#     name = "email_spider"

#     custom_settings = {
#         "ROBOTSTXT_OBEY": False,
#         "USER_AGENT": "Mozilla/5.0"
#     }

#     email_regex = r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}"

#     keywords = ["contact", "about", "team", "support", "company", "location"]

#     collected_emails = set()

#     start_urls = [
  


#     ]

#     def parse(self, response):

#         emails = re.findall(self.email_regex, response.text)

#         for email in emails:
#             if email not in self.collected_emails:

#                 self.collected_emails.add(email)

#                 yield {
#                     "email": email,
#                     "source": response.url
#                 }

#         for link in response.css("a::attr(href)").getall():

#             url = response.urljoin(link).lower()

#             if any(word in url for word in self.keywords):

#                 yield scrapy.Request(url, callback=self.parse_contact)

#     def parse_contact(self, response):

#         emails = re.findall(self.email_regex, response.text)

#         for email in emails:

#             if email not in self.collected_emails:

#                 self.collected_emails.add(email)

#                 yield {
#                     "email": email,
#                     "source": response.url
#                 }
# import scrapy
# import re


# class EmailSpider(scrapy.Spider):
#     name = "email_spider"

#     custom_settings = {
#         "ROBOTSTXT_OBEY": False,
#         "USER_AGENT": "Mozilla/5.0",
#         "FEEDS": {
#             "emails.csv": {
#                 "format": "csv",
#                 "overwrite": False  # ✅ append mode (updates live)
#             }
#         }
#     }

#     # ---------------------------
#     # EMAIL REGEX (IMPROVED)
#     # ---------------------------
#     email_regex = r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}"

#     # ---------------------------
#     # FILTERS
#     # ---------------------------
#     BAD_EXTENSIONS = (
#         ".png", ".jpg", ".jpeg", ".gif", ".webp",
#         ".svg", ".css", ".js", ".ico", ".woff", ".ttf"
#     )

#     KEYWORDS = [
#         "contact", "about", "team", "support",
#         "company", "location", "service", "help"
#     ]

#     collected_emails = set()
#     visited_urls = set()

#     # ---------------------------
#     # LOAD DOMAINS
#     # ---------------------------
#     def start_requests(self):
#         try:
#             with open("domains.txt", "r") as f:
#                 for line in f:
#                     url = line.strip()

#                     if not url.startswith("http"):
#                         url = "http://" + url

#                     yield scrapy.Request(url, callback=self.parse, dont_filter=True)

#         except Exception as e:
#             self.logger.error(f"Error loading domains.txt: {e}")

#     # ---------------------------
#     # MAIN PARSER
#     # ---------------------------
#     def parse(self, response):

#         self.visited_urls.add(response.url)

#         # ✅ Extract emails
#         emails = re.findall(self.email_regex, response.text)

#         for email in emails:
#             email = email.lower()

#             # ❌ filter garbage emails
#             if any(email.endswith(ext) for ext in self.BAD_EXTENSIONS):
#                 continue

#             if "@" not in email:
#                 continue

#             if email not in self.collected_emails:
#                 self.collected_emails.add(email)

#                 yield {
#                     "email": email,
#                     "source": response.url
#                 }

#         # ---------------------------
#         # FOLLOW LINKS (SMART)
#         # ---------------------------
#         for link in response.css("a::attr(href)").getall():

#             url = response.urljoin(link).lower()

#             # ❌ skip unwanted
#             if any(ext in url for ext in self.BAD_EXTENSIONS):
#                 continue

#             if url in self.visited_urls:
#                 continue

#             # ✅ priority pages
#             if any(word in url for word in self.KEYWORDS):
#                 self.visited_urls.add(url)
#                 yield scrapy.Request(url, callback=self.parse)

#             # ✅ limited crawling (depth control)
#             elif response.url.count("/") < 5:
#                 self.visited_urls.add(url)
#                 yield scrapy.Request(url, callback=self.parse)

#     # ---------------------------
#     # EXTRA SAFETY PARSER
#     # ---------------------------
#     def parse_contact(self, response):

#         emails = re.findall(self.email_regex, response.text)

#         for email in emails:
#             email = email.lower()

#             if any(email.endswith(ext) for ext in self.BAD_EXTENSIONS):
#                 continue

#             if email not in self.collected_emails:
#                 self.collected_emails.add(email)

#                 yield {
#                     "email": email,
#                     "source": response.url
#                 }










# import scrapy
# import re
# from urllib.parse import urlparse


# class EmailSpider(scrapy.Spider):
#     name = "email_spider"

#     custom_settings = {
#         "ROBOTSTXT_OBEY": False,
#         "USER_AGENT": "Mozilla/5.0",
#         "DOWNLOAD_TIMEOUT": 10,
#         "RETRY_TIMES": 1,
#         "FEEDS": {
#             "emails.csv": {
#                 "format": "csv",
#                 "overwrite": False
#             }
#         }
#     }

#     email_regex = r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}"

#     BAD_EXTENSIONS = (
#         ".png", ".jpg", ".jpeg", ".gif", ".webp",
#         ".svg", ".css", ".js", ".ico", ".woff", ".ttf"
#     )

#     KEYWORDS = ["contact", "about", "team", "support", "service"]

#     collected_emails = set()
#     visited_urls = set()

#     MAX_PAGES_PER_DOMAIN = 20  # 🔥 LIMIT

#     domain_counts = {}

#     # ---------------------------
#     # LOAD DOMAINS
#     # ---------------------------
#     def start_requests(self):
#         try:
#             with open("domains.txt", "r") as f:
#                 for line in f:
#                     url = line.strip()

#                     if not url.startswith("http"):
#                         url = "http://" + url

#                     domain = urlparse(url).netloc
#                     self.domain_counts[domain] = 0

#                     yield scrapy.Request(
#                         url,
#                         callback=self.parse,
#                         errback=self.handle_error,
#                         dont_filter=True
#                     )

#         except Exception as e:
#             self.logger.error(f"Error loading domains.txt: {e}")

#     # ---------------------------
#     # ERROR HANDLING
#     # ---------------------------
#     def handle_error(self, failure):
#         self.logger.info(f"Skipping failed request: {failure.request.url}")

#     # ---------------------------
#     # MAIN PARSER
#     # ---------------------------
#     def parse(self, response):

#         # ❌ skip bad responses
#         if response.status in [403, 404, 429]:
#             return

#         domain = urlparse(response.url).netloc

#         # 🔥 limit per domain
#         if self.domain_counts.get(domain, 0) >= self.MAX_PAGES_PER_DOMAIN:
#             return

#         self.domain_counts[domain] += 1
#         self.visited_urls.add(response.url)

#         # ---------------------------
#         # EMAIL EXTRACTION
#         # ---------------------------
#         text = response.text.lower()
#         text = text.replace(" [at] ", "@").replace(" [dot] ", ".")

#         emails = re.findall(self.email_regex, text)

#         for email in emails:

#             if any(email.endswith(ext) for ext in self.BAD_EXTENSIONS):
#                 continue

#             if any(x in email for x in ["example", "test", "sample"]):
#                 continue

#             if email not in self.collected_emails:
#                 self.collected_emails.add(email)

#                 yield {
#                     "email": email,
#                     "source": response.url
#                 }

#         # ---------------------------
#         # FOLLOW LINKS (SAME DOMAIN ONLY)
#         # ---------------------------
#         for link in response.css("a::attr(href)").getall():

#             url = response.urljoin(link).lower()

#             # ❌ skip files
#             if any(ext in url for ext in self.BAD_EXTENSIONS):
#                 continue

#             # ❌ skip external domains
#             if urlparse(url).netloc != domain:
#                 continue

#             # ❌ skip visited
#             if url in self.visited_urls:
#                 continue

#             # ❌ skip useless pages
#             if any(x in url for x in ["privacy", "terms", "login"]):
#                 continue

#             # ✅ follow priority
#             if any(word in url for word in self.KEYWORDS):
#                 yield scrapy.Request(url, callback=self.parse, errback=self.handle_error)

#             # ✅ limited crawl depth
#             elif response.url.count("/") < 5:
#                 yield scrapy.Request(url, callback=self.parse, errback=self.handle_error)



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
