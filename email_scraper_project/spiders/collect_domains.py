import requests
from bs4 import BeautifulSoup
from urllib.parse import urlparse, parse_qs, unquote
import time
import random

# ---------------------------
# CONFIG
# ---------------------------

SERVICES = [

    # HR
    "recruitment agency",
    "staffing company",
    "HR consulting firm",
    "talent acquisition company",
]

STATES = [

  
    # Extra UK Regions (better coverage)
    "England UK",
    "Scotland UK",
    "Wales UK",
    "Northern Ireland UK",

]
PAGES = [0]
DELAY_RANGE = (4, 8)

BLOCKED = [
    "youtube", "facebook", "linkedin", "instagram", "twitter",
    "yelp",
]

BAD_DOMAINS = [
    # job boards
    "indeed.com",
    "nijobs.com",
    "reed.co.uk",
    "glassdoor.com",
    "monster.com",

    # directories / listings
    "yelp.com",
    "yellowpages.com",
    "thomasnet.com",
    "angi.com",
    "mapquest.com",
    "clutch.co",
    "industryselect.com",

    # content / blog sites
    "builtin.com",
    "forbes.com",
    "medium.com",

    # marketplaces
    "f6s.com",
]

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)",
    "Mozilla/5.0 (X11; Linux x86_64)"
]

# ---------------------------
# SESSION
# ---------------------------
session = requests.Session()

# ---------------------------
# HELPERS
# ---------------------------

def get_headers():
    return {
        "User-Agent": random.choice(USER_AGENTS),
        "Accept-Language": "en-US,en;q=0.9"
    }


def generate_queries():
    return [f"{s} {state}" for s in SERVICES for state in STATES]



def clean_domain(url):
    try:
        parsed = urlparse(url)

        if not parsed.scheme or not parsed.netloc:
            return None

        domain = parsed.netloc.lower().replace("www.", "")

        # ❌ STRICT search engine filter (ONLY exact domains)
        SEARCH_ENGINES = [
            "google.com",
            "bing.com",
            "duckduckgo.com",
            "search.yahoo.com",
            "yahoo.com",
            "startpage.com"
        ]

        if any(domain.endswith(se) for se in SEARCH_ENGINES):
            return None

        # ❌ Social / junk
        if any(x in domain for x in [
            "youtube", "facebook", "linkedin", "instagram", "twitter"
        ]):
            return None

        # ❌ Bad directories
        if any(bad in domain for bad in BAD_DOMAINS):
            return None

        # ❌ junk links
        if "aclick" in url or "y.js" in url:
            return None

        return domain

    except:
        return None


# ---------------------------
# DUCKDUCKGO
# ---------------------------

def fetch_duckduckgo(query, start):
    url = "https://duckduckgo.com/html/"

    params = {
        "q": query,
        "s": str(start)
    }

    try:
        r = session.get(url, params=params, headers=get_headers(), timeout=15)
        soup = BeautifulSoup(r.text, "html.parser")

        links = []

        for a in soup.select("a.result__a"):
            href = a.get("href")

            if href and "uddg=" in href and "y.js" not in href:
                real_url = parse_qs(urlparse(href).query).get("uddg", [])
                if real_url:
                    real_url = unquote(real_url[0])
                    links.append(real_url)

        return links

    except Exception as e:
        print("DDG error:", e)
        return []


# ---------------------------
# YAHOO (FIXED)
# ---------------------------

def fetch_yahoo(query):
    url = "https://search.yahoo.com/search"

    params = {
        "p": query
    }

    links = []

    try:
        r = session.get(url, params=params, headers=get_headers(), timeout=15)
        soup = BeautifulSoup(r.text, "html.parser")

        results = soup.select("h3.title a")

        for a in results:
            href = a.get("href")

            if href and href.startswith("http"):
                links.append(href)

        print(f"Yahoo found {len(links)} links")

    except Exception as e:
        print("Yahoo error:", e)

    return links
#bing
def fetch_bing(query):
    url = "https://www.bing.com/search"

    params = {
        "q": query
    }

    links = []

    try:
        r = session.get(url, params=params, headers=get_headers(), timeout=15)
        soup = BeautifulSoup(r.text, "html.parser")

        for a in soup.select("li.b_algo h2 a"):
            href = a.get("href")
            if href:
                links.append(href)

        print(f"Bing found {len(links)} links")

    except Exception as e:
        print("Bing error:", e)

    return links

#startpage
def fetch_startpage(query):
    url = "https://www.startpage.com/sp/search"

    params = {
        "query": query
    }

    links = []

    try:
        r = session.get(url, params=params, headers=get_headers(), timeout=15)
        soup = BeautifulSoup(r.text, "html.parser")

        for a in soup.select("a.w-gl__result-title"):
            href = a.get("href")
            if href:
                links.append(href)

        print(f"Startpage found {len(links)} links")

    except Exception as e:
        print("Startpage error:", e)

    return links
# ---------------------------
# MAIN
# ---------------------------

def main():
    queries = generate_queries()
    all_domains = set()

    try:
        with open("domains.txt", "r") as f:
            for line in f:
                d = line.strip().replace("http://", "")
                if d:
                    all_domains.add(d)
        print(f"📂 Loaded existing: {len(all_domains)}")
    except FileNotFoundError:
        print("📂 Starting fresh")

    print("Total queries:", len(queries))

    for q in queries:
        print(f"\n🔍 {q}")

        new_domains = set()

    # DDG
        for start in PAGES:
            links = fetch_duckduckgo(q, start)

            print(f"DDG links: {len(links)}")
            print(links[:3])

            for link in links:
                if "duckduckgo.com" in link or "aclick" in link:
                    continue

                d = clean_domain(link)
                if d and d not in all_domains:
                    new_domains.add(d)

            time.sleep(random.uniform(*DELAY_RANGE))

    # YAHOO 
        time.sleep(random.uniform(2, 4))
        links = fetch_yahoo(q)

        for link in links:
            d = clean_domain(link)
            if d and d not in all_domains:
                new_domains.add(d)

        # BING
        time.sleep(random.uniform(2, 4))
        links = fetch_bing(q)

        for link in links:
            d = clean_domain(link)
            if d and d not in all_domains:
                new_domains.add(d)

        # STARTPAGE
        time.sleep(random.uniform(2, 4))
        # links = fetch_startpage(q)

        for link in links:
            d = clean_domain(link)
            if d and d not in all_domains:
                new_domains.add(d)


        # SAVE
        if new_domains:
            with open("domains.txt", "a") as f:
                for d in new_domains:
                    f.write(f"http://{d}\n")

            all_domains.update(new_domains)
            print(f"➕ Added {len(new_domains)} | Total: {len(all_domains)}")

        time.sleep(random.uniform(*DELAY_RANGE))

    print("\n✅ DONE:", len(all_domains))


# ---------------------------
# RUN
# ---------------------------

if __name__ == "__main__":
    main()