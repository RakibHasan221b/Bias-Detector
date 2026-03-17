import sys
import asyncio
import pandas as pd
from datetime import datetime
import random
from urllib.parse import urljoin, urlparse
from playwright.async_api import async_playwright
import fake_useragent
from bs4 import BeautifulSoup
from tqdm import tqdm
from trafilatura import fetch_url, extract, extract_metadata
from dateutil import parser as date_parser
import logging

# Quiet warnings
logging.getLogger('playwright').setLevel(logging.WARNING)
logging.getLogger('urllib3').setLevel(logging.WARNING)

# ---------------------------
# Config
# ---------------------------
CSV_FILE = r"C:\Users\rakib\Desktop\NEW desktop\THESIS WORK\news_comparison.csv"

sources = [
    {"name": "BBC", "region": "Europe", "search_base": "https://www.bbc.com/search?q=", "domain": "bbc.com", "pagination": "&page="},
    {"name": "Euronews", "region": "Europe", "search_base": "https://www.euronews.com/search?query=", "domain": "euronews.com", "pagination": "&page="},
    {"name": "Reuters", "region": "Europe", "search_base": "https://www.reuters.com/search/news?blob=", "domain": "reuters.com", "pagination": "&offset="},  # or &page= — test
    {"name": "The Daily Star", "region": "Bangladesh", "search_base": "https://www.thedailystar.net/search?keys=", "domain": "thedailystar.net", "pagination": "&page="},
    {"name": "Prothom Alo English", "region": "Bangladesh", "search_base": "https://en.prothomalo.com/search?q=", "domain": "prothomalo.com", "pagination": "&page="},
    {"name": "bdnews24", "region": "Bangladesh", "search_base": "https://bdnews24.com/search?q=", "domain": "bdnews24.com", "pagination": "&page="}
]

topics = {
    "Russia Ukraine war": "russia ukraine",
    "Iran war": "iran israel",
    "China": "china"
}

START_DATE = datetime(2025, 6, 1)   
END_DATE   = datetime(2026, 3, 17)
MAX_ARTICLES_PER_SOURCE_TOPIC = 20

ua = fake_useragent.UserAgent()

# ---------------------------
# Source-specific selectors 
# ---------------------------
SELECTORS = {
    "BBC": ['a[href*="/news/"]:not([href*="/live/"]):not([href*="/video/"])', 'a.sc-2e6e5fcd-1', 'div[data-testid="promo"] a'],
    "Euronews": ['article a', 'a.media__link', 'a[href*="/my-europe/"]', 'a[href*="/news/"]'],
    "Reuters": ['a[data-testid="Heading"]', 'a.search-result__link', 'div[data-testid="article"] a', 'h3 a'],
    "The Daily Star": ['.story-card a', '.teaser-title a', 'a[href*="/news/"]', '.card-title a'],
    "Prothom Alo English": ['.search-item a', '.title a', 'a[href*="/story/"]'],
    "bdnews24": ['.search-result a', '.cat-news-item a', 'a[href*="/article/"]']
}

# ---------------------------
# Scraper function
# ---------------------------
async def scrape_articles(source, topic, keywords):
    name, base, domain, pag_param = source["name"], source["search_base"], source["domain"], source.get("pagination", "")
    query = keywords.replace(" ", "+")
    search_url = f"{base}{query}"
    collected, visited = [], set()
    page = 1

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context(user_agent=ua.random)
        page_browser = await context.new_page()

        while len(collected) < MAX_ARTICLES_PER_SOURCE_TOPIC and page <= 7:
            url = search_url if page == 1 else f"{search_url}{pag_param}{page}"

            print(f"→ {name} | {topic} | page {page} | {url}")

            try:
                await page_browser.goto(url, wait_until="networkidle", timeout=60000)
                await page_browser.wait_for_timeout(random.randint(2000, 5000))
                html = await page_browser.content()
                soup = BeautifulSoup(html, "html.parser")

                if page == 1:
                    with open(f"debug_{name.replace(' ','_')}_{topic.replace(' ','_')}.html", "w", encoding="utf-8") as f:
                        f.write(html)
                    print(f"Debug saved: debug_{name.replace(' ','_')}_{topic.replace(' ','_')}.html")

                # Get links using source-specific selectors
                links = []
                for sel in SELECTORS.get(name, []):
                    for a in soup.select(sel):
                        href = a.get("href", "")
                        if not href: continue
                        full_href = urljoin(url, href)
                        parsed = urlparse(full_href)
                        if (domain in parsed.netloc and
                            full_href not in visited and
                            len(full_href) > 40 and
                            not any(x in full_href.lower() for x in ["/video/", "/live/", "/gallery/", "/tag/", "/author/", "/search", "/amp/"])):
                            links.append(full_href)

                links = list(set(links))[:30]
                print(f"Links found on page {page}: {len(links)}")

            except Exception as e:
                print(f"Error on search page {page}: {e}")
                break

            for link in tqdm(links, desc=f"{name} | {topic}", unit="art"):
                if len(collected) >= MAX_ARTICLES_PER_SOURCE_TOPIC: break
                visited.add(link)

                try:
                    await asyncio.sleep(random.uniform(1.8, 4.2))
                    downloaded = fetch_url(link)
                    if not downloaded:
                        continue

                    metadata = extract_metadata(downloaded)
                    text = extract(downloaded, include_comments=False, output_format="txt")

                    title = metadata.title or ""
                    pub_str = metadata.date

                    pub = None
                    if pub_str:
                        try:
                            pub = date_parser.parse(str(pub_str))
                        except:
                            pass

                    if not pub or not (START_DATE <= pub <= END_DATE):
                        continue

                    full_content = (title + " " + (text or "")).lower()
                    if not any(k.lower() in full_content for k in keywords.split()):
                        continue

                    collected.append({
                        "published_date": pub.strftime("%Y-%m-%d"),
                        "topic": topic,
                        "source": name,
                        "region": source["region"],
                        "title": title.strip(),
                        "url": link,
                        "full_text": (text or "").strip()[:15000]
                    })
                    print(f"  + Added: {title[:60]}...")

                except Exception as e:
                    print(f"  Skip {link}: {e}")

            page += 1

        await browser.close()

    return collected

# ---------------------------
# Main
# ---------------------------
async def main():
    print("Script started at", datetime.now().strftime("%Y-%m-%d %H:%M"))
    df = pd.DataFrame(columns=["published_date","topic","source","region","title","url","full_text"])

    for topic_name, kw in topics.items():
        print(f"\n=== {topic_name} ===")
        for src in sources:
            arts = await scrape_articles(src, topic_name, kw)
            print(f"  {src['name']} → {len(arts)} articles")
            if arts:
                df = pd.concat([df, pd.DataFrame(arts)], ignore_index=True)
        df.to_csv(CSV_FILE, index=False, encoding="utf-8-sig")  # utf-8-sig better for Excel
        print(f"CSV saved after {topic_name}")

    print(f"\nFinished. Total: {len(df)} articles")
    print(f"Saved → {CSV_FILE}")

if __name__ == "__main__":
    if sys.platform == "win32":
        asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())
    asyncio.run(main())