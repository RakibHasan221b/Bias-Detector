import sys
import asyncio
import pandas as pd
from newspaper import Article
from datetime import datetime
import random
from urllib.parse import urljoin, urlparse
from playwright.async_api import async_playwright
import fake_useragent
from bs4 import BeautifulSoup
from tqdm import tqdm  # <- progress bar

# ---------------------------
# Config
# ---------------------------
CSV_FILE = r"C:\Users\rakib\Desktop\NEW desktop\THESIS WORK\news_comparison.csv"

sources = [
    {"name": "BBC", "region": "Europe", "search_base": "https://www.bbc.com/search?q=", "domain": "bbc.com"},
    {"name": "Euronews", "region": "Europe", "search_base": "https://www.euronews.com/search?query=", "domain": "euronews.com"},
    {"name": "Reuters", "region": "Europe", "search_base": "https://www.reuters.com/search/news?blob=", "domain": "reuters.com"},
    {"name": "The Daily Star", "region": "Bangladesh", "search_base": "https://www.thedailystar.net/search?keys=", "domain": "thedailystar.net"},
    {"name": "Prothom Alo English", "region": "Bangladesh", "search_base": "https://en.prothomalo.com/search?q=", "domain": "prothomalo.com"},
    {"name": "bdnews24", "region": "Bangladesh", "search_base": "https://bdnews24.com/search?q=", "domain": "bdnews24.com"}
]

topics = {
    "Russia Ukraine war": "russia ukraine",
    "Iran war": "iran israel",
    "China": "china"
}

START_DATE = datetime(2026, 1, 1)
END_DATE   = datetime(2026, 5, 31)
MAX_ARTICLES_PER_SOURCE_TOPIC = 20

ua = fake_useragent.UserAgent()

# ---------------------------
# Scraper function
# ---------------------------
async def scrape_articles(source, topic, keywords):
    name, base, domain = source["name"], source["search_base"], source["domain"]
    query = keywords.replace(" ", "+")
    search_url = f"{base}{query}"
    collected, visited, page = [], set(), 1

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context(user_agent=ua.random)
        page_browser = await context.new_page()

        while len(collected) < MAX_ARTICLES_PER_SOURCE_TOPIC and page <= 6:
            url = search_url
            if page > 1:
                url += f"&pn={page}" if "reuters.com" in domain else f"&page={page}"

            print(f"→ {name} | {topic} | page {page}")

            try:
                await page_browser.goto(url, wait_until="networkidle", timeout=60000)
                await page_browser.wait_for_timeout(3000)
                html = await page_browser.content()
                soup = BeautifulSoup(html, "html.parser")

                if page == 1:
                    with open(f"debug_{name}_{topic.replace(' ','_')}.html", "w", encoding="utf-8") as f:
                        f.write(html)
                    print(f"Debug HTML saved: debug_{name}_{topic.replace(' ','_')}.html")

                links = []
                for a in soup.find_all("a", href=True):
                    href = urljoin(url, a["href"])
                    parsed = urlparse(href)
                    if domain not in parsed.netloc or href in visited: 
                        continue
                    if any(x in href.lower() for x in ["/video/","/live/","/gallery/","/tag/","/author/","/search","/amp/"]): 
                        continue
                    if len(href) < 35: 
                        continue
                    links.append(href)

                links = list(set(links))[:30]
                print(f"Links found: {len(links)}")

            except Exception as e:
                print(f"Error fetching search page: {e}")
                break

            # Use tqdm progress bar for articles
            for link in tqdm(links, desc=f"{name} | {topic}", unit="article"):
                if len(collected) >= MAX_ARTICLES_PER_SOURCE_TOPIC: break
                visited.add(link)

                try:
                    await asyncio.sleep(random.uniform(1.5, 3.5))
                    article = Article(link, request_timeout=20)
                    article.download()
                    article.parse()

                    pub = article.publish_date
                    if not pub or not (START_DATE <= pub <= END_DATE): 
                        continue

                    text = ((article.title or "") + " " + (article.text or "")).lower()
                    if not any(k.lower() in text for k in keywords.split()): 
                        continue

                    collected.append({
                        "published_date": pub.strftime("%Y-%m-%d"),
                        "topic": topic,
                        "source": name,
                        "region": source["region"],
                        "title": article.title.strip(),
                        "url": link,
                        "full_text": (article.text or "").strip()[:15000]
                    })
                except Exception as e:
                    print(f"Error parsing article: {e}")
                    continue

            page += 1

        await browser.close()

    return collected

# ---------------------------
# Main function
# ---------------------------
async def main():
    print("Script started!")
    df = pd.DataFrame(columns=["published_date","topic","source","region","title","url","full_text"])

    for topic_name, kw in topics.items():
        print(f"\n=== {topic_name} ===")
        for src in sources:
            arts = await scrape_articles(src, topic_name, kw)
            print(f"  {src['name']} → {len(arts)} articles collected")
            if arts:
                df = pd.concat([df, pd.DataFrame(arts)], ignore_index=True)
        df.to_csv(CSV_FILE, index=False, encoding="utf-8")
        print(f"CSV saved after topic: {topic_name}")

    print(f"\nDone. Total articles collected: {len(df)}")
    print(f"CSV location: {CSV_FILE}")

# ---------------------------
# Entry point for script
# ---------------------------
if __name__ == "__main__":
    if sys.platform == "win32":
        asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())
    asyncio.run(main())