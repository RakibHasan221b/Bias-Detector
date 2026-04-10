import sys
import asyncio
import pandas as pd
from datetime import datetime, timedelta
import random
from urllib.parse import urljoin
from playwright.async_api import async_playwright
import fake_useragent
from bs4 import BeautifulSoup
from tqdm import tqdm
from trafilatura import fetch_url, extract, extract_metadata
from dateutil import parser as date_parser
import os

# ========================= CONFIG =========================
# YOUR EXACT PATHS
BASE_PATH = r"C:\Users\rakib\Desktop\NEW desktop\THESIS WORK\Data"
os.makedirs(BASE_PATH, exist_ok=True)

CSV_FILE = os.path.join(BASE_PATH, "bbc.csv")
PKL_FILE = os.path.join(BASE_PATH, "bbc.pkl")

START_DATE = datetime(2025, 6, 1)
MAX_PAGES = 15
STOP_AFTER_OLD = 3

ua = fake_useragent.UserAgent()

# Dedicated clean topic pages
TOPICS = {
    "Russia Ukraine war": {
        "url": "https://www.bbc.com/news/war-in-ukraine",
        "keywords": "russia ukraine war"
    },
    "Iran Israel war": {
        "url": "https://www.bbc.com/news/world/middle_east",
        "keywords": "iran israel war"
    },
    "Taiwan Strait conflict": {
        "url": "https://www.bbc.com/news/topics/cg41ylwvw2qt",
        "keywords": "taiwan strait conflict china"
    }
}

BBC_SELECTORS = [
    'a[href*="/news/"]:not([href*="/live/"]):not([href*="/video/"])',
    'a.sc-2e6e5fcd-1',
    'div[data-testid="promo"] a',
    'article a'
]

def load_last_state():
    if not os.path.exists(CSV_FILE):
        print(f"🆕 No CSV found at {CSV_FILE}. Starting fresh from {START_DATE.date()}")
        return pd.DataFrame(), START_DATE, set()

    try:
        df = pd.read_csv(CSV_FILE, encoding="utf-8-sig")
        df = df.dropna(subset=['url', 'published_date'])
        df['published_date'] = pd.to_datetime(df['published_date'], errors='coerce')
        df = df.dropna(subset=['published_date'])

        if df.empty:
            print("⚠️ CSV empty after cleaning. Starting fresh.")
            return pd.DataFrame(), START_DATE, set()

        last_date = df['published_date'].max()
        existing_urls = set(df['url'].dropna().tolist())

        print(f"✅ Loaded CSV successfully → {len(df)} articles | Last date: {last_date.date()} | Existing URLs: {len(existing_urls)}")
        return df, last_date, existing_urls

    except Exception as e:
        print(f"❌ Failed to load CSV: {e}. Starting fresh.")
        return pd.DataFrame(), START_DATE, set()


async def scrape_bbc(df_old, last_date, existing_urls):
    all_collected = []
    visited_global = set()

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context(user_agent=ua.random)
        page_browser = await context.new_page()

        for topic_name, config in TOPICS.items():
            print(f"\n{'═' * 35} {topic_name} {'═' * 35}")
            base_url = config["url"]
            keywords = config["keywords"].split()

            page = 1
            topic_collected = 0
            old_seen = 0
            topic_stopped = False

            while page <= MAX_PAGES and not topic_stopped:
                url = base_url if page == 1 else f"{base_url}?page={page}"
                print(f"→ Scraping {topic_name} | page {page}")

                try:
                    await page_browser.goto(url, wait_until="networkidle", timeout=60000)
                    await page_browser.wait_for_timeout(random.randint(3000, 5000))
                    html = await page_browser.content()
                    soup = BeautifulSoup(html, "html.parser")

                    links = []
                    for sel in BBC_SELECTORS:
                        for a in soup.select(sel):
                            href = a.get("href", "")
                            if not href: continue
                            full = urljoin(url, href)
                            if ("bbc.com" in full and full not in visited_global and
                                len(full) > 40 and not any(x in full.lower() for x in ["/live/", "/video/", "/gallery/", "/tag/", "/author/", "/search", "/amp/"])):
                                links.append(full)

                    links = list(set(links))[:25]
                    print(f"  Found {len(links)} candidate links")

                except Exception as e:
                    print(f"  Page error: {e}")
                    break

                for link in tqdm(links, desc=f"{topic_name}", unit="art"):
                    if link in visited_global: continue
                    visited_global.add(link)

                    if link in existing_urls:
                        old_seen += 1
                        if old_seen >= STOP_AFTER_OLD:
                            print(f"  → Reached existing articles. Stopping {topic_name}.")
                            topic_stopped = True
                        continue

                    try:
                        await asyncio.sleep(random.uniform(2.0, 4.5))
                        downloaded = fetch_url(link)
                        if not downloaded: continue

                        metadata = extract_metadata(downloaded)
                        text = extract(downloaded, output_format="txt", favor_recall=True)

                        title = metadata.title or ""
                        if not metadata.date: continue

                        pub = date_parser.parse(str(metadata.date)).replace(tzinfo=None)

                        if pub <= last_date:
                            old_seen += 1
                            if old_seen >= STOP_AFTER_OLD:
                                print(f"  → Reached known date ({pub.date()}). Stopping {topic_name}.")
                                topic_stopped = True
                            continue

                        full_content = (title + " " + (text or "")).lower()
                        if sum(word in full_content for word in keywords) < 2:
                            continue

                        all_collected.append({
                            "published_date": pub.strftime("%Y-%m-%d"),
                            "topic": topic_name,
                            "source": "BBC",
                            "region": "Europe" if "Russia" in topic_name else "Middle East" if "Iran" in topic_name else "Asia",
                            "title": title.strip(),
                            "url": link,
                            "full_text": (text or "")[:25000]
                        })
                        topic_collected += 1
                        print(f"  + Added: {title[:70]}... ({pub.date()})")

                    except Exception:
                        continue

                page += 1

            print(f"→ {topic_name} finished → {topic_collected} new articles")

        await browser.close()

    return all_collected


def save_data(all_collected, df_old):
    if not all_collected:
        print("\n🟡 No new articles found today.")
        return

    new_df = pd.DataFrame(all_collected)
    combined = pd.concat([df_old, new_df], ignore_index=True) if not df_old.empty else new_df
    combined = combined.drop_duplicates(subset=['url'])
    combined['published_date'] = pd.to_datetime(combined['published_date'], errors='coerce')
    combined = combined.dropna(subset=['published_date'])

    topic_order = ["Russia Ukraine war", "Iran Israel war", "Taiwan Strait conflict"]
    combined['topic'] = pd.Categorical(combined['topic'], categories=topic_order, ordered=True)
    combined = combined.sort_values(by=['published_date', 'topic', 'source'], ascending=[False, True, True])

    # Save to your original location
    combined.to_csv(CSV_FILE, index=False, encoding="utf-8-sig")
    combined.to_pickle(PKL_FILE)

    print(f"\n✅ Added {len(new_df)} new articles → Total: {len(combined)}")


async def main():
    print(f"🚀 BBC Daily Scraper Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    df_old, last_date, existing_urls = load_last_state()
    new_articles = await scrape_bbc(df_old, last_date, existing_urls)
    save_data(new_articles, df_old)
    print("✅ BBC Daily Scraper finished.")


if __name__ == "__main__":
    if sys.platform == "win32":
        asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())
    asyncio.run(main())