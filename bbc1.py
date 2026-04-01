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
import os

# ---------------------------
# Silence warnings
# ---------------------------
logging.getLogger('playwright').setLevel(logging.WARNING)
logging.getLogger('urllib3').setLevel(logging.WARNING)

# ---------------------------
# CONFIG
# ---------------------------
CSV_FILE = r"C:\Users\rakib\Desktop\NEW desktop\THESIS WORK\bbc.csv"
PKL_FILE = r"C:\Users\rakib\Desktop\NEW desktop\THESIS WORK\bbc.pkl"
START_DATE = datetime(2025, 6, 1)
MAX_PAGES = 60
ua = fake_useragent.UserAgent()

topics = {
    "Russia Ukraine war": "russia ukraine war",
    "Iran Israel war": "iran israel war",
    "Taiwan Strait conflict": "taiwan strait conflict"
}

BBC = {
    "name": "BBC",
    "region": "Europe",
    "search_base": "https://www.bbc.com/search?q=",
    "domain": "bbc.com",
    "pagination": "&page="
}

BBC_SELECTORS = [
    'a[href*="/news/"]:not([href*="/live/"]):not([href*="/video/"])',
    'a.sc-2e6e5fcd-1',
    'div[data-testid="promo"] a'
]

# ---------------------------
# Helper: Load last state
# ---------------------------
def load_last_state():
    if os.path.exists(PKL_FILE):
        try:
            df = pd.read_pickle(PKL_FILE)
            df['published_date'] = pd.to_datetime(df['published_date'], errors='coerce')
            last_date = df['published_date'].max()
            existing_urls = set(df['url'].dropna().tolist())
            return df, last_date, existing_urls
        except:
            pass
    # No previous state
    return pd.DataFrame(), START_DATE, set()

# ---------------------------
# Scraper function
# ---------------------------
async def scrape_bbc():
    df_old, last_date, existing_urls = load_last_state()
    if df_old.empty:
        print(f"🆕 First run: collecting from {START_DATE.date()}")
    else:
        print(f"🔄 Updating from last date: {last_date.date()}")

    all_collected = []
    visited_global = set()

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context(user_agent=ua.random)
        page_browser = await context.new_page()

        for topic_name, keywords in topics.items():
            print(f"\n=== {topic_name} ===")
            query = keywords.replace(" ", "+")
            search_url = f"{BBC['search_base']}{query}"
            page = 1
            empty_pages = 0
            topic_collected = 0

            while page <= MAX_PAGES:
                url = search_url if page == 1 else f"{search_url}{BBC['pagination']}{page}"
                print(f"→ BBC | {topic_name} | page {page} | {url}")

                try:
                    await page_browser.goto(url, wait_until="networkidle", timeout=90000)
                    await page_browser.wait_for_timeout(random.randint(3000, 6000))
                    html = await page_browser.content()
                    soup = BeautifulSoup(html, "html.parser")

                    links = []
                    for sel in BBC_SELECTORS:
                        for a in soup.select(sel):
                            href = a.get("href", "")
                            if not href: continue
                            full = urljoin(url, href)
                            parsed = urlparse(full)
                            if (BBC["domain"] in parsed.netloc and
                                full not in visited_global and
                                full not in existing_urls and
                                len(full) > 40 and
                                not any(x in full.lower() for x in ["/live/", "/video/", "/gallery/", "/tag/", "/author/", "/search", "/amp/"])):
                                links.append(full)

                    links = list(set(links))[:50]
                    if not links:
                        empty_pages += 1
                        if empty_pages >= 3:
                            print("  → No more results for this topic")
                            break
                    else:
                        empty_pages = 0

                except Exception as e:
                    print(f"  Page error: {e}")
                    break

                for link in tqdm(links, desc=f"BBC | {topic_name}", unit="art"):
                    if link in visited_global: continue
                    visited_global.add(link)

                    try:
                        await asyncio.sleep(random.uniform(2.5, 6.0))
                        downloaded = fetch_url(link)
                        if not downloaded:
                            continue

                        metadata = extract_metadata(downloaded)
                        text = extract(downloaded, output_format="txt", favor_recall=True)

                        title = metadata.title or ""
                        pub = None
                        if metadata.date:
                            try:
                                pub = date_parser.parse(str(metadata.date))
                            except:
                                continue

                        # Skip old articles
                        if not pub or pub <= last_date:
                            continue

                        full_content = (title + " " + (text or "")).lower()
                        keyword_list = keywords.split()
                        if sum(word in full_content for word in keyword_list) < 2:
                            continue

                        all_collected.append({
                            "published_date": pub.strftime("%Y-%m-%d"),
                            "topic": topic_name,
                            "source": BBC["name"],
                            "region": BBC["region"],
                            "title": title.strip(),
                            "url": link,
                            "full_text": (text or "")[:25000]
                        })
                        topic_collected += 1

                        print(f"  + Added: {title[:70]}... ({pub.date()}) | {link}")

                    except Exception as e:
                        print(f"  Skip {link}: {e}")

                page += 1

            print(f"  → Topic '{topic_name}' done. {topic_collected} new articles collected.")

        await browser.close()

    # ---------------------------
    # SAVE / APPEND TO CSV and PKL (sorted by date)
    # ---------------------------
    if all_collected:
        new_df = pd.DataFrame(all_collected)
        combined = pd.concat([df_old, new_df], ignore_index=True) if not df_old.empty else new_df
        combined = combined.drop_duplicates(subset=['url'], keep='last')
        combined['published_date'] = pd.to_datetime(combined['published_date'], errors='coerce')
        combined = combined.sort_values(by='published_date', ascending=False)

        # Save CSV (human-readable)
        combined.to_csv(CSV_FILE, index=False, encoding="utf-8-sig")
        # Save PKL (fast next run)
        combined.to_pickle(PKL_FILE)

        print(f"\n✅ {len(new_df)} new articles added → Total in CSV/PKL: {len(combined)} (sorted by date)")
    else:
        print("\n🟡 No new articles found — already up to date")

# ---------------------------
# MAIN
# ---------------------------
async def main():
    print("🚀 BBC scraper started:", datetime.now().strftime("%Y-%m-%d %H:%M"))
    await scrape_bbc()
    print("\n✅ DONE. Check:", CSV_FILE)

if __name__ == "__main__":
    if sys.platform == "win32":
        asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())
    asyncio.run(main())