# bdnews24_single_browser_fixed.py
import asyncio
import random
import pandas as pd
from datetime import datetime
import os
import re
from playwright.async_api import async_playwright
from bs4 import BeautifulSoup
from tqdm import tqdm
import ftfy

# ================= CONFIG =================
BASE_URL = "https://bdnews24.com/world"
CSV_FILE = "bdnews24_news.csv"
PKL_FILE = "bdnews24_news.pkl"
START_DATE = datetime(2025, 6, 1)
MAX_PAGES = 25

TOPICS = {
    "Russia Ukraine war": ["russia", "ukraine", "putin", "zelensky", "kyiv", "kiev", "donbas", "crimea", "russian invasion", "ukraine war"],
    "Iran Israel war": ["iran", "israel", "tehran", "tel aviv", "houthis", "hezbollah", "hormuz", "iran war", "israel war", "us israel", "middle east war"],
    "Taiwan Strait conflict": ["taiwan", "taipei", "beijing", "china taiwan", "taiwan strait", "cross strait", "taiwanese", "pla navy", "taiwan independence"]
}

# Load previous data
def load_state():
    if os.path.exists(PKL_FILE):
        df = pd.read_pickle(PKL_FILE)
        df['published_date'] = pd.to_datetime(df['published_date'], errors='coerce')
        return df, set(df['url'].dropna().tolist())
    cols = ["published_date", "topic", "source", "region", "title", "url", "full_text"]
    return pd.DataFrame(columns=cols), set()

def get_topic(title: str, text: str):
    combined = (title + " " + (text or "")[:2000]).lower()
    for t, kws in TOPICS.items():
        if any(kw in combined for kw in kws):
            return t
    return None

def parse_article(html: str):
    try:
        soup = BeautifulSoup(html, "html.parser")
        title = soup.find("h1").get_text(strip=True) if soup.find("h1") else "No title"

        published_date = None
        meta = soup.find("meta", property="article:published_time")
        if meta and meta.get("content"):
            try:
                published_date = datetime.fromisoformat(meta["content"].replace("Z", "+00:00"))
            except:
                pass

        # Full text
        body = soup.select_one("div.article-body, div.story-content, article")
        full_text = ""
        if body:
            paragraphs = body.find_all("p")
            full_text = "\n\n".join(p.get_text(strip=True) for p in paragraphs if len(p.get_text(strip=True)) > 30)

        full_text = ftfy.fix_text(full_text)
        full_text = re.sub(r'\s+', ' ', full_text).strip()

        if published_date is None and len(full_text) > 300:
            published_date = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)

        return title, published_date, full_text
    except:
        return "Parse Error", None, ""

# ================= MAIN (Single Browser) =================
async def main():
    df_old, existing_urls = load_state()
    all_new = []

    print("🚀 Starting bdnews24 with SINGLE browser window...\n")

    async with async_playwright() as p:
        # Launch ONE browser
        browser = await p.chromium.launch(headless=False)   # ← Change to True when stable
        context = await browser.new_context(
            viewport={"width": 1366, "height": 768},
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        )
        page = await context.new_page()   # ← Only ONE page object

        page_num = 1
        while page_num <= MAX_PAGES:
            url = f"{BASE_URL}?page={page_num}" if page_num > 1 else BASE_URL
            print(f"📄 Page {page_num}: {url}")

            await page.goto(url, wait_until="domcontentloaded", timeout=60000)
            await page.wait_for_timeout(6000)   # let cards load

            html = await page.content()
            soup = BeautifulSoup(html, "html.parser")

            # Correct link pattern for bdnews24
            links = []
            for a in soup.select('a[href]'):
                href = a.get("href", "")
                if re.search(r'/[a-z]+/[a-f0-9]{10,}', href):   # e.g. /world/22c423fb9edf
                    full_url = "https://bdnews24.com" + href if href.startswith("/") else href
                    if full_url not in existing_urls and full_url not in links:
                        links.append(full_url)

            links = list(dict.fromkeys(links))[:20]
            print(f"   Found {len(links)} new links")

            if not links:
                print("   No more articles. Stopping.")
                break

            for link in tqdm(links, desc=f"Page {page_num}"):
                if link in existing_urls:
                    continue

                try:
                    await page.goto(link, wait_until="domcontentloaded", timeout=45000)
                    await page.wait_for_timeout(random.randint(2500, 4500))

                    article_html = await page.content()
                    title, pub_date, full_text = parse_article(article_html)

                    if not pub_date or pub_date < START_DATE:
                        continue

                    topic = get_topic(title, full_text)
                    if not topic:
                        continue

                    all_new.append({
                        "published_date": pub_date.strftime("%Y-%m-%d"),
                        "topic": topic,
                        "source": "bdnews24",
                        "region": "World",
                        "title": title,
                        "url": link,
                        "full_text": full_text[:25000]
                    })

                    print(f"   ✅ Added [{topic}] {title[:65]}...")

                except Exception as e:
                    print(f"   Skip: {e}")

                await page.wait_for_timeout(random.randint(1500, 3000))   # polite delay

            page_num += 1
            await page.wait_for_timeout(random.randint(4000, 7000))

        await browser.close()

    # Save results
    if all_new:
        new_df = pd.DataFrame(all_new)
        combined = pd.concat([df_old, new_df], ignore_index=True)
        combined = combined.drop_duplicates(subset=['url'])
        combined['published_date'] = pd.to_datetime(combined['published_date'])
        combined = combined.sort_values(by='published_date', ascending=False)

        combined.to_csv(CSV_FILE, index=False, encoding="utf-8-sig")
        combined.to_pickle(PKL_FILE)
        print(f"\n🎉 Added {len(new_df)} new articles. Total: {len(combined)}")
    else:
        print("\nNo new matching articles found.")

if __name__ == "__main__":
    asyncio.run(main())