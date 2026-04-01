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
BASE_URL = "https://www.dhakatribune.com/world"
OUTPUT_CSV = "dhakatribune_news.csv"
OUTPUT_PKL = "dhakatribune_news.pkl"
START_DATE = datetime(2025, 6, 1)
MAX_PAGES = 20

TOPICS = {
    "Russia Ukraine war": ["russia", "ukraine", "putin", "zelensky", "kyiv", "kiev", "donbas", "crimea", "russian invasion", "ukraine war"],
    "Iran Israel war": ["iran", "israel", "tehran", "tel aviv", "houthis", "hezbollah", "hormuz", "iran war", "israel war", "us israel", "middle east war"],
    "Taiwan Strait conflict": ["taiwan", "taipei", "beijing", "china taiwan", "taiwan strait", "cross strait", "taiwanese", "pla navy", "taiwan independence"]
}

def load_state():
    if os.path.exists(OUTPUT_PKL):
        df = pd.read_pickle(OUTPUT_PKL)
        df['published_date'] = pd.to_datetime(df['published_date'], errors='coerce')
        return df, set(df['url'].dropna().tolist())
    cols = ["published_date", "topic", "source", "region", "title", "url", "full_text"]
    return pd.DataFrame(columns=cols), set()

def get_topic(title: str, text: str):
    combined = (title + " " + text[:2000]).lower()
    for topic_name, keywords in TOPICS.items():
        if any(kw in combined for kw in keywords):
            return topic_name
    return None

def parse_article(html: str):
    try:
        soup = BeautifulSoup(html, "html.parser")
        title = soup.find("h1").get_text(strip=True) if soup.find("h1") else "No title"

        published_date = None
        meta = soup.find("meta", {"property": "article:published_time"})
        if meta and meta.get("content"):
            try:
                published_date = datetime.fromisoformat(meta["content"].replace("Z", "+00:00"))
            except:
                pass

        if not published_date:
            match = re.search(r'(\d{1,2}\s+[A-Za-z]+\s+202[5-6])', soup.get_text())
            if match:
                try:
                    published_date = datetime.strptime(match.group(1), "%d %B %Y")
                except:
                    pass

        # Full text
        selectors = ["article", "div.article-content", "div.content", "div.post-content", ".entry-content"]
        full_text = ""
        for sel in selectors:
            container = soup.select_one(sel)
            if container:
                paragraphs = container.find_all("p")
                cleaned = [p.get_text(strip=True) for p in paragraphs if len(p.get_text(strip=True)) > 40]
                if cleaned:
                    full_text = "\n\n".join(cleaned)
                    break

        full_text = ftfy.fix_text(full_text)
        full_text = re.sub(r'\s+', ' ', full_text).strip()

        if published_date is None and len(full_text) > 300:
            published_date = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)

        return title, published_date, full_text
    except:
        return "Parse Error", None, ""

async def human_behavior(page):
    await page.wait_for_timeout(random.randint(1500, 4000))
    for _ in range(4):
        await page.evaluate("window.scrollBy(0, window.innerHeight * 0.7)")
        await page.wait_for_timeout(random.randint(800, 2000))
    await page.mouse.move(random.randint(200, 1000), random.randint(200, 700))
    await page.wait_for_timeout(random.randint(600, 1500))

# ================= MAIN =================
async def main():
    df_old, existing_urls = load_state()
    all_new = []

    print("🚀 Starting Dhaka Tribune v2 - Stronger Stealth + Better Link Detection...\n")

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=False)
        context = await browser.new_context(
            viewport={"width": 1366, "height": 900},
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36",
        )

        await context.add_init_script("""
            Object.defineProperty(navigator, 'webdriver', {get: () => undefined});
            Object.defineProperty(navigator, 'plugins', {get: () => [1,2,3,4,5]});
        """)

        page = await context.new_page()

        page_num = 1
        while page_num <= MAX_PAGES:
            url = f"{BASE_URL}?page={page_num}" if page_num > 1 else BASE_URL
            print(f"📄 Loading page {page_num}...")

            await page.goto(url, wait_until="domcontentloaded", timeout=90000)
            await human_behavior(page)        # Scroll and move mouse

            html = await page.content()
            soup = BeautifulSoup(html, "html.parser")

            # === BROADER LINK EXTRACTION ===
            links = []
            for a in soup.select('a[href]'):
                href = a.get("href", "")
                if not href:
                    continue
                if href.startswith('/world/') or '/world/' in href:
                    full_url = "https://www.dhakatribune.com" + href if href.startswith("/") else href
                    # Filter reasonable article URLs
                    if len(full_url) > 50 and full_url.count('/') >= 4 and full_url not in existing_urls and full_url not in links:
                        links.append(full_url)

            links = list(dict.fromkeys(links))[:25]
            print(f"   Found {len(links)} potential article links")

            if len(links) == 0:
                print("   ❌ Still 0 links. Cloudflare or structure changed again.")
                print("   Try closing browser and running again, or we move to FlareSolverr.")
                break

            for link in tqdm(links, desc=f"Page {page_num}"):
                try:
                    await page.goto(link, wait_until="domcontentloaded", timeout=60000)
                    await human_behavior(page)

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
                        "source": "Dhaka Tribune",
                        "region": "World",
                        "title": title,
                        "url": link,
                        "full_text": full_text[:25000]
                    })

                    print(f"   ✅ Added [{topic}] {title[:65]}...")

                except Exception as e:
                    print(f"   Skip: {e}")

                await page.wait_for_timeout(random.randint(2500, 5000))

            page_num += 1
            await page.wait_for_timeout(random.randint(6000, 12000))

        await browser.close()

    if all_new:
        new_df = pd.DataFrame(all_new)
        combined = pd.concat([df_old, new_df], ignore_index=True)
        combined = combined.drop_duplicates(subset=['url'])
        combined['published_date'] = pd.to_datetime(combined['published_date'])
        combined = combined.sort_values(by='published_date', ascending=False)

        combined.to_csv(OUTPUT_CSV, index=False, encoding="utf-8-sig")
        combined.to_pickle(OUTPUT_PKL)
        print(f"\n🎉 Success! Added {len(new_df)} articles.")
    else:
        print("\nStill no articles. We may need FlareSolverr next.")

if __name__ == "__main__":
    asyncio.run(main())