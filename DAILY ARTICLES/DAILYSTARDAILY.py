# DAILYSTAR_NEWS_DAILY.PY
import requests
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime, timedelta
import time
import os
import re
import ftfy
import random

# ========================= CONFIG =========================
BASE_URL = "https://www.thedailystar.net/news/world"
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

BASE_PATH = os.path.join(BASE_DIR, "..", "Data")   # go one folder up
BASE_PATH = os.path.abspath(BASE_PATH)

OUTPUT_CSV = os.path.join(BASE_PATH, "dailystar_news.csv")

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36"
}

TOPICS = {
    "Russia Ukraine war": ["russia", "ukraine", "putin", "zelensky", "kyiv", "kiev", "donbas", "crimea"],
    "Iran Israel war": ["iran", "israel", "tehran", "tel aviv", "houthis", "hezbollah", "hormuz", "iran war", "middle east war"],
    "Taiwan Strait conflict": ["taiwan strait", "taiwan invasion", "blockade taiwan", "invade taiwan",
                               "taiwan military", "china taiwan strait", "cross-strait"]
}

OPINION_KEYWORDS = ["opinion", "editorial", "analysis", "commentary", "op-ed", "column"]

# ====================== LOAD CSV ======================
if os.path.exists(OUTPUT_CSV):
    df = pd.read_csv(OUTPUT_CSV)
    df['published_date'] = pd.to_datetime(df['published_date'], format="%d-%m-%y", errors='coerce')
    print(f"Loaded {len(df)} existing articles from CSV.")
    print(f"Latest date in CSV: {df['published_date'].max().strftime('%d-%m-%Y')}")
else:
    df = pd.DataFrame(columns=["published_date", "topic", "source", "region", "title", "url", "full_text"])
    print("CSV not found!")

existing_urls = set(df['url'].tolist())
latest_date = df['published_date'].max().date() if len(df) > 0 else datetime(2025, 1, 1).date()

# ====================== STRONGER DATE PARSER ======================
def parse_date(full_text: str):
    text_head = full_text[:6000]  # more text

    # Stronger patterns - prioritize UPDATED
    patterns = [
        r'UPDATED\s*(\d{1,2}\s+[A-Za-z]+\s+202[6](?:,\s*\d{1,2}:\d{2}\s*[AP]M)?)',
        r'(\d{1,2}\s+[A-Za-z]+\s+2026,\s*\d{1,2}:\d{1,2}\s*[AP]M)',
        r'(\d{1,2}\s+[A-Za-z]+\s+2026)'
    ]

    for pat in patterns:
        match = re.search(pat, text_head, re.IGNORECASE | re.DOTALL)
        if match:
            date_str = (match.group(1) or match.group(0)).strip()
            date_str = re.sub(r'^UPDATED\s*', '', date_str, flags=re.I).strip()
            date_str = re.sub(r',\s*\d{1,2}:\d{1,2}\s*[AP]M.*$', '', date_str, flags=re.I).strip()

            for fmt in ["%d %B %Y", "%d %b %Y"]:
                try:
                    dt = datetime.strptime(date_str, fmt)
                    # Only accept 2026 dates (safety)
                    if dt.year == 2026:
                        return dt.replace(hour=0, minute=0, second=0, microsecond=0)
                except ValueError:
                    continue
    return None

# ====================== HELPERS (unchanged) ======================
def is_opinion_piece(title: str, url: str) -> bool:
    title_lower = (title or "").lower()
    url_lower = url.lower()
    return any(kw in title_lower for kw in OPINION_KEYWORDS) or any(kw in url_lower for kw in ['/opinion', '/editorial', '/analysis', '/column', '/view'])

def get_topic(title: str, text: str) -> str | None:
    combined = (title + " " + text[:2000]).lower()
    for topic_name, keywords in TOPICS.items():
        if any(kw in combined for kw in keywords):
            return topic_name
    return None

def parse_article(url: str):
    try:
        res = requests.get(url, headers=HEADERS, timeout=15)
        res.raise_for_status()
        soup = BeautifulSoup(res.text, "html.parser")

        title_tag = soup.find("h1") or soup.find("title")
        title = title_tag.get_text(strip=True).split("|")[0].strip() if title_tag else "No title"

        full_page = soup.get_text()
        published_date = parse_date(full_page)

        selectors = ["div.article-content", "div.entry-content", "article"]
        full_text = ""
        for sel in selectors:
            container = soup.select_one(sel)
            if container:
                ps = [p.get_text(strip=True) for p in container.find_all("p") if len(p.get_text(strip=True)) > 40]
                full_text = "\n\n".join(ps)
                break
        if not full_text:
            full_text = "\n\n".join([p.get_text(strip=True) for p in soup.find_all("p") if len(p.get_text(strip=True)) > 40])

        full_text = ftfy.fix_text(full_text)
        full_text = re.sub(r'\s+', ' ', full_text).strip()
        title = ftfy.fix_text(title).strip()

        return title, published_date, full_text

    except Exception as e:
        print(f"   Error parsing {url}: {e}")
        return None, None, ""

# ====================== MAIN ======================
def main():
    new_articles = []
    page = 1
    max_pages = 25
    consecutive_old = 0

    print(f"\nStarting daily scrape - Latest known: {latest_date.strftime('%d-%m-%Y')}\n")

    while page <= max_pages:
        try:
            res = requests.get(f"{BASE_URL}?page={page}", headers=HEADERS, timeout=15)
            soup = BeautifulSoup(res.text, "html.parser")

            links = []
            for a in soup.select('h2 a[href], h3 a[href], .article-title a'):
                href = a.get('href')
                if not href: continue
                if href.startswith('/'):
                    href = "https://www.thedailystar.net" + href
                if href in existing_urls: continue
                if "/news/world" not in href and "/news/" not in href: continue
                if any(x in href.lower() for x in ['/bangladesh', '/dhaka', '/metro', '/opinion']): continue
                links.append(href)

            print(f"📄 Page {page}: {len(links)} new candidate links")

            if not links and page > 6:
                break

            for link_url in links:
                title, pub_date, full_text = parse_article(link_url)
                
                if pub_date is None or not full_text.strip():
                    continue
                if is_opinion_piece(title, link_url):
                    continue

                if pub_date.date() < (latest_date - timedelta(days=5)):
                    consecutive_old += 1
                    print(f"   🕒 Old: {pub_date.strftime('%d-%m-%Y')} | {title[:60]}...")
                else:
                    consecutive_old = 0
                    if link_url not in existing_urls:
                        matched_topic = get_topic(title, full_text)
                        if matched_topic:
                            new_articles.append({
                                "published_date": pub_date,
                                "topic": matched_topic,
                                "source": "The Daily Star",
                                "region": "World",
                                "title": title,
                                "url": link_url,
                                "full_text": full_text
                            })
                            print(f"   ✅ [{matched_topic}] {pub_date.strftime('%d-%m-%Y')} | {title[:65]}...")

                time.sleep(random.uniform(0.3, 0.55))

            if consecutive_old >= 8:   # lower threshold now
                print(f"⏹️ Stopping - reached old articles (page {page})")
                break

            page += 1
            time.sleep(1.0)

        except Exception as e:
            print(f"Page {page} error: {e}")
            break

    return new_articles

# ====================== SAVE ======================
if __name__ == "__main__":
    collected = main()

    if collected:
        new_df = pd.DataFrame(collected)
        combined = pd.concat([df, new_df], ignore_index=True).drop_duplicates(subset=['url'])

        combined['published_date'] = pd.to_datetime(combined['published_date'])
        topic_order = {"Russia Ukraine war": 1, "Iran Israel war": 2, "Taiwan Strait conflict": 3}
        combined['topic_order'] = combined['topic'].map(topic_order).fillna(999)

        combined = combined.sort_values(by=['published_date', 'topic_order'], ascending=[False, True])
        combined = combined.drop(columns=['topic_order'])
        combined['published_date'] = combined['published_date'].dt.strftime("%d-%m-%y")

        combined.to_csv(OUTPUT_CSV, index=False)
        print(f"\n✅ DONE! Added {len(new_df)} new articles. Total: {len(combined)}")
    else:
        print("\n✅ No new articles found today.")