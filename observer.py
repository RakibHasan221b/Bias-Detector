# daily_observer_scraper.py
import requests
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime
import time
import os
import pickle
import re
from tqdm import tqdm
import ftfy

# ================= CONFIG =================
BASE_URL = "https://www.observerbd.com/menu/198"   # Foreign News / International
OUTPUT_CSV = "daily_observer_news.csv"
OUTPUT_PKL = "daily_observer_news.pkl"
START_DATE = datetime(2025, 6, 1)

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36"
}

TOPICS = {
    "Russia Ukraine war": ["russia", "ukraine", "putin", "zelensky", "kyiv", "kiev", "donbas", "crimea", "russian invasion", "ukraine war"],
    "Iran Israel war": ["iran", "israel", "tehran", "tel aviv", "houthis", "hezbollah", "hormuz", "iran war", "israel war", "us israel", "middle east war"],
    "Taiwan Strait conflict": ["taiwan", "taipei", "beijing", "china taiwan", "taiwan strait", "cross strait", "taiwanese", "pla navy", "taiwan independence"]
}

# Load existing
if os.path.exists(OUTPUT_PKL):
    with open(OUTPUT_PKL, "rb") as f:
        articles_df = pickle.load(f)
else:
    articles_df = pd.DataFrame(columns=["published_date", "topic", "source", "region", "title", "url", "full_text"])

existing_urls = set(articles_df['url'].tolist())

def get_topic(title: str, text: str):
    if not title and not text:
        return None
    combined = (title + " " + text[:2000]).lower()
    for topic_name, keywords in TOPICS.items():
        if any(kw in combined for kw in keywords):
            return topic_name
    return None

def parse_article(url: str):
    try:
        res = requests.get(url, headers=HEADERS, timeout=20)
        res.raise_for_status()
        soup = BeautifulSoup(res.text, "html.parser")

        title_tag = soup.find("h1") or soup.find("h2")
        title = title_tag.get_text(strip=True) if title_tag else "No title"

        # Date
        published_date = None
        full_raw = soup.get_text()
        match = re.search(r'(\d{1,2}\s+[A-Za-z]+\s+202[5-6])', full_raw)
        if match:
            try:
                published_date = datetime.strptime(match.group(1), "%d %B %Y")
            except:
                pass

        # Full text
        selectors = ["div.news-details", "div.article-content", "article", "div.details"]
        full_text = ""
        for sel in selectors:
            container = soup.select_one(sel)
            if container:
                paragraphs = container.find_all("p")
                cleaned = [p.get_text(strip=True) for p in paragraphs if len(p.get_text(strip=True)) > 40]
                if cleaned:
                    full_text = "\n\n".join(cleaned)
                    break

        if not full_text:
            paragraphs = soup.find_all("p")
            full_text = "\n\n".join([p.get_text(strip=True) for p in paragraphs if len(p.get_text(strip=True)) > 40])

        full_text = ftfy.fix_text(full_text)
        full_text = re.sub(r'\s+', ' ', full_text).strip()

        if published_date is None and len(full_text) > 300:
            published_date = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)

        return title, published_date, full_text

    except Exception as e:
        print(f"   ⚠️ Error parsing {url}: {e}")
        return "Parse Error", None, ""

# ================= MAIN =================
all_new_articles = []
page = 1
max_pages = 40
empty_count = 0

print("🚀 Starting The Daily Observer scraper...\n")

while page <= max_pages:
    url = f"{BASE_URL}/{page}" if page > 1 else BASE_URL
    print(f"📄 Page {page}: {url}")

    try:
        res = requests.get(url, headers=HEADERS, timeout=15)
        soup = BeautifulSoup(res.text, "html.parser")

        links = []
        for a in soup.select('a[href]'):
            href = a.get('href')
            if href and '/news.php?id=' in href:          # Observer uses this pattern
                full_url = "https://www.observerbd.com" + href if href.startswith('/') else href
                if full_url not in existing_urls and full_url not in links:
                    links.append(full_url)

        print(f"   Found {len(links)} new links")

    except Exception as e:
        print(f"❌ Page error: {e}")
        break

    if not links:
        empty_count += 1
        if empty_count >= 5:
            print("⏹️ Stopping - no more pages")
            break
    else:
        empty_count = 0

    for link in tqdm(links, desc=f"Page {page}"):
        title, pub_date, full_text = parse_article(link)

        if pub_date is None:
            if len(full_text) > 300:
                pub_date = datetime.now()
            else:
                continue

        if pub_date < START_DATE:
            print("⏹️ Reached old articles. Stopping.")
            page = max_pages + 1
            break

        topic = get_topic(title, full_text)
        if not topic:
            continue

        all_new_articles.append({
            "published_date": pub_date.strftime("%d-%m-%y"),
            "topic": topic,
            "source": "The Daily Observer",
            "region": "World",
            "title": title,
            "url": link,
            "full_text": full_text
        })

        print(f"   ✅ SAVED [{topic}] {title[:80]}")
        time.sleep(1.2)

    page += 1

# Save
if all_new_articles:
    new_df = pd.DataFrame(all_new_articles)
    articles_df = pd.concat([articles_df, new_df], ignore_index=True)
    articles_df = articles_df.drop_duplicates(subset=['url'])

    articles_df.to_csv(OUTPUT_CSV, index=False)
    with open(OUTPUT_PKL, "wb") as f:
        pickle.dump(articles_df, f)

    print(f"\n🎉 Added {len(new_df)} articles. Total: {len(articles_df)}")
else:
    print("No new articles added.")

print("Finished.")