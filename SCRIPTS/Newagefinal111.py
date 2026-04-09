# NEWAGE_NEWS_FINAL.PY
import requests
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime
import time
import os
import pickle
import re
import ftfy
from tqdm import tqdm
import random
from zoneinfo import ZoneInfo
import signal
import sys

# ========================= CONFIG =========================
BASE_URL = "https://www.newagebd.net/articlelist/31/world"
OUTPUT_CSV = "newage_news.csv"
OUTPUT_PKL = "newage_news.pkl"
LAST_PAGE_PKL = "newage_last_page.pkl"

CUTOFF_DATE = datetime(2025, 6, 1).date()

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Referer": "https://www.newagebd.net/",
}

# ==================== TOPIC SCORING ====================
TOPICS = {
    "Russia Ukraine war": {
        "core": ["ukraine war", "russian invasion", "kyiv strike", "zelensky", "putin", "donbas", "crimea", "russian forces", "ukrainian army"],
        "support": ["russia", "ukraine", "kiev", "zelenskyy"]
    },
    "Iran Israel war": {
        "core": ["iran retaliates", "israel attacks", "israeli strike", "iranian missile", "tehran strikes", "netanyahu", "hezbollah", "houthis", "iran war", "israel war"],
        "support": ["iran", "israel", "tehran", "tel aviv", "middle east war", "gulf", "us israel"]
    },
    "Taiwan Strait conflict": {
        "core": ["taiwan strait", "pla navy", "taiwan incursion", "military drill", "chinese warship", "pla aircraft", "taiwan blockade", "cross strait tension", "beijing threatens taiwan"],
        "support": ["taiwan military", "taiwan independence", "taiwanese defense", "us taiwan", "taiwan arms"]
    }
}

OPINION_KEYWORDS = ["opinion", "editorial", "analysis", "commentary", "op-ed", "column"]

# ====================== LOAD DATA ======================
if os.path.exists(OUTPUT_PKL):
    with open(OUTPUT_PKL, "rb") as f:
        articles_df = pickle.load(f)
    print(f"Loaded {len(articles_df)} existing articles.")
else:
    articles_df = pd.DataFrame(columns=["published_date", "topic", "source", "region", "title", "url", "full_text"])

existing_urls = set(articles_df['url'].tolist())

start_page = 1
if os.path.exists(LAST_PAGE_PKL):
    with open(LAST_PAGE_PKL, "rb") as f:
        start_page = pickle.load(f) + 1
    print(f"Resuming from page {start_page}")

# ====================== FILTERS ======================
def is_opinion_piece(title: str, url: str) -> bool:
    title_lower = title.lower()
    url_lower = url.lower()
    if any(kw in title_lower for kw in OPINION_KEYWORDS):
        return True
    if any(kw in url_lower for kw in ['/opinion', '/editorial', '/analysis', '/column', '/view']):
        return True
    return False

def get_topic(title: str, text: str) -> str | None:
    if not title and not text:
        return None
    combined = (title + " " + text[:2500]).lower()
    best_topic = None
    best_score = 0
    for topic_name, data in TOPICS.items():
        core_hits = sum(2 for kw in data["core"] if kw in combined)
        support_hits = sum(1 for kw in data["support"] if kw in combined)
        total_score = core_hits + support_hits
        if total_score > best_score:
            best_score = total_score
            best_topic = topic_name
    return best_topic if best_score >= 3 else None

# ====================== ARTICLE PARSER ======================
def parse_article(url: str):
    try:
        res = requests.get(url, headers=HEADERS, timeout=15)
        res.raise_for_status()
        soup = BeautifulSoup(res.text, "html.parser")

        title_tag = soup.find("h1") or soup.find("title")
        title = title_tag.get_text(strip=True) if title_tag else "No title"
        if "|" in title:
            title = title.split("|")[0].strip()

        published_date = None
        full_page = soup.get_text()
        matches = re.findall(r'(\d{1,2}\s+[A-Za-z]+\s*,\s*202[5-6](?:,\s*\d{1,2}:\d{2})?)', full_page)
        if matches:
            date_text = matches[0].strip()
            clean = re.sub(r'\s+', ' ', date_text).strip()
            formats = ["%d %B, %Y, %H:%M", "%d %B, %Y", "%d %B %Y, %H:%M", "%d %B %Y"]
            for fmt in formats:
                try:
                    dt = datetime.strptime(clean, fmt)
                    published_date = dt.date()
                    break
                except ValueError:
                    continue

        selectors = ["div.article-content", "div.post-content", "div.entry-content", "article", "div.content"]
        full_text = ""
        for sel in selectors:
            container = soup.select_one(sel)
            if container:
                paragraphs = container.find_all("p")
                cleaned_paras = [p.get_text(strip=True) for p in paragraphs 
                                if len(p.get_text(strip=True)) > 40 
                                and "Google News" not in p.get_text() 
                                and "Follow" not in p.get_text()]
                if cleaned_paras:
                    full_text = "\n\n".join(cleaned_paras)
                    break
        if not full_text:
            paragraphs = soup.find_all("p")
            cleaned_paras = [p.get_text(strip=True) for p in paragraphs if len(p.get_text(strip=True)) > 50]
            full_text = "\n\n".join(cleaned_paras)

        full_text = ftfy.fix_text(full_text)
        full_text = re.sub(r'[\u200b\u200c\u200d\u2060\xa0]', ' ', full_text)
        full_text = re.sub(r'\s+', ' ', full_text).strip()
        title = ftfy.fix_text(title)
        title = re.sub(r'\s+', ' ', title).strip()

        return title, published_date, full_text

    except Exception:
        return None, None, ""

# ====================== SAFE SAVE ======================
def safe_save_csv(df, filename):
    try:
        df.to_csv(filename, index=False)
    except PermissionError:
        alt_name = filename.replace(".csv", "_new.csv")
        print(f"⚠️ File open. Saving as {alt_name}")
        df.to_csv(alt_name, index=False)

# ====================== SORT FUNCTION - FIXED ======================
def sort_df(df):
    if df.empty:
        return df
    
    df = df.copy()
    
    # Convert to datetime safely
    df['published_date'] = pd.to_datetime(df['published_date'], errors='coerce')
    
    # Remove rows with missing dates (this prevents the NaT error)
    df = df.dropna(subset=['published_date'])
    
    # Now convert to date
    df['published_date'] = df['published_date'].dt.date
    
    topic_order = {
        "Russia Ukraine war": 1,
        "Iran Israel war": 2,
        "Taiwan Strait conflict": 3
    }
    df['topic_order'] = df['topic'].map(topic_order).fillna(999)
    
    df = df.sort_values(
        by=['published_date', 'topic_order'],
        ascending=[False, True]
    ).drop(columns=['topic_order'])
    
    # Safe strftime - only apply to valid dates
    df['published_date'] = df['published_date'].apply(lambda x: x.strftime("%d-%m-%Y") if pd.notna(x) else "")
    
    return df

# ====================== CTRL+C HANDLER ======================
collected_articles = []
current_page = start_page

def signal_handler(sig, frame):
    print("\n\n⚠️ Interrupted! Saving progress...")
    if collected_articles:
        new_df = pd.DataFrame(collected_articles)
        combined = pd.concat([articles_df, new_df], ignore_index=True).drop_duplicates(subset=['url'])
        combined = sort_df(combined)   # now safe
        
        safe_save_csv(combined, OUTPUT_CSV)
        with open(OUTPUT_PKL, "wb") as f:
            pickle.dump(combined, f)
        with open(LAST_PAGE_PKL, "wb") as f:
            pickle.dump(current_page - 1, f)
        print(f"✅ Saved {len(new_df)} new articles.")
    else:
        print("No new articles to save.")
    sys.exit(0)

signal.signal(signal.SIGINT, signal_handler)

# ====================== MAIN ======================
def main():
    global current_page
    seen_this_run = set(existing_urls)
    stop_scraping = False

    print(f"Today in Bangladesh: {datetime.now(ZoneInfo('Asia/Dhaka')).strftime('%d-%m-%Y')}")
    print("Starting New Age scraper...\n")

    while True:
        try:
            url = f"{BASE_URL}?page={current_page}" if current_page > 1 else BASE_URL
            res = requests.get(url, headers=HEADERS, timeout=15)
            soup = BeautifulSoup(res.text, "html.parser")

            links = []
            for a in soup.select('h2 a, h3 a, .news-title a, article a, a[href*="/post/"]'):
                href = a.get('href')
                if not href: continue
                if href.startswith('/'):
                    href = "https://www.newagebd.net" + href
                if "/post/" not in href or href in seen_this_run: continue
                links.append(href)
                seen_this_run.add(href)

            print(f"📄 Page {current_page}: {len(links)} new potential articles")

            if not links and current_page > 10:
                print("No more articles found.")
                break

        except Exception as e:
            print(f"Page {current_page} error: {e}")
            break

        for link_url in tqdm(links, desc=f"Page {current_page}", leave=False):
            if is_opinion_piece("", link_url):
                continue

            title, pub_date, full_text = parse_article(link_url)

            if pub_date is None or not full_text.strip():
                continue

            if is_opinion_piece(title, link_url):
                continue

            if pub_date < CUTOFF_DATE:
                print("⏹️ Cutoff date (1 June 2025) reached. Stopping...")
                stop_scraping = True
                break

            matched_topic = get_topic(title, full_text)
            if not matched_topic:
                continue

            collected_articles.append({
                "published_date": pub_date,
                "topic": matched_topic,
                "source": "New Age",
                "region": "World",
                "title": title,
                "url": link_url,
                "full_text": full_text
            })

            print(f"   ✅ [{matched_topic}] {pub_date.strftime('%d-%m-%Y')} | {title[:70]}...")

            time.sleep(random.uniform(0.3, 0.6))

        if stop_scraping:
            break

        current_page += 1
        time.sleep(0.6)

    if collected_articles:
        new_df = pd.DataFrame(collected_articles)
        combined_df = pd.concat([articles_df, new_df], ignore_index=True).drop_duplicates(subset=['url'])
        combined_df = sort_df(combined_df)   # now safe from NaT error

        safe_save_csv(combined_df, OUTPUT_CSV)
        with open(OUTPUT_PKL, "wb") as f:
            pickle.dump(combined_df, f)

        if os.path.exists(LAST_PAGE_PKL):
            os.remove(LAST_PAGE_PKL)

        print(f"\n✅ Done! Added {len(new_df)} new articles. Total: {len(combined_df)}")
    else:
        print("\nNo new valid articles found.")

if __name__ == "__main__":
    main()