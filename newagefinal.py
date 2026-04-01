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

# Configuration
BASE_URL = "https://www.newagebd.net/articlelist/31/world"
OUTPUT_CSV = "newage_news.csv"
OUTPUT_PKL = "newage_news.pkl"
START_DATE = datetime(2025, 6, 1)

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Referer": "https://www.newagebd.net/",
}

# ==================== STRICT & CLEVER TOPIC SCORING ====================
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

if os.path.exists(OUTPUT_PKL):
    with open(OUTPUT_PKL, "rb") as f:
        articles_df = pickle.load(f)
else:
    articles_df = pd.DataFrame(columns=["published_date", "topic", "source", "region", "title", "url", "full_text"])

existing_urls = set(articles_df['url'].tolist())

def get_topic(title: str, text: str) -> str | None:
    if not title and not text:
        return None
    
    combined = (title + " " + text[:2500]).lower()
    
    best_topic = None
    best_score = 0
    
    for topic_name, data in TOPICS.items():
        core_hits = sum(2 for kw in data["core"] if kw in combined)   # core words give more points
        support_hits = sum(1 for kw in data["support"] if kw in combined)
        total_score = core_hits + support_hits
        
        if total_score > best_score:
            best_score = total_score
            best_topic = topic_name
    
    # Stricter minimum: needs real context (at least one core + one support, or multiple cores)
    if best_score >= 3:
        return best_topic
    return None

def parse_article(url: str):
    try:
        res = requests.get(url, headers=HEADERS, timeout=15)
        res.raise_for_status()
        soup = BeautifulSoup(res.text, "html.parser")
        
        # Extract Title
        title_tag = soup.find("h1") or soup.find("title")
        title = title_tag.get_text(strip=True) if title_tag else "No title"
        if "|" in title:
            title = title.split("|")[0].strip()

        # Date Parsing
        published_date = None
        date_text = ""
        full_page = soup.get_text()
        matches = re.findall(r'(\d{1,2}\s+[A-Za-z]+\s*,\s*202[5-6](?:,\s*\d{1,2}:\d{2})?)', full_page)
        if matches:
            date_text = matches[0].strip()

        print(f"   📅 RAW DATE FOUND: '{date_text}'")

        if date_text:
            clean = re.sub(r'\s+', ' ', date_text).strip()
            formats = ["%d %B, %Y, %H:%M", "%d %B, %Y", "%d %B %Y, %H:%M", "%d %B %Y"]
            for fmt in formats:
                try:
                    published_date = datetime.strptime(clean, fmt)
                    print(f"   ✅ Parsed successfully as {published_date.strftime('%Y-%m-%d')}")
                    break
                except ValueError:
                    continue

        # Full Text Extraction
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

        if published_date is None and len(full_text) > 400:
            published_date = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)

        return title, published_date, full_text

    except Exception as e:
        print(f"   ⚠️ Error parsing {url}: {e}")
        return "Parse Error", None, ""

# ====================== MAIN LOOP (unchanged) ======================
all_new_articles = []
page = 1
max_pages = 60
empty_pages_in_row = 0
stop_after_empty_pages = 5

print("🚀 Starting New Age scraper with STRICT topic scoring...")
print(f"   Will stop before {START_DATE.date()}\n")

while page <= max_pages:
    url = f"{BASE_URL}?page={page}" if page > 1 else BASE_URL
    try:
        res = requests.get(url, headers=HEADERS, timeout=15)
        soup = BeautifulSoup(res.text, "html.parser")
        links = []
        for a in soup.select('h2 a, h3 a, .news-title a, article a, a[href*="/post/"]'):
            href = a.get('href')
            if href:
                if href.startswith('/'):
                    href = "https://www.newagebd.net" + href
                if "/post/" in href and href not in existing_urls and href not in links:
                    links.append(href)
        print(f"📄 Page {page}: Found {len(links)} new article links")
    except Exception as e:
        print(f"❌ Error on page {page}: {e}")
        break

    if not links:
        empty_pages_in_row += 1
        if empty_pages_in_row >= stop_after_empty_pages:
            print(f"⏹️ {stop_after_empty_pages} consecutive empty pages. Stopping.")
            break
    else:
        empty_pages_in_row = 0

    for link_url in tqdm(links, desc=f"Page {page}", unit="art", leave=False):
        title, pub_date, full_text = parse_article(link_url)

        if pub_date is None:
            if len(full_text) > 300:
                pub_date = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
            else:
                print(f"   ⚠️ Skipped (no date/content)")
                time.sleep(1.1)
                continue

        matched_topic = get_topic(title, full_text)
        if not matched_topic:
            print(f"   ⏭ SKIPPED (not relevant): {title[:130]}...")
            time.sleep(1.1)
            continue

        if pub_date and pub_date < START_DATE:
            print(f"   ⏹️ Reached old articles before {START_DATE.date()}. Stopping.")
            page = max_pages + 1
            break

        if pub_date and pub_date >= START_DATE and full_text.strip():
            date_str = pub_date.strftime("%d-%m-%y")
            all_new_articles.append({
                "published_date": date_str,
                "topic": matched_topic,
                "source": "New Age",
                "region": "World",
                "title": title,
                "url": link_url,
                "full_text": full_text
            })
            print(f"   ✅ SAVED [{matched_topic}]: {date_str} | {title}")
        else:
            print(f"   ⏭ Skipped")

        time.sleep(1.2)

    page += 1

# Save results
if all_new_articles:
    new_df = pd.DataFrame(all_new_articles)
    articles_df = pd.concat([articles_df, new_df], ignore_index=True)
    articles_df = articles_df.drop_duplicates(subset=['url'])
    articles_df.to_csv(OUTPUT_CSV, index=False)
    with open(OUTPUT_PKL, "wb") as f:
        pickle.dump(articles_df, f)
    print(f"\n🎉 DONE! Added {len(new_df)} articles. Total: {len(articles_df)}")
else:
    print("No new articles added.")

print("Finished.")