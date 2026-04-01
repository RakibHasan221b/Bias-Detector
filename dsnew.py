# dailystar_world_scraper.py
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
BASE_URL = "https://www.thedailystar.net/news/world"
OUTPUT_CSV = "dailystar_news.csv"
OUTPUT_PKL = "dailystar_news.pkl"
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

if os.path.exists(OUTPUT_PKL):
    with open(OUTPUT_PKL, "rb") as f:
        articles_df = pickle.load(f)
else:
    articles_df = pd.DataFrame(columns=["published_date", "topic", "source", "region", "title", "url", "full_text"])

existing_urls = set(articles_df['url'].tolist())

def get_topic(title: str, text: str) -> str | None:
    if not title and not text:
        return None
    combined = (title + " " + text[:1500]).lower()
    for topic_name, keywords in TOPICS.items():
        if any(kw in combined for kw in keywords):
            return topic_name
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

        # ==================== DATE PARSING ====================
        published_date = None
        date_text = ""

        full_page = soup.get_text()

        # Prioritize UPDATED line first (most reliable)
        match = re.search(r'UPDATED\s*(\d{1,2}\s+[A-Za-z]+\s+2026(?:,\s*\d{1,2}:\d{2}\s*[AP]M)?)', 
                         full_page, re.IGNORECASE)
        
        if match:
            date_text = match.group(1).strip()
        else:
            # Fallback: find the first occurrence of "dd Month 2026" 
            # This helps catch February, March, etc. reliably
            matches = re.findall(r'(\d{1,2}\s+[A-Za-z]+\s+2026(?:,\s*\d{1,2}:\d{2}\s*[AP]M)?)', 
                                full_page, re.IGNORECASE)
            if matches:
                date_text = matches[0].strip()

        print(f"   📅 RAW DATE FOUND: '{date_text}'")

        if date_text:
            # Clean date string
            clean = re.sub(r'^UPDATED\s+', '', date_text, flags=re.IGNORECASE).strip()
            clean = clean.split(', UPDATED')[0].strip()
            clean = re.sub(r'\s+', ' ', clean).strip()

            # Parse with multiple formats
            formats = [
                "%d %B %Y, %I:%M %p",
                "%d %B %Y, %H:%M",
                "%d %B %Y"
            ]
            
            for fmt in formats:
                try:
                    published_date = datetime.strptime(clean, fmt)
                    print(f"   ✅ Parsed successfully as {published_date.strftime('%Y-%m-%d')}")
                    break
                except ValueError:
                    continue

            # Fallback: remove AM/PM if it causes parsing error
            if published_date is None and ("AM" in clean or "PM" in clean):
                clean_no_ampm = re.sub(r'\s+[AP]M$', '', clean)
                try:
                    published_date = datetime.strptime(clean_no_ampm, "%d %B %Y, %H:%M")
                    print(f"   ✅ Parsed successfully (AM/PM removed) as {published_date.strftime('%Y-%m-%d')}")
                except:
                    pass

        # ==================== FULL TEXT EXTRACTION ====================
        selectors = ["div.article-content", "div.entry-content", "div.td-post-content", "article"]
        full_text = ""

        for sel in selectors:
            container = soup.select_one(sel)
            if container:
                paragraphs = container.find_all("p")
                cleaned_paras = []
                for p in paragraphs:
                    text = p.get_text(strip=True)
                    if len(text) > 40 and "Google News" not in text and "Follow" not in text:
                        cleaned_paras.append(text)
                if cleaned_paras:
                    full_text = "\n\n".join(cleaned_paras)
                    break

        if not full_text:
            paragraphs = soup.find_all("p")
            cleaned_paras = []
            for p in paragraphs:
                text = p.get_text(strip=True)
                if len(text) > 40 and "Google News" not in text and "Follow" not in text:
                    cleaned_paras.append(text)
            full_text = "\n\n".join(cleaned_paras)

        # ==================== CLEAN FULL TEXT ====================
        full_text = ftfy.fix_text(full_text)
        full_text = re.sub(r'[\u200b\u200c\u200d\u2060\xa0]', ' ', full_text)
        full_text = re.sub(r'\s+', ' ', full_text).strip()

        # ==================== CLEAN TITLE ====================
        title = ftfy.fix_text(title)
        title = re.sub(r'[\u200b\u200c\u200d\u2060\xa0]', ' ', title)
        title = re.sub(r'\s+', ' ', title).strip()

        # Fallback date if parsing failed but content is good
        if published_date is None and len(full_text) > 400:
            published_date = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
            print("   ⚠️ Using fallback date (today)")

        return title, published_date, full_text

    except Exception as e:
        print(f"   ⚠️ Error parsing {url}: {e}")
        return "Parse Error", None, ""

# Main scraping loop
all_new_articles = []
page = 1
max_pages = 50
empty_pages_in_row = 0
stop_after_empty_pages = 5

print("🚀 Starting scrape...")
print(f"   Will stop before {START_DATE.date()}\n")

while page <= max_pages:
    url = f"{BASE_URL}?page={page}"
    try:
        res = requests.get(url, headers=HEADERS, timeout=15)
        soup = BeautifulSoup(res.text, "html.parser")
        links = []
        for a in soup.select('h3 a[href], h2 a[href], article h3 a, .article-title a'):
            href = a.get('href')
            if href and href.startswith('/'):
                href = "https://www.thedailystar.net" + href
            if "/news/" in href and href not in existing_urls and href not in links:
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
                "source": "The Daily Star",
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