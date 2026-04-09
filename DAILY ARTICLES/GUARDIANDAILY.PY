import requests
import pandas as pd
from datetime import datetime
import time
import random
import os
import logging
from bs4 import BeautifulSoup
import re

logging.getLogger('urllib3').setLevel(logging.WARNING)

# ========================= CONFIG =========================
# ========================= CONFIG =========================
BASE_PATH = "./Data"
os.makedirs(BASE_PATH, exist_ok=True)

CSV_FILE = os.path.join(BASE_PATH, "guardian.csv")
PKL_FILE = os.path.join(BASE_PATH, "guardian.pkl")

START_DATE = datetime(2025, 6, 1)
API_KEY = "9942b4c5-ebe7-48d7-9b4f-b2b8c5194aaa"

PAGE_SIZE = 50
MAX_PAGES_PER_TOPIC = 70

topics = {
    "Russia Ukraine war": '"russia ukraine war" OR "ukraine war" OR "russian invasion"',
    "Iran Israel war": '"iran israel" OR "israel iran" OR "iran war"',
    "Taiwan Strait conflict": '"taiwan strait" OR "taiwan conflict" OR "china taiwan"'
}

GUARDIAN = {"name": "The Guardian", "region": "Europe"}

BASE_URL = "https://content.guardianapis.com/search"

def clean_html_text(html):
    if not isinstance(html, str) or len(html) < 100:
        return ""
    soup = BeautifulSoup(html, "html.parser")
    text = soup.get_text(separator=" ")
    text = re.sub(r'</?p[^>]*>', ' ', text)
    text = re.sub(r'</?[^>]+>', ' ', text)
    text = re.sub(r'\s+', ' ', text)
    return text.strip()

def is_relevant(title, clean_text, topic):
    check = (title + " " + clean_text[:700]).lower()
    if topic == "Russia Ukraine war":
        if not ("russia" in check or "russian" in check): return False
        if not ("ukraine" in check or "ukrainian" in check): return False
        if sum(w in check for w in ["iran", "israel", "hormuz", "tehran", "netanyahu"]) >= 2: return False
        return any(w in check for w in ["war", "invasion", "zelenskyy", "putin", "kyiv", "donetsk", "kharkiv"])
    elif topic == "Iran Israel war":
        if not ("iran" in check and ("israel" in check or "israeli" in check)): return False
        if sum(w in check for w in ["zelenskyy", "ukraine", "russia", "putin"]) >= 3: return False
        return True
    elif topic == "Taiwan Strait conflict":
        return "taiwan" in check and ("china" in check or "chinese" in check)
    return False

def load_last_state():
    if os.path.exists(CSV_FILE):
        try:
            df = pd.read_csv(CSV_FILE)
            if not df.empty:
                # Take the first row because CSV is sorted newest first
                last_date_str = df['published_date'].iloc[0]
                last_date = datetime.strptime(last_date_str, "%Y-%m-%d")
                existing_urls = set(df['url'].dropna().unique())
                print(f"🔄 Last date from CSV: {last_date_str}")
                return df, last_date, existing_urls
        except Exception as e:
            print(f"Warning: Could not read CSV for last date: {e}")

    # Fallback
    print(f"🔄 No CSV found. Starting from {START_DATE.date()}")
    return pd.DataFrame(), START_DATE, set()

def scrape_guardian():
    df_old, last_date, existing_urls = load_last_state()
    print(f"🔄 Guardian updating from last known date: {last_date.date()}")

    all_new = []
    visited = set()
    headers = {"User-Agent": "GuardianDailyScraper/1.0"}

    for topic_name, query in topics.items():
        print(f"\n{'═' * 25} {topic_name} {'═' * 25}")
        from_date = last_date.strftime("%Y-%m-%d")
        params_base = {
            "q": query,
            "section": "-commentisfree",
            "from-date": from_date,
            "order-by": "newest",
            "show-fields": "headline,body,trailText",
            "page-size": PAGE_SIZE,
            "api-key": API_KEY
        }

        page = 1
        topic_count = 0

        while page <= MAX_PAGES_PER_TOPIC:
            params = params_base.copy()
            params["page"] = page
            print(f"→ Guardian | {topic_name} | page {page}")

            try:
                r = requests.get(BASE_URL, params=params, headers=headers, timeout=40)

                if r.status_code == 429:
                    print("  Rate limited — sleeping 70s...")
                    time.sleep(70)
                    continue

                if r.status_code == 400:
                    print("  No more new articles (API returned Bad Request)")
                    break

                r.raise_for_status()
                data = r.json()
                results = data.get("response", {}).get("results", [])

                if not results:
                    print("  No more results")
                    break

                for article in results:
                    url = article.get("webUrl")
                    if not url or url in visited or url in existing_urls or "/live/" in url:
                        continue
                    if any(x in url.lower() for x in ["first-thing", "morning-briefing"]):
                        continue

                    visited.add(url)

                    pub_iso = article.get("webPublicationDate")
                    try:
                        pub_dt = datetime.fromisoformat(pub_iso.replace("Z", "+00:00")).replace(tzinfo=None)
                    except:
                        continue

                    # FIXED: Only stop current topic when we reach older date
                    if pub_dt <= last_date:
                        print(f"  → Reached known date ({pub_dt.date()}). Stopping {topic_name}.")
                        break   # Important fix

                    fields = article.get("fields", {})
                    title = fields.get("headline", "").strip()
                    body_html = fields.get("body", "")
                    clean_text = clean_html_text(body_html)

                    if not is_relevant(title, clean_text, topic_name):
                        continue

                    all_new.append({
                        "published_date": pub_dt.strftime("%Y-%m-%d"),
                        "topic": topic_name,
                        "source": GUARDIAN["name"],
                        "region": GUARDIAN["region"],
                        "title": title,
                        "url": url,
                        "full_text": clean_text
                    })
                    topic_count += 1
                    print(f"  ✓ {pub_dt.date()} | {title[:85]}{'…' if len(title) > 85 else ''}")

                time.sleep(random.uniform(2.0, 4.5))
                page += 1

            except Exception as e:
                print(f"  Error on page {page}: {e}")
                break

        print(f"→ {topic_name} finished → {topic_count} new articles")

    return all_new

def save_data(all_new, df_old):
    if not all_new:
        print("\n🟡 No new Guardian articles this run.")
        return

    new_df = pd.DataFrame(all_new)
    combined = pd.concat([df_old, new_df], ignore_index=True) if not df_old.empty else new_df
    combined = combined.drop_duplicates(subset=['url'])
    combined['published_date'] = pd.to_datetime(combined['published_date'])

    topic_order = ["Russia Ukraine war", "Iran Israel war", "Taiwan Strait conflict"]
    combined['topic'] = pd.Categorical(combined['topic'], categories=topic_order, ordered=True)
    combined = combined.sort_values(by=['published_date', 'topic', 'source'], ascending=[False, True, True])

    combined.to_csv(CSV_FILE, index=False, encoding="utf-8-sig")
    combined.to_pickle(PKL_FILE)

    print(f"\n✅ Guardian: Added {len(new_df)} new articles → Total now: {len(combined)}")

def main():
    print(f"🚀 Guardian Daily Scraper Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    df_old, _, _ = load_last_state()
    new_articles = scrape_guardian()
    save_data(new_articles, df_old)
    print("✅ Guardian Daily Scraper finished.")

if __name__ == "__main__":
    main()