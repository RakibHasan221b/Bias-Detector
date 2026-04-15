import sys
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
CSV_FILE = r"C:\Users\rakib\Desktop\NEW desktop\THESIS WORK\Data\guardian.csv"
PKL_FILE = r"C:\Users\rakib\Desktop\NEW desktop\THESIS WORK\Data\guardian.pkl"

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

# ====================== CLEAN TEXT (saved as full_text) ======================
def clean_html_text(html):
    if not isinstance(html, str) or len(html) < 100:
        return ""
    
    soup = BeautifulSoup(html, "html.parser")
    text = soup.get_text(separator=" ")
    
    # Extra aggressive cleaning for Guardian's messy HTML
    text = re.sub(r'</?p[^>]*>', ' ', text)
    text = re.sub(r'</?[^>]+>', ' ', text)
    text = re.sub(r'\s+', ' ', text)
    text = text.strip()
    
    return text

# ====================== STRICT RELEVANCE FILTER ======================
def is_relevant(title, clean_text, topic):
    check = (title + " " + clean_text[:700]).lower()

    if topic == "Russia Ukraine war":
        if not ("russia" in check or "russian" in check): return False
        if not ("ukraine" in check or "ukrainian" in check): return False
        iran_words = ["iran", "israel", "hormuz", "tehran", "netanyahu"]
        if sum(w in check for w in iran_words) >= 2: return False
        war_words = ["war", "invasion", "zelenskyy", "putin", "kyiv", "donetsk", "kharkiv"]
        if any(w in check for w in war_words): return True
        return False

    elif topic == "Iran Israel war":
        if not ("iran" in check and ("israel" in check or "israeli" in check)): return False
        ukraine_words = ["zelenskyy", "ukraine", "russia", "putin"]
        if sum(w in check for w in ukraine_words) >= 3: return False
        return True

    elif topic == "Taiwan Strait conflict":
        if not ("taiwan" in check and ("china" in check or "chinese" in check)): return False
        return True

    return False

# ====================== LOAD STATE ======================
def load_last_state():
    if os.path.exists(PKL_FILE):
        try:
            df = pd.read_pickle(PKL_FILE)
            df['published_date'] = pd.to_datetime(df['published_date'], errors='coerce', utc=True).dt.tz_localize(None)
            last_date = df['published_date'].max() or START_DATE
            existing_urls = set(df['url'].dropna().unique())
            return df, last_date, existing_urls
        except:
            pass
    return pd.DataFrame(), START_DATE, set()

# ====================== SCRAPER ======================
def scrape_guardian():
    df_old, last_date, existing_urls = load_last_state()
    print(f"Starting from {last_date.date()} — Saving clean text under 'full_text' column")

    all_new = []
    visited = set()

    headers = {"User-Agent": "GuardianThesisScraper/1.0"}

    for topic_name, query in topics.items():
        print(f"\n{'═' * 20} {topic_name} {'═' * 20}")

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
            print(f"→ Page {page}")

            try:
                r = requests.get(BASE_URL, params=params, headers=headers, timeout=40)

                if r.status_code == 429:
                    print("  Rate limited — sleeping 70s...")
                    time.sleep(70)
                    continue
                if r.status_code == 400:
                    print("  Bad Request — skipping remaining pages")
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
                    if pub_dt <= last_date:
                        continue

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

                print(f"  Page {page} completed")
                time.sleep(random.uniform(2.0, 4.5))
                page += 1

            except Exception as e:
                print(f"  Error on page {page}: {e}")
                break

        print(f"→ {topic_name} finished → {topic_count} new relevant articles")

    # ====================== SAVE ======================
    if all_new:
        new_df = pd.DataFrame(all_new)
        combined = pd.concat([df_old, new_df], ignore_index=True) if not df_old.empty else new_df
        combined = combined.drop_duplicates(subset=['url'])
        combined['published_date'] = pd.to_datetime(combined['published_date'])

        # ====================== NEW LLM-FRIENDLY SORT ======================
        topic_order = ["Russia Ukraine war", "Iran Israel war", "Taiwan Strait conflict"]
        combined['topic'] = pd.Categorical(combined['topic'], categories=topic_order, ordered=True)
        combined = combined.sort_values(by=['published_date', 'topic', 'source'], ascending=[False, True, True])

        combined.to_csv(CSV_FILE, index=False, encoding="utf-8-sig")
        combined.to_pickle(PKL_FILE)

        print(f"\n✅ Added {len(new_df)} relevant articles | Total: {len(combined)}")
        print(f"   'full_text' column now contains clean text (no HTML tags)")
    else:
        print("\n🟡 No new relevant articles this run.")

def main():
    print(f"🚀 Guardian News Scraper Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    scrape_guardian()
    print("✅ Finished.")

if __name__ == "__main__":
    main()