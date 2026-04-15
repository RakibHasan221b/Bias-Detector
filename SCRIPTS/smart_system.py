# smart_system.py
import pandas as pd
from sklearn.feature_extraction.text import TfidfVectorizer
from llm import BiasEngine

# ---------------- LOAD DATA ----------------
def load_data():
    paths = {
        "dailystar": r"C:\Users\rakib\Desktop\NEW desktop\THESIS WORK\Data\dailystar_news.csv",
        "newage":   r"C:\Users\rakib\Desktop\NEW desktop\THESIS WORK\Data\newage_news.csv",
        "bbc":      r"C:\Users\rakib\Desktop\NEW desktop\THESIS WORK\Data\bbc.csv",
        "guardian": r"C:\Users\rakib\Desktop\NEW desktop\THESIS WORK\Data\guardian.csv"
    }
    
    dfs = []
    
    for name, path in paths.items():
        df = pd.read_csv(path, low_memory=False)
        
        if name == "dailystar":
            unnamed_cols = [col for col in df.columns if col.startswith('Unnamed')]
            df = df.drop(columns=unnamed_cols)
            print(f"Cleaned Daily Star: dropped {len(unnamed_cols)} unnamed columns")
        
        df["media_type"] = "BD" if name in ["dailystar", "newage"] else "International"
        print(f"Loaded {name}: {len(df)} rows")
        dfs.append(df)
    
    df = pd.concat(dfs, ignore_index=True)
    
    df["published_date"] = pd.to_datetime(df["published_date"], errors='coerce')
    
    # Softer cleaning
    before = len(df)
    df = df[df["full_text"].notna()]
    df = df[df["full_text"].str.strip().str.len() > 30]   # reduced threshold
    df = df[df["published_date"].notna()]
    after = len(df)
    
    print(f"\n=== FINAL DATA SUMMARY ===")
    print(f"Rows before cleaning: {before} | After: {after}")
    print(f"BD rows: {len(df[df['media_type']=='BD'])}")
    print(f"International rows: {len(df[df['media_type']=='International'])}")
    
    return df


# ---------------- SEARCH ENGINE (FIXED) ----------------
class SearchEngine:
    def __init__(self, df):
        self.df = df.copy().reset_index(drop=True)
        self.df["search_text"] = (
            self.df["title"].fillna("") + " " + self.df["full_text"].fillna("")
        )
        
        # Fixed TF-IDF with safeguards
        self.vectorizer = TfidfVectorizer(
            stop_words="english",
            max_features=15000,
            min_df=1,           # important fix
            lowercase=True
        )
        self.tfidf_matrix = self.vectorizer.fit_transform(self.df["search_text"])
        
        print(f"TF-IDF vocabulary size: {len(self.vectorizer.vocabulary_)}")  # debug

    def search(self, keyword=None, topic=None, start_date=None, end_date=None):
        data = self.df.copy()

        # Date filter
        if start_date:
            start_date = pd.to_datetime(start_date)
            data = data[data["published_date"] >= start_date]
        if end_date:
            end_date = pd.to_datetime(end_date)
            data = data[data["published_date"] <= end_date]

        # Topic filter
        if topic and topic != "All":
            data = data[data["topic"].str.contains(topic, case=False, na=False)]

        # Keyword search
        if keyword and str(keyword).strip():
            query_vec = self.vectorizer.transform([keyword])
            scores = (self.tfidf_matrix @ query_vec.T).toarray().flatten()
            
            scored_df = self.df.copy()
            scored_df["score"] = scores

            # Re-apply filters
            if start_date:
                scored_df = scored_df[scored_df["published_date"] >= start_date]
            if end_date:
                scored_df = scored_df[scored_df["published_date"] <= end_date]
            if topic and topic != "All":
                scored_df = scored_df[scored_df["topic"].str.contains(topic, case=False, na=False)]

            # Boost exact matches
            terms = [t.strip() for t in str(keyword).lower().split() if t.strip()]
            if terms:
                pattern = '|'.join(terms)
                mask = scored_df["search_text"].str.lower().str.contains(pattern, na=False, regex=True)
                scored_df.loc[mask, "score"] += 2.0

            scored_df = scored_df.sort_values("score", ascending=False)
            data = scored_df.head(60)
        else:
            data = data.head(80)

        return data

    def split_media(self, df):
        return df[df["media_type"] == "BD"], df[df["media_type"] == "International"]

    def compress(self, df, max_articles=10):
        if len(df) == 0:
            return []
        texts = []
        for _, row in df[["title", "full_text"]].head(max_articles).iterrows():
            text = str(row['full_text'])[:3000]
            texts.append(f"TITLE: {row['title']}\n\nTEXT: {text}")
        return texts


# ---------------- MAIN PIPELINE ----------------
def run_analysis(keyword=None, topic=None, start_date=None, end_date=None):
    df = load_data()
    engine = SearchEngine(df)
    llm = BiasEngine()

    filtered = engine.search(
        keyword=keyword,
        topic=topic,
        start_date=start_date,
        end_date=end_date
    )

    print(f"DEBUG: Total articles found = {len(filtered)} | BD: {len(filtered[filtered['media_type']=='BD'])}")

    if len(filtered) == 0:
        return "No relevant articles found. Try different dates or keywords."

    bd_df, intl_df = engine.split_media(filtered)
    bd_texts = engine.compress(bd_df)
    intl_texts = engine.compress(intl_df)

    clean_topic = topic if topic and topic != "All" else (keyword or "General News")

    result = llm.analyze(
        bd_texts=bd_texts,
        intl_texts=intl_texts,
        topic=clean_topic,
        start_date=start_date or "N/A",
        end_date=end_date or "N/A"
    )

    return result