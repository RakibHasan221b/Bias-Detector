# Bias Detector Project

This project collects news articles from different international sources every day and organizes them into a structured dataset. The goal is to understand how different media outlets report on the same global events, especially around major geopolitical conflicts.
It focuses on building a clean and reliable data pipeline that can later be used for bias detection, comparison, or research.

## Where the data comes from
The project collects articles from:

- BBC News  
- The Guardian  
- The Daily Star  
- New Age Bangladesh  

These sources were chosen because they cover global politics from different perspectives.

## What it tracks

The scraper focuses on three main geopolitical topics:

- Russia–Ukraine war  
- Iran–Israel conflict  
- Taiwan–China / Taiwan Strait tensions  

Each article is filtered and assigned to one of these topics based on its content.

## What the project does

The system:

- Automatically visits news websites  
- Collects new articles daily  
- Removes duplicate articles  
- Extracts clean article text  
- Classifies articles into topics  
- Stores everything in structured files  

It also keeps track of previously collected articles so it does not scrape duplicates again.
The project is written in Python and uses the following tools:

- requests (for HTTP requests)  
- BeautifulSoup (for HTML parsing)  
- Playwright (for dynamic pages like BBC)  
- pandas (for data storage and processing)  
- trafilatura (for clean text extraction)  
- fake-useragent (for request headers)  
- tqdm (for progress tracking)  
- ftfy (for text cleaning)  

## Folder structure

THESIS_WORK/

- bbc.py  
- guardian.py  
- dailystar.py  
- newage.py  
- Data/  
  - bbc.csv  
  - guardian.csv  
  - dailystar_news.csv  
  - newage_news.csv  
- README.md  

## Automation

The project is designed to run automatically every day at midnight Denmark time using GitHub Actions. This ensures the dataset stays updated without manual execution.

## Output

Each article in the dataset contains:

- Published date  
- Topic classification  
- Source name  
- Region  
- Title  
- Full article text  
- URL  
