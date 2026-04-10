# %% [markdown]
# # Data Collection Notebook
# 
# Pairs Eurostat data with Wikipedia pageviews, Google Trends, and Reddit mentions.

# %%
# ━━━ MODE SWITCH ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
DATA_MODE = 1   # Set to 1, 2, or 3

# Option 1: Use region name only (Wikipedia article = region name)
# Option 2: Use wiki_api_ext.csv (region + capital + attraction 1)
# Option 3: Use wiki_api_ext.csv, but only load regions where capital_city or attraction is not NULL
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

# %%
import os
import time
import calendar
from pathlib import Path
import json

import requests
import pandas as pd
import numpy as np
from pytrends.request import TrendReq

# ── Paths ────────────────────────────────────────────────────────────────────
BASE_DIR = Path.cwd().parent
XLSX_PATH = BASE_DIR / "00_Raw_Data" / "tour_occ_nin2$defaultview_spreadsheet.xlsx"
WIKI_EXT_PATH = BASE_DIR / "01_Region_Extraction_Wiki_API" / "wiki_api_ext.csv"

# Output files based on MODE
OUTPUT_DIR = BASE_DIR / "02_Data_Collection"
import glob

TIMESTAMP = time.strftime("%Y%m%d_%H%M%S")

def get_newest_file(prefix):
    files = list(OUTPUT_DIR.glob(f"{prefix}_*.csv"))
    files.extend(OUTPUT_DIR.glob(f"{prefix}.csv")) # fallback to old names
    return max(files, key=os.path.getmtime) if files else None

WIKI_CSV = OUTPUT_DIR / f"wiki_option{DATA_MODE}_{TIMESTAMP}.csv"
TRENDS_CSV = OUTPUT_DIR / f"trends_option{DATA_MODE}_{TIMESTAMP}.csv"
REDDIT_CSV = OUTPUT_DIR / f"reddit_option{DATA_MODE}_{TIMESTAMP}.csv"

MERGED_CSV = OUTPUT_DIR / f"merged_panel_option{DATA_MODE}_{TIMESTAMP}.csv"
MERGED_ALL_CSV = OUTPUT_DIR / f"merged_panel_all_{TIMESTAMP}.csv"

# Limit runs using a text file with selected regions
SELECTED_REGIONS_PATH = OUTPUT_DIR / "selected_regions.txt"
# Note: Google Trends blocks aggressively if you query all ~2000 regions rapidly.

# ── Dates ────────────────────────────────────────────────────────────────────
DATE_RANGE = pd.date_range("2020-01", "2025-12", freq="MS")

# %% [markdown]
# ## Load Region Data

# %%
def extract_regions(xlsx_path):
    raw = pd.read_excel(xlsx_path, sheet_name="Sheet 1", header=0, dtype=str).fillna("")
    geo_row_idx = None
    for i, row in raw.iterrows():
        if str(row.iloc[0]).strip() == "GEO (Labels)":
            geo_row_idx = i
            break
    if geo_row_idx is not None:
        regions = raw.iloc[geo_row_idx + 1:, 0].dropna().tolist()
        return [r.strip() for r in regions if r.strip()]
    return []

all_regions = extract_regions(XLSX_PATH)

meta_df = pd.DataFrame({"region": all_regions})
if DATA_MODE in [2, 3]:
    if WIKI_EXT_PATH.exists():
        ext_df = pd.read_csv(WIKI_EXT_PATH)
        if DATA_MODE == 3:
            ext_df = ext_df[~((ext_df["capital_city"].isin(["NULL", "", np.nan])) & 
                              (ext_df["attraction_1"].isin(["NULL", "", np.nan])))]
            meta_df = meta_df.merge(ext_df, on="region", how="inner")
        else:
            meta_df = meta_df.merge(ext_df, on="region", how="left")
    else:
        print(f"Warning: {WIKI_EXT_PATH} not found. Running Option {DATA_MODE} with missing data!")

if SELECTED_REGIONS_PATH.exists():
    with open(SELECTED_REGIONS_PATH, "r", encoding="utf-8") as f:
        selected = [line.strip() for line in f if line.strip()]
    if selected:
        meta_df = meta_df[meta_df["region"].isin(selected)]
        print(f"Filtered to {len(meta_df)} regions based on {SELECTED_REGIONS_PATH.name}.")

print(f"Loaded {len(meta_df)} regions for processing.")

# %% [markdown]
# ## 1. Wikipedia Pageviews

# %%
WIKI_HEADERS = {"User-Agent": "tourism-resilience-bot/1.0 (academic research)"}

def get_wikipedia_pageviews(article: str, year: int, month: int) -> int:
    if pd.isna(article) or str(article).strip() == "NULL" or not article: 
        return None
    last_day = calendar.monthrange(year, month)[1]
    start = f"{year}{month:02d}0100"
    end   = f"{year}{month:02d}{last_day:02d}00"
    article_enc = requests.utils.quote(str(article).replace(" ", "_"))
    url = f"https://wikimedia.org/api/rest_v1/metrics/pageviews/per-article/en.wikipedia/all-access/all-agents/{article_enc}/daily/{start}/{end}"
    try:
        r = requests.get(url, headers=WIKI_HEADERS, timeout=10)
        if r.status_code == 200:
            items = r.json().get("items", [])
            return sum(item["views"] for item in items)
    except Exception:
        pass
    return None

# Load existing wiki data
wiki_done = set()
latest_wiki = get_newest_file(f"wiki_option{DATA_MODE}")
if latest_wiki:
    wiki_df_existing = pd.read_csv(latest_wiki)
    wiki_done = set(wiki_df_existing["region"].unique())
else:
    wiki_df_existing = pd.DataFrame()

new_wiki_rows = []
for idx, row in meta_df.iterrows():
    region = row["region"]
    if region in wiki_done:
        continue
    
    articles_to_check = []
    if DATA_MODE == 1:
        articles_to_check.append(region)
    else:
        # Option 2
        articles_to_check.append(region)
        if "capital_city" in row and pd.notna(row["capital_city"]) and row["capital_city"] != "NULL":
            articles_to_check.append(row["capital_city"])
        if "attraction_1" in row and pd.notna(row["attraction_1"]) and row["attraction_1"] != "NULL":
            articles_to_check.append(row["attraction_1"])
            
    print(f"[{idx+1}/{len(meta_df)}] Wiki fetching for {region}...")
    for period in DATE_RANGE:
        y, m = period.year, period.month
        total_views = 0
        for article in articles_to_check:
            views = get_wikipedia_pageviews(article, y, m)
            if views: total_views += views
        
        new_wiki_rows.append({
            "region": region,
            "year_month": period.strftime("%Y-%m"),
            "wiki_views": total_views
        })
    time.sleep(0.5)

if new_wiki_rows:
    new_wiki_df = pd.DataFrame(new_wiki_rows)
    wiki_final = pd.concat([wiki_df_existing, new_wiki_df], ignore_index=True)
    wiki_final.to_csv(WIKI_CSV, index=False)
    print(f"Saved {len(new_wiki_rows)} new Wikipedia records.")
else:
    print("Wikipedia data up-to-date.")

# %% [markdown]
# ## 2. Google Trends

# %%
def get_google_trends(keywords: list, start_date="2020-01-01", end_date="2025-12-31"):
    try:
        pytrends = TrendReq(hl='en-US', tz=0, timeout=(10,25))
        pytrends.build_payload(keywords, timeframe=f"{start_date} {end_date}")
        df = pytrends.interest_over_time()
        if "isPartial" in df.columns:
            df = df.drop(columns="isPartial")
        return df
    except Exception as e:
        print(f"  Trends error for {keywords}: {e}")
        return None

trends_done = set()
latest_trends = get_newest_file(f"trends_option{DATA_MODE}")
if latest_trends:
    trends_df_existing = pd.read_csv(latest_trends)
    trends_done = set(trends_df_existing["region"].unique())
else:
    trends_df_existing = pd.DataFrame()

new_trends_rows = []
for idx, row in meta_df.iterrows():
    region = row["region"]
    if region in trends_done:
        continue
        
    keywords = []
    if DATA_MODE == 1:
        # Strip long names to fit Google Trends length limit and accuracy
        short_region = region.split(",")[0].split("(")[0].strip()[:30]
        keywords.append(short_region)
    else:
        if "capital_city" in row and pd.notna(row["capital_city"]) and row["capital_city"] != "NULL":
            keywords.append(row["capital_city"])
        else:
            short_region = region.split(",")[0].split("(")[0].strip()[:30]
            keywords.append(short_region)

    print(f"[{idx+1}/{len(meta_df)}] Trends fetching for {region}...")
    df = get_google_trends(keywords)
    if df is not None and not df.empty:
        # Average across the keywords searched
        df["trends_index"] = df[keywords].mean(axis=1)
        monthly = df[["trends_index"]].resample("MS").mean().round(2)
        
        for date, row_tr in monthly.iterrows():
            new_trends_rows.append({
                "region": region,
                "year_month": date.strftime("%Y-%m"),
                "trends_index": row_tr["trends_index"]
            })
    
    time.sleep(2) # Backoff

if new_trends_rows:
    new_trends_df = pd.DataFrame(new_trends_rows)
    trends_final = pd.concat([trends_df_existing, new_trends_df], ignore_index=True)
    trends_final.to_csv(TRENDS_CSV, index=False)
    print(f"Saved {len(new_trends_rows)} new Trends records.")
else:
    print("Trends data up-to-date.")

# %% [markdown]
# ## 3. Reddit Mentions (Arctic Shift)
#
# > **⚠️ IMPORTANT NOTICE: Reddit API Issue**
# > The Arctic Shift API currently has **full-text keyword searches disabled** due to resource constraints. 
# > If you pass a keyword to the `search` endpoint, it will reject it with a `400 Bad Request` 
# > (`Unknown query parameter: 'q'`). 
# >
# > **Because of this, the Reddit column values will be 0/null.**
# > 
# > **Options moving forward:**
# > 1. **Check for API updates:** Watch the photon-reddit/arctic-shift GitHub repository to see if `q` search gets restored.
# > 2. **Skip Reddit entirely:** Just rely on Wikipedia & Google Trends (which are far more robust for this anyway).
# > 3. **Alternative source:** Run an alternative social API scraper if discourse text is strictly needed.

# %%
ARCTIC_BASE = "https://arctic-shift.photon-reddit.com/api/posts/search"

def arctic_monthly_count(keyword: str, year: int, month: int):
    if not keyword or str(keyword).strip() == "NULL":
        return None
    last_day  = calendar.monthrange(year, month)[1]
    after_ts  = int(pd.Timestamp(year, month, 1).timestamp())
    before_ts = int(pd.Timestamp(year, month, last_day, 23, 59, 59).timestamp())
    params = {"q": keyword, "after": after_ts, "before": before_ts, "limit": 0, "metadata": "true"}
    try:
        r = requests.get(ARCTIC_BASE, params=params, headers=WIKI_HEADERS, timeout=15)
        if r.status_code == 200:
            data = r.json()
            total = (data.get("metadata") or {}).get("total", data.get("total"))
            if total is not None: return int(total)
    except Exception:
        pass
    return None

reddit_done = set()
latest_reddit = get_newest_file(f"reddit_option{DATA_MODE}")
if latest_reddit:
    reddit_df_existing = pd.read_csv(latest_reddit)
    reddit_done = set(reddit_df_existing["region"].unique())
else:
    reddit_df_existing = pd.DataFrame()

new_reddit_rows = []
for idx, row in meta_df.iterrows():
    region = row["region"]
    if region in reddit_done:
        continue
        
    keywords_to_check = []
    if DATA_MODE == 1:
        keywords_to_check.append(region.split("(")[0].strip())
    else:
        if "capital_city" in row and pd.notna(row["capital_city"]) and row["capital_city"] != "NULL":
            keywords_to_check.append(row["capital_city"])
        if "attraction_1" in row and pd.notna(row["attraction_1"]) and row["attraction_1"] != "NULL":
            keywords_to_check.append(row["attraction_1"])
        if not keywords_to_check:
            keywords_to_check.append(region.split("(")[0].strip())
            
    print(f"[{idx+1}/{len(meta_df)}] Reddit fetching for {region}...")
    for period in DATE_RANGE:
        y, m = period.year, period.month
        total_posts = 0
        for kw in keywords_to_check:
            count = arctic_monthly_count(kw, y, m)
            if count: total_posts += count
            
        new_reddit_rows.append({
            "region": region,
            "year_month": period.strftime("%Y-%m"),
            "reddit_posts": total_posts
        })
        time.sleep(1)

if new_reddit_rows:
    new_reddit_df = pd.DataFrame(new_reddit_rows)
    reddit_final = pd.concat([reddit_df_existing, new_reddit_df], ignore_index=True)
    reddit_final.to_csv(REDDIT_CSV, index=False)
    print(f"Saved {len(new_reddit_rows)} new Reddit records.")
else:
    print("Reddit data up-to-date.")

# %% [markdown]
# ## 4. Panel Merge
# Merge Eurostat target data with the fetched API data.

# %%
raw = pd.read_excel(XLSX_PATH, sheet_name="Sheet 1", header=0, dtype=str).fillna("")
geo_row_idx = None
for i, row in raw.iterrows():
    if str(row.iloc[0]).strip() == "GEO (Labels)":
        geo_row_idx = i
        break

if geo_row_idx is not None:
    # Identify dimension and time columns
    clean = raw.iloc[geo_row_idx + 1:].copy()
    clean.columns = raw.iloc[geo_row_idx].tolist()
    clean = clean.rename(columns={clean.columns[0]: "region"})
    
    time_cols = [c for c in clean.columns if str(c).strip()[:4].isdigit()]
    dim_cols = ["region"]
    
    df_long = clean.melt(id_vars=dim_cols, value_vars=time_cols, var_name="year_month", value_name="nights_spent")
    df_long["year_month"] = df_long["year_month"].astype(str).str.strip()
    df_long["nights_spent"] = pd.to_numeric(df_long["nights_spent"].replace("[^0-9.]", "", regex=True), errors="coerce")
    df_long["year_month"] = df_long["year_month"].apply(lambda x: f"{x}-01" if len(x) == 4 else x)
    
    # Standardize to YYYY-MM
    df_long["year_month"] = pd.to_datetime(df_long["year_month"], format="%Y-%m", errors="coerce").dt.strftime("%Y-%m")
else:
    df_long = pd.DataFrame(columns=["region", "year_month", "nights_spent"])

# Filter valid regions
df_long = df_long[df_long["region"].isin(all_regions)]

# Load collected data
# Load collected data from the latest generated files
wiki_path = get_newest_file(f"wiki_option{DATA_MODE}")
wiki = pd.read_csv(wiki_path) if wiki_path else pd.DataFrame(columns=["region", "year_month", "wiki_views"])

trends_path = get_newest_file(f"trends_option{DATA_MODE}")
trends = pd.read_csv(trends_path) if trends_path else pd.DataFrame(columns=["region", "year_month", "trends_index"])

reddit_path = get_newest_file(f"reddit_option{DATA_MODE}")
reddit = pd.read_csv(reddit_path) if reddit_path else pd.DataFrame(columns=["region", "year_month", "reddit_posts"])

# Merge
panel = df_long.copy()
for other in [wiki, trends, reddit]:
    if not other.empty:
        panel = panel.merge(other, on=["region", "year_month"], how="left")

# Save specific option
panel.to_csv(MERGED_CSV, index=False)
print(f"Panel saved to {MERGED_CSV}")

# Remove the old duplicated get_newest_file block down here
# function is now at top of the script


opt1_file = get_newest_file("merged_panel_option1")
opt2_file = get_newest_file("merged_panel_option2")
opt3_file = get_newest_file("merged_panel_option3")

# Load them if they exist
dfs = []
if opt1_file:
    df1 = pd.read_csv(opt1_file)
    df1 = df1.rename(columns={"wiki_views": "wiki_views_opt1", "trends_index": "trends_index_opt1", "reddit_posts": "reddit_posts_opt1"})
    dfs.append(df1)
if opt2_file:
    df2 = pd.read_csv(opt2_file)
    df2 = df2.rename(columns={"wiki_views": "wiki_views_opt2", "trends_index": "trends_index_opt2", "reddit_posts": "reddit_posts_opt2"})
    dfs.append(df2)
if opt3_file:
    df3 = pd.read_csv(opt3_file)
    df3 = df3.rename(columns={"wiki_views": "wiki_views_opt3", "trends_index": "trends_index_opt3", "reddit_posts": "reddit_posts_opt3"})
    dfs.append(df3)

if dfs:
    import functools
    base_cols = ["region", "year_month"]
    merged_all = functools.reduce(lambda left, right: left.merge(right[base_cols + [c for c in right.columns if c not in left.columns]], on=base_cols, how='outer'), dfs)
    merged_all.to_csv(MERGED_ALL_CSV, index=False)
    print(f"Combined data from all options saved to {MERGED_ALL_CSV}")
else:
    # Just save whichever exists to the all file for now
    panel.to_csv(MERGED_ALL_CSV, index=False)
    print(f"Fallback: Created {MERGED_ALL_CSV} with current option data only (other options not generated yet).")
