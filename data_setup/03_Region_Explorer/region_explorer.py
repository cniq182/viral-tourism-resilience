# %% [markdown]
# # Region Explorer Notebook
# 
# Use this notebook to interactively explore and visualize the API-collected data
# for specific regions without running the full collection pipeline.

# %%
import os
import pandas as pd
import matplotlib.pyplot as plt
from pathlib import Path
import seaborn as sns

# Make sure inline plotting is set
%matplotlib inline

# %%
# ━━━ CONFIGURATION ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Specify region(s) to explore. These must match the GEO (Labels) exactly.
REGIONS_TO_EXPLORE = ["Paris", "Berlin"]  # Edit as needed
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

# %%
# ── Paths ────────────────────────────────────────────────────────────────────
BASE_DIR = Path.cwd().parent
MERGED_ALL_CSV = BASE_DIR / "02_Data_Collection" / "merged_panel_all.csv"

# Load the merged panel data
if MERGED_ALL_CSV.exists():
    df = pd.read_csv(MERGED_ALL_CSV)
    print(f"Loaded {len(df)} records from {MERGED_ALL_CSV.name}")
else:
    print(f"File not found: {MERGED_ALL_CSV}. Please run the data_collection pipeline first.")
    df = pd.DataFrame()

# %%
# Filter down to the regions we care about
if not df.empty:
    filtered_df = df[df["region"].isin(REGIONS_TO_EXPLORE)]
    
    # Sort for plotting
    if "year_month" in filtered_df.columns:
        filtered_df = filtered_df.sort_values(by=["region", "year_month"])
    
    print(f"Found {len(filtered_df)} records for the requested regions.")
else:
    filtered_df = pd.DataFrame()

# %% [markdown]
# ## 1. Eurostat Tourism Data Overview
# Time series of nights spent for selected regions.

# %%
if not filtered_df.empty and "nights_spent" in filtered_df.columns:
    plt.figure(figsize=(10, 5))
    sns.lineplot(data=filtered_df, x="year_month", y="nights_spent", hue="region", marker="o")
    plt.title("Eurostat: Nights Spent in Tourist Accommodation", fontsize=14)
    plt.ylabel("Nights Spent")
    plt.xlabel("Month")
    plt.xticks(rotation=45)
    plt.grid(True, alpha=0.3)
    plt.tight_layout()
    plt.show()

# %% [markdown]
# ## 2. Wikipedia Pageviews
# Shows how often the Wikipedia pages for these regions were viewed.

# %%
# If merged_panel_all was created, it may have wiki_views_opt1 and wiki_views_opt2
if not filtered_df.empty:
    wiki_cols = [c for c in filtered_df.columns if "wiki" in c]
    if wiki_cols:
        col_to_plot = wiki_cols[0] # Just pick the first wiki col for preview
        plt.figure(figsize=(10, 5))
        sns.lineplot(data=filtered_df, x="year_month", y=col_to_plot, hue="region", marker="s")
        plt.title(f"Wikipedia Pageviews ({col_to_plot})", fontsize=14)
        plt.ylabel("Views")
        plt.xlabel("Month")
        plt.xticks(rotation=45)
        plt.grid(True, alpha=0.3)
        plt.tight_layout()
        plt.show()

# %% [markdown]
# ## 3. Google Trends
# Search interest over time for selected regions.

# %%
if not filtered_df.empty:
    trend_cols = [c for c in filtered_df.columns if "trends" in c]
    if trend_cols:
        col_to_plot = trend_cols[0]
        plt.figure(figsize=(10, 5))
        sns.lineplot(data=filtered_df, x="year_month", y=col_to_plot, hue="region", marker="d")
        plt.title(f"Google Trends Interest Index ({col_to_plot})", fontsize=14)
        plt.ylabel("Index (0-100)")
        plt.xlabel("Month")
        plt.xticks(rotation=45)
        plt.grid(True, alpha=0.3)
        plt.tight_layout()
        plt.show()

# %% [markdown]
# ## 4. Reddit Mentions (Arctic Shift)
# Number of Reddit posts related to the region keywords.

# %%
if not filtered_df.empty:
    reddit_cols = [c for c in filtered_df.columns if "reddit" in c]
    if reddit_cols:
        col_to_plot = reddit_cols[0]
        plt.figure(figsize=(10, 5))
        sns.lineplot(data=filtered_df, x="year_month", y=col_to_plot, hue="region", marker="^")
        plt.title(f"Reddit Mentions ({col_to_plot})", fontsize=14)
        plt.ylabel("Post Count")
        plt.xlabel("Month")
        plt.xticks(rotation=45)
        plt.grid(True, alpha=0.3)
        plt.tight_layout()
        plt.show()

# %% [markdown]
# ## 5. Combined Data Summary
# A data completeness report.

# %%
if not filtered_df.empty:
    completeness = filtered_df.groupby("region").apply(lambda x: x.notna().mean() * 100).round(1)
    print("Percentage of non-null values by feature:")
    display(completeness)
