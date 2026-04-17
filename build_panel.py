import pandas as pd

# --- Load Eurostat ---
eurostat = pd.read_csv("data/eurostat_italy.csv")

# --- Load Wikipedia (pivot articles into columns) ---
wiki = pd.read_csv("data/wiki_italy.csv")
wiki_wide = wiki.pivot_table(index=["geo", "date"], columns="article", values="wiki_pageviews").reset_index()
wiki_wide.columns.name = None
wiki_wide = wiki_wide.rename(columns={
    "Italy": "wiki_italy",
    "Rome": "wiki_rome",
    "Tourism_in_Italy": "wiki_tourism_italy",
})

# --- Load Google Trends (pivot keywords into columns) ---
trends = pd.read_csv("data/trends_italy.csv")
trends_wide = trends.pivot_table(index=["geo", "date"], columns="keyword", values="score").reset_index()
trends_wide.columns.name = None
trends_wide = trends_wide.rename(columns={
    "Rome travel": "trend_rome_travel",
    "visit Rome": "trend_visit_rome",
    "Italy tourism": "trend_italy_tourism",
})

# --- Merge ---
panel = (
    eurostat
    .merge(wiki_wide, on=["geo", "date"], how="outer")
    .merge(trends_wide, on=["geo", "date"], how="outer")
    .sort_values("date")
    .reset_index(drop=True)
)

panel.to_csv("data/panel_italy.csv", index=False)
print(f"Panel shape: {panel.shape}")
print(panel.head(10))
print("\nNull counts:\n", panel.isnull().sum())
