import pandas as pd


def build_italy_panel() -> pd.DataFrame:
    eurostat = pd.read_csv("data/eurostat_italy.csv")

    wiki = pd.read_csv("data/wiki_italy.csv")
    wiki_wide = wiki.pivot_table(index=["geo", "date"], columns="article", values="wiki_pageviews").reset_index()
    wiki_wide.columns.name = None
    wiki_wide = wiki_wide.rename(columns={
        "Italy": "wiki_italy",
        "Rome": "wiki_rome",
        "Tourism_in_Italy": "wiki_tourism_italy",
    })

    trends = pd.read_csv("data/trends_italy.csv")
    trends_wide = trends.pivot_table(index=["geo", "date"], columns="keyword", values="score").reset_index()
    trends_wide.columns.name = None
    trends_wide = trends_wide.rename(columns={
        "Rome travel": "trend_rome_travel",
        "visit Rome": "trend_visit_rome",
        "Italy tourism": "trend_italy_tourism",
    })

    panel = (
        eurostat
        .merge(wiki_wide, on=["geo", "date"], how="outer")
        .merge(trends_wide, on=["geo", "date"], how="outer")
        .sort_values("date")
        .reset_index(drop=True)
    )
    panel.to_csv("data/panel_italy.csv", index=False)
    print(f"Italy panel shape: {panel.shape}")
    print(panel.isnull().sum())
    return panel


def build_japan_panel() -> pd.DataFrame:
    jnto = pd.read_csv("data/jnto_japan.csv")

    wiki = pd.read_csv("data/wiki_jp.csv")
    wiki_wide = wiki.pivot_table(index=["geo", "date"], columns="article", values="wiki_pageviews").reset_index()
    wiki_wide.columns.name = None
    wiki_wide = wiki_wide.rename(columns={
        "Japan": "wiki_japan",
        "Tourism_in_Japan": "wiki_tourism_japan",
    })

    trends = pd.read_csv("data/trends_jp.csv")
    trends_wide = trends.pivot_table(index=["geo", "date"], columns="keyword", values="score").reset_index()
    trends_wide.columns.name = None
    trends_wide = trends_wide.rename(columns={
        "Japan travel": "trend_japan_travel",
        "visit Japan": "trend_visit_japan",
        "Japan tourism": "trend_japan_tourism",
    })

    panel = (
        jnto
        .merge(wiki_wide, on=["geo", "date"], how="outer")
        .merge(trends_wide, on=["geo", "date"], how="outer")
        .sort_values("date")
        .reset_index(drop=True)
    )
    panel.to_csv("data/panel_japan.csv", index=False)
    print(f"\nJapan panel shape: {panel.shape}")
    print(panel.isnull().sum())
    return panel


if __name__ == "__main__":
    build_italy_panel()
    build_japan_panel()
