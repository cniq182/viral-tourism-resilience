import requests
import pandas as pd
import time

# Italy / Rome articles — captures both country and the city driving viral tourism
ARTICLES = {
    "Italy": "IT",
    "Rome": "IT",
    "Tourism_in_Italy": "IT",
}

BASE_URL = (
    "https://wikimedia.org/api/rest_v1/metrics/pageviews/per-article"
    "/en.wikipedia/all-access/all-agents/{article}/monthly/{start}/{end}"
)

START = "20150101"
END = "20241201"


def fetch_pageviews(article: str) -> list[dict]:
    url = BASE_URL.format(article=article, start=START, end=END)
    resp = requests.get(url, headers={"User-Agent": "viral-tourism-research/1.0"})
    resp.raise_for_status()
    items = resp.json().get("items", [])
    return [
        {"article": article, "date": item["timestamp"][:4] + "-" + item["timestamp"][4:6], "wiki_pageviews": item["views"]}
        for item in items
    ]


def main():
    rows = []
    for article, geo in ARTICLES.items():
        print(f"Fetching: {article}")
        data = fetch_pageviews(article)
        for row in data:
            row["geo"] = geo
        rows.extend(data)
        time.sleep(1)

    df = pd.DataFrame(rows)[["geo", "date", "article", "wiki_pageviews"]]
    df = df.sort_values(["article", "date"]).reset_index(drop=True)
    df.to_csv("data/wiki_italy.csv", index=False)
    print(f"Saved {len(df)} rows to data/wiki_italy.csv")
    print(df.tail(10))


if __name__ == "__main__":
    main()
