import pandas as pd
import time
from pytrends.request import TrendReq

# Focus on Rome — most viral Italian destination and explicitly listed in project scope
KEYWORDS = ["Rome travel", "visit Rome", "Italy tourism"]
TIMEFRAME = "2015-01-01 2024-12-31"
GEO = "IT"


def fetch_trend(pytrends: TrendReq, keyword: str) -> pd.DataFrame:
    pytrends.build_payload([keyword], timeframe=TIMEFRAME, geo="")
    df = pytrends.interest_over_time()
    if df.empty:
        print(f"  No data for: {keyword}")
        return pd.DataFrame()
    df = df[[keyword]].reset_index()
    df.columns = ["date", "score"]
    df["keyword"] = keyword
    df["geo"] = GEO
    df["date"] = df["date"].dt.strftime("%Y-%m")
    return df[["geo", "date", "keyword", "score"]]


def main():
    pytrends = TrendReq(hl="en-US", tz=0)
    rows = []

    for kw in KEYWORDS:
        print(f"Fetching: {kw}")
        try:
            df = fetch_trend(pytrends, kw)
            if not df.empty:
                rows.append(df)
        except Exception as e:
            print(f"  Error: {e}")
        time.sleep(10)

    combined = pd.concat(rows, ignore_index=True)
    combined.to_csv("data/trends_italy.csv", index=False)
    print(f"Saved {len(combined)} rows to data/trends_italy.csv")
    print(combined.head(10))


if __name__ == "__main__":
    main()
