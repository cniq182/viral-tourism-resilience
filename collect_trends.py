import argparse
import pandas as pd
import time
from pytrends.request import TrendReq

DESTINATIONS = {
    "IT": {
        "geo": "IT",
        "keywords": ["Rome travel", "visit Rome", "Italy tourism"],
    },
    "JP": {
        "geo": "JP",
        "keywords": ["Japan travel", "visit Japan", "Japan tourism"],
    },
}

TIMEFRAME = "2015-01-01 2024-12-31"


def fetch_trend(pytrends: TrendReq, keyword: str, geo: str) -> pd.DataFrame:
    pytrends.build_payload([keyword], timeframe=TIMEFRAME, geo="")
    df = pytrends.interest_over_time()
    if df.empty:
        print(f"  No data for: {keyword}")
        return pd.DataFrame()
    df = df[[keyword]].reset_index()
    df.columns = ["date", "score"]
    df["keyword"] = keyword
    df["geo"] = geo
    df["date"] = df["date"].dt.strftime("%Y-%m")
    return df[["geo", "date", "keyword", "score"]]


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--geo", required=True, choices=list(DESTINATIONS.keys()),
                        help="Destination geo code (IT or JP)")
    args = parser.parse_args()

    config = DESTINATIONS[args.geo]
    output = f"data/trends_{args.geo.lower()}.csv"

    pytrends = TrendReq(hl="en-US", tz=0)
    rows = []

    for kw in config["keywords"]:
        print(f"Fetching: {kw}")
        try:
            df = fetch_trend(pytrends, kw, config["geo"])
            if not df.empty:
                rows.append(df)
        except Exception as e:
            print(f"  Error: {e}")
        time.sleep(10)

    combined = pd.concat(rows, ignore_index=True)
    combined.to_csv(output, index=False)
    print(f"Saved {len(combined)} rows to {output}")
    print(combined.head(6))


if __name__ == "__main__":
    main()
