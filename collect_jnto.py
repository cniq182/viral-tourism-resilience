import pandas as pd
import requests
import io

JNTO_URL = "https://www.jnto.go.jp/statistics/data/_files/20260415_1615-5.xlsx"
YEARS = range(2015, 2025)
def download_excel() -> bytes:
    cache = "data/jnto_raw.xlsx"
    import os
    if os.path.exists(cache):
        with open(cache, "rb") as f:
            return f.read()
    r = requests.get(JNTO_URL, headers={"User-Agent": "viral-tourism-research/1.0"}, timeout=30)
    if r.status_code != 200:
        raise RuntimeError(
            f"Failed to download JNTO data (HTTP {r.status_code}). "
            "The URL may have rotated — check https://www.jnto.go.jp/statistics/ for the latest file."
        )
    with open(cache, "wb") as f:
        f.write(r.content)
    return r.content


def parse_year(xl: pd.ExcelFile, year: int) -> list[dict]:
    sheet = str(year)
    if sheet not in xl.sheet_names:
        return []
    df = pd.read_excel(xl, sheet_name=sheet, header=None)
    # Row 3 = month header row; find where "1月" (January) is to detect column offset
    header_row = df.iloc[3].astype(str)
    start_col = header_row.tolist().index("1月")  # col where January count sits
    month_cols = [start_col + i * 2 for i in range(12)]

    # Row 4 (0-indexed) = 総数 (grand total of all inbound visitors)
    total_row = df.iloc[4]
    rows = []
    for i, col_idx in enumerate(month_cols):
        month = i + 1
        val = total_row.iloc[col_idx]
        try:
            val = float(val)
        except (TypeError, ValueError):
            val = None
        rows.append({
            "geo": "JP",
            "date": f"{year}-{month:02d}",
            "inbound_visitors": val,
        })
    return rows


def main():
    print("Downloading JNTO Excel...")
    content = download_excel()
    xl = pd.ExcelFile(io.BytesIO(content))

    all_rows = []
    for year in YEARS:
        rows = parse_year(xl, year)
        all_rows.extend(rows)
        print(f"  {year}: {len(rows)} months")

    df = pd.DataFrame(all_rows)
    df = df[df["inbound_visitors"].notna()].reset_index(drop=True)
    df.to_csv("data/jnto_japan.csv", index=False)
    print(f"\nSaved {len(df)} rows to data/jnto_japan.csv")
    print(df.head(10))


if __name__ == "__main__":
    main()
