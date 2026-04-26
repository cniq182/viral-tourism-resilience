import os
import time
import random
import pandas as pd
from pytrends.request import TrendReq

# =========================================================
# Configuration
# =========================================================
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
INPUT_FILE = os.path.join(BASE_DIR, "spain_data_raw.csv")
OUTPUT_FILE = os.path.join(BASE_DIR, "spain_city_trends_monthly_2.csv")

MONTH_COLUMNS = [
    "January", "February", "March", "April", "May", "June",
    "July", "August", "September", "October", "November", "December"
]

MAX_RETRIES = 8
BASE_WAIT_429 = 60
BASE_WAIT_OTHER = 30

# =========================================================
# Helpers
# =========================================================
def empty_trends_df(city: str) -> pd.DataFrame:
    return pd.DataFrame(columns=[
        "date",
        "Year",
        "Month",
        f"{city} flight",
        f"{city} airbnb",
        f"{city} hotel"
    ])


def fetch_city_trends(city: str, start_year: int, end_year: int) -> pd.DataFrame:
    """
    Fetch Google Trends once for the whole year range for:
      - {city} flight
      - {city} airbnb
      - {city} hotel

    Search is worldwide (geo="").
    Returns monthly aggregated data with Year and Month columns.
    """
    keywords = [f"{city} flight", f"{city} airbnb", f"{city} hotel"]
    timeframe = f"{start_year}-01-01 {end_year}-12-31"

    attempt = 1
    while attempt <= MAX_RETRIES:
        try:
            print(
                f"  Requesting Google Trends for {city} "
                f"({start_year}-{end_year}) | attempt {attempt}/{MAX_RETRIES}"
            )

            pt = TrendReq(hl="en-US", tz=360)
            pt.build_payload(
                kw_list=keywords,
                timeframe=timeframe,
                geo=""
            )

            df = pt.interest_over_time()

            if df.empty:
                print(f"  Empty response for {city}")
                return empty_trends_df(city)

            if "isPartial" in df.columns:
                df = df.drop(columns=["isPartial"])

            df.index = pd.to_datetime(df.index)
            monthly_df = df.resample("MS").mean().reset_index()
            monthly_df["Year"] = monthly_df["date"].dt.year
            monthly_df["Month"] = monthly_df["date"].dt.month_name()

            print(f"  Success for {city}")
            print("  Retrieved data preview:")
            print(monthly_df.to_string(index=False))

            return monthly_df

        except Exception as e:
            error_message = str(e)
            print(f"  Error fetching trends for {city}: {error_message}")

            if attempt < MAX_RETRIES:
                if "429" in error_message:
                    wait_time = BASE_WAIT_429 * attempt + random.uniform(10, 25)
                    print(f"  Google rate limit detected. Waiting {wait_time:.2f}s before retrying...")
                else:
                    wait_time = BASE_WAIT_OTHER * attempt + random.uniform(5, 15)
                    print(f"  Waiting {wait_time:.2f}s before retrying...")

                time.sleep(wait_time)
                attempt += 1
            else:
                print(f"  Max retries reached for {city}. Returning empty data.")
                return empty_trends_df(city)

    return empty_trends_df(city)


def load_existing_output(output_file: str) -> pd.DataFrame:
    if os.path.exists(output_file):
        existing_df = pd.read_csv(output_file)
        print(f"Found existing output file with {len(existing_df)} rows.")
        return existing_df
    return pd.DataFrame(columns=[
        "Region",
        "City",
        "Year",
        "Country",
        "Month",
        "OriginalMonthlyValue",
        "flight_trends",
        "airbnb_trends",
        "hotel_trends"
    ])


def append_rows_to_output(rows: list[dict], output_file: str) -> None:
    if not rows:
        return

    row_df = pd.DataFrame(rows)

    if os.path.exists(output_file):
        row_df.to_csv(output_file, mode="a", header=False, index=False)
    else:
        row_df.to_csv(output_file, mode="w", header=True, index=False)

    print(f"Saved {len(row_df)} rows to {output_file}")


# =========================================================
# Main
# =========================================================
def main():
    df = pd.read_csv(INPUT_FILE, sep=";")
    df.columns = df.columns.str.replace("\ufeff", "", regex=False).str.strip()

    if "Unnamed: 16" in df.columns:
        df = df.drop(columns=["Unnamed: 16"])

    print(df.columns.tolist())

    required_columns = ["Region", "City", "Year", "Country"] + MONTH_COLUMNS
    missing = [col for col in required_columns if col not in df.columns]

    if missing:
        raise ValueError(f"Missing columns in input file: {missing}")

    df["Year"] = df["Year"].astype(int)

    existing_df = load_existing_output(OUTPUT_FILE)
    processed_keys = set()

    if not existing_df.empty:
        existing_df["Year"] = existing_df["Year"].astype(int)
        processed_keys = set(
            zip(existing_df["City"], existing_df["Year"], existing_df["Month"])
        )

    unique_cities = df["City"].dropna().unique().tolist()
    print(f"Starting collection for {len(unique_cities)} unique cities...")

    city_trends_map = {}

    for city in unique_cities:
        city_years = df.loc[df["City"] == city, "Year"]
        start_year = int(city_years.min())
        end_year = int(city_years.max())

        already_done_for_city = True
        for year in sorted(df.loc[df["City"] == city, "Year"].unique()):
            for month_name in MONTH_COLUMNS:
                if (city, int(year), month_name) not in processed_keys:
                    already_done_for_city = False
                    break
            if not already_done_for_city:
                break

        if already_done_for_city:
            print(f"\nSkipping city {city}: all rows already saved.")
            continue

        print(f"\nFetching trends for city: {city} | years {start_year}-{end_year}")
        city_trends_map[city] = fetch_city_trends(city, start_year, end_year)

        city_rows_to_save = []

        city_source_rows = df[df["City"] == city]

        for idx, row in city_source_rows.iterrows():
            region = row["Region"]
            year = int(row["Year"])
            country = row["Country"]

            print(f"Processing row {idx + 1}/{len(df)}: {city} ({year})")

            city_trends = city_trends_map.get(city, empty_trends_df(city))

            for month_name in MONTH_COLUMNS:
                row_key = (city, year, month_name)

                if row_key in processed_keys:
                    print(f"  Skipping {city} | {year} | {month_name}: already saved.")
                    continue

                original_value = row[month_name]

                if not city_trends.empty and {"Year", "Month"}.issubset(city_trends.columns):
                    month_data = city_trends[
                        (city_trends["Year"] == year) &
                        (city_trends["Month"] == month_name)
                    ]

                    if not month_data.empty:
                        flight_trends = month_data[f"{city} flight"].iloc[0] if f"{city} flight" in month_data.columns else None
                        airbnb_trends = month_data[f"{city} airbnb"].iloc[0] if f"{city} airbnb" in month_data.columns else None
                        hotel_trends = month_data[f"{city} hotel"].iloc[0] if f"{city} hotel" in month_data.columns else None
                    else:
                        flight_trends = None
                        airbnb_trends = None
                        hotel_trends = None
                else:
                    flight_trends = None
                    airbnb_trends = None
                    hotel_trends = None

                result_row = {
                    "Region": region,
                    "City": city,
                    "Year": year,
                    "Country": country,
                    "Month": month_name,
                    "OriginalMonthlyValue": original_value,
                    "flight_trends": flight_trends,
                    "airbnb_trends": airbnb_trends,
                    "hotel_trends": hotel_trends
                }

                city_rows_to_save.append(result_row)
                processed_keys.add(row_key)

        append_rows_to_output(city_rows_to_save, OUTPUT_FILE)

        wait_time = random.uniform(20, 40)
        print(f"Finished fetching {city}. Waiting {wait_time:.2f}s before next city...")
        time.sleep(wait_time)

    print("\nDone.")
    print(f"Output saved to: {OUTPUT_FILE}")

    final_df = pd.read_csv(OUTPUT_FILE)
    print(final_df.head())


if __name__ == "__main__":
    main()