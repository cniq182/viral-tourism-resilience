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
OUTPUT_FILE = os.path.join(BASE_DIR, "spain_city_trends_monthly.csv")

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

    all_rows = []

    unique_cities = df["City"].dropna().unique().tolist()
    print(f"Starting collection for {len(unique_cities)} unique cities...")

    city_trends_map = {}

    for city in unique_cities:
        city_years = df.loc[df["City"] == city, "Year"]
        start_year = int(city_years.min())
        end_year = int(city_years.max())

        print(f"\nFetching trends for city: {city} | years {start_year}-{end_year}")
        city_trends_map[city] = fetch_city_trends(city, start_year, end_year)

        wait_time = random.uniform(20, 40)
        print(f"Finished fetching {city}. Waiting {wait_time:.2f}s before next city...")
        time.sleep(wait_time)

    print(f"\nBuilding final dataset for {len(df)} rows...")

    for idx, row in df.iterrows():
        region = row["Region"]
        city = row["City"]
        year = int(row["Year"])
        country = row["Country"]

        print(f"Processing row {idx + 1}/{len(df)}: {city} ({year})")

        city_trends = city_trends_map.get(city, empty_trends_df(city))

        for month_name in MONTH_COLUMNS:
            original_value = row[month_name]

            if not city_trends.empty and {"Year", "Month"}.issubset(city_trends.columns):
                month_data = city_trends[
                    (city_trends["Year"] == year) &
                    (city_trends["Month"] == month_name)
                ]

                if not month_data.empty:
                    flight_trends = float(month_data[f"{city} flight"].iloc[0]) if f"{city} flight" in month_data.columns else 0.0
                    airbnb_trends = float(month_data[f"{city} airbnb"].iloc[0]) if f"{city} airbnb" in month_data.columns else 0.0
                    hotel_trends = float(month_data[f"{city} hotel"].iloc[0]) if f"{city} hotel" in month_data.columns else 0.0
                else:
                    flight_trends = 0.0
                    airbnb_trends = 0.0
                    hotel_trends = 0.0
            else:
                flight_trends = 0.0
                airbnb_trends = 0.0
                hotel_trends = 0.0

            all_rows.append({
                "Region": region,
                "City": city,
                "Year": year,
                "Country": country,
                "Month": month_name,
                "OriginalMonthlyValue": original_value,
                "flight_trends": flight_trends,
                "airbnb_trends": airbnb_trends,
                "hotel_trends": hotel_trends
            })

    final_df = pd.DataFrame(all_rows)
    final_df.to_csv(OUTPUT_FILE, index=False)

    print("\nDone.")
    print(f"Output saved to: {OUTPUT_FILE}")
    print(final_df.head())


if __name__ == "__main__":
    main()