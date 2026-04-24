"""
build_dataset_custom_covid.py
=============================
Merges the master tourism dataset with specific OxCGRT indicators
tailored for tourism research (travel bans and beach/gathering limits).

Outputs:
- dest_stay_at_home (from C6M)
- dest_gathering_restrictions (from C4M)
- origin_travel_ban indices (from C8EV)
"""

import pandas as pd
import os

# ─────────────────────────────────────────────────────────────────────────────
# CONFIGURATION
# ─────────────────────────────────────────────────────────────────────────────

LOCAL_FILE = "master_tourism_dataset (1).csv"
OXCGRT_FILE = "oxcgrt_covid_data.csv"
OUTPUT_FILE = "tourism_dataset_with_custom_covid.csv"

# Top 3 inbound origin countries for each destination in the dataset
top_origins = {
    "Albania": ["Kosovo", "North Macedonia", "Italy"],
    "Croatia": ["Germany", "Slovenia", "Austria"],
    "Greece": ["United Kingdom", "Germany", "France"],
    "Italy": ["Germany", "United States", "United Kingdom"],
    "Malta": ["United Kingdom", "Italy", "France"],
    "Spain": ["United Kingdom", "France", "Germany"],
}

# ─────────────────────────────────────────────────────────────────────────────
# STEP 1: Load the data
# ─────────────────────────────────────────────────────────────────────────────

print("=" * 60)
print("STEP 1: Loading data")
print("=" * 60)

print(f"  Loading local file: {LOCAL_FILE} ...")
df = pd.read_csv(LOCAL_FILE, parse_dates=["date"])
print(f"  ✓ Master dataset loaded: {df.shape[0]} rows × {df.shape[1]} columns")

print(f"  Loading local OxCGRT data: {OXCGRT_FILE} ...")
if not os.path.exists(OXCGRT_FILE):
    raise FileNotFoundError(f"{OXCGRT_FILE} not found. Please run download_covid_data.py first.")

cols_to_load = [
    "CountryName", "Date", 
    "C8EV_International travel controls", 
    "C6M_Stay at home requirements", 
    "C4M_Restrictions on gatherings"
]
ox = pd.read_csv(OXCGRT_FILE, usecols=cols_to_load, low_memory=False)
print(f"  ✓ OxCGRT dataset loaded: {ox.shape[0]} rows")

# ─────────────────────────────────────────────────────────────────────────────
# STEP 2: Clean and aggregate the Oxford data
# ─────────────────────────────────────────────────────────────────────────────

print("\n" + "=" * 60)
print("STEP 2: Cleaning & aggregating OxCGRT data")
print("=" * 60)

# Convert the YYYYMMDD integer date to datetime, then floor to monthly
ox["Date"] = pd.to_datetime(ox["Date"].astype(str), format="%Y%m%d")
ox["year_month"] = ox["Date"].values.astype("datetime64[M]")

# Group by country and year-month, taking the mean of our chosen indicators
ox_monthly = ox.groupby(["CountryName", "year_month"], as_index=False).mean()

print(f"  ✓ Aggregated to {ox_monthly.shape[0]} country-month observations")

# ─────────────────────────────────────────────────────────────────────────────
# STEP 3: Merge destination accessibility limits
# ─────────────────────────────────────────────────────────────────────────────

print("\n" + "=" * 60)
print("STEP 3: Merging destination beach/outdoor accessibility indicators")
print("=" * 60)

df["date"] = pd.to_datetime(df["date"])

# We rename the specific columns for the destination merge
dest_ox = ox_monthly[["CountryName", "year_month", 
                      "C6M_Stay at home requirements", 
                      "C4M_Restrictions on gatherings"]].copy()
dest_ox.rename(columns={
    "C6M_Stay at home requirements": "dest_stay_at_home",
    "C4M_Restrictions on gatherings": "dest_gathering_restrictions"
}, inplace=True)

# Left merge on country name + month
df = df.merge(
    dest_ox,
    how="left",
    left_on=["country", "date"],
    right_on=["CountryName", "year_month"],
)

df.drop(columns=["CountryName", "year_month"], inplace=True)
print(f"  ✓ dest_stay_at_home & dest_gathering_restrictions merged")

# ─────────────────────────────────────────────────────────────────────────────
# STEP 4: Calculate and merge origin travel bans
# ─────────────────────────────────────────────────────────────────────────────

print("\n" + "=" * 60)
print("STEP 4: Computing origin international travel bans (Top 3)")
print("=" * 60)

# Lookup dictionary for international travel controls
# (country, year_month) -> C8EV value
travel_lookup = ox_monthly.set_index(["CountryName", "year_month"])["C8EV_International travel controls"].to_dict()

def get_origin_travel_bans(row):
    origins = top_origins.get(row["country"])
    if origins is None:
        return pd.Series([None, None, None, None])
    vals = []
    for origin in origins:
        vals.append(travel_lookup.get((origin, row["date"])))
    non_null = [v for v in vals if v is not None]
    avg = sum(non_null) / len(non_null) if non_null else None
    return pd.Series(vals + [avg])

df[["origin1_travel_ban", "origin2_travel_ban", "origin3_travel_ban",
    "origin_top3_travel_ban_avg"]] = df.apply(get_origin_travel_bans, axis=1)

for col in ["origin1_travel_ban", "origin2_travel_ban", "origin3_travel_ban", "origin_top3_travel_ban_avg"]:
    print(f"  ✓ {col}  —  non-null values: {df[col].notna().sum()}")

# ─────────────────────────────────────────────────────────────────────────────
# STEP 5: Final cleanup and export
# ─────────────────────────────────────────────────────────────────────────────

print("\n" + "=" * 60)
print("STEP 5: Final cleanup & export")
print("=" * 60)

# Fill NaN values with 0 (pre-2020 / post-pandemic rows)
cols_to_fill = ["dest_stay_at_home", "dest_gathering_restrictions",
                "origin1_travel_ban", "origin2_travel_ban", "origin3_travel_ban",
                "origin_top3_travel_ban_avg"]

for col in cols_to_fill:
    df[col] = df[col].fillna(0)
print("  ✓ NaN stringency values filled with 0")

df.to_csv(OUTPUT_FILE, index=False)
print(f"  ✓ Saved to {OUTPUT_FILE}")

# Verification output
print("\n" + "=" * 60)
print("VERIFICATION")
print("=" * 60)
print(df[cols_to_fill].head(10))
print("\nDone ✓")
