"""
build_dataset.py
================
Merges the master tourism dataset with the Oxford Covid-19 Government Response
Tracker (OxCGRT) to add destination and origin stringency index columns.

Usage:
    python build_dataset.py

Output:
    tourism_dataset_with_covid.csv
"""

import pandas as pd
import os

# ─────────────────────────────────────────────────────────────────────────────
# CONFIGURATION
# ─────────────────────────────────────────────────────────────────────────────

LOCAL_FILE = "master_tourism_dataset (1).csv"
OXCGRT_FILE = "oxcgrt_covid_data.csv"
OUTPUT_FILE = "tourism_dataset_with_covid.csv"

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

# Load the local master tourism dataset
print(f"  Loading local file: {LOCAL_FILE} ...")
df = pd.read_csv(LOCAL_FILE, parse_dates=["date"])
print(f"  ✓ Master dataset loaded: {df.shape[0]} rows × {df.shape[1]} columns")

# Load the OxCGRT national dataset from local file
print(f"  Loading local OxCGRT data: {OXCGRT_FILE} ...")
if not os.path.exists(OXCGRT_FILE):
    raise FileNotFoundError(f"{OXCGRT_FILE} not found. Please run download_covid_data.py first.")
ox = pd.read_csv(OXCGRT_FILE, low_memory=False)
print(f"  ✓ OxCGRT dataset loaded: {ox.shape[0]} rows × {ox.shape[1]} columns")

# ─────────────────────────────────────────────────────────────────────────────
# STEP 2: Clean and aggregate the Oxford data
# ─────────────────────────────────────────────────────────────────────────────

print("\n" + "=" * 60)
print("STEP 2: Cleaning & aggregating OxCGRT data")
print("=" * 60)

# Convert the YYYYMMDD integer date to datetime, then floor to monthly
ox["Date"] = pd.to_datetime(ox["Date"].astype(str), format="%Y%m%d")
ox["year_month"] = ox["Date"].values.astype("datetime64[M]")  # first of each month

# Group by country and year-month, taking the mean of StringencyIndex_Average
ox_monthly = (
    ox.groupby(["CountryName", "year_month"], as_index=False)["StringencyIndex_Average"]
    .mean()
    .rename(columns={"StringencyIndex_Average": "dest_stringency_index"})
)
print(f"  ✓ Aggregated to {ox_monthly.shape[0]} country-month observations")
print(f"  Date range: {ox_monthly['year_month'].min()} → {ox_monthly['year_month'].max()}")

# ─────────────────────────────────────────────────────────────────────────────
# STEP 3: Merge destination lockdown levels
# ─────────────────────────────────────────────────────────────────────────────

print("\n" + "=" * 60)
print("STEP 3: Merging destination stringency index")
print("=" * 60)

# Ensure the master dataset date column is also floored to month start
df["date"] = pd.to_datetime(df["date"])

# Left merge on country name + month
df = df.merge(
    ox_monthly,
    how="left",
    left_on=["country", "date"],
    right_on=["CountryName", "year_month"],
)

# Drop the helper merge columns from OxCGRT
df.drop(columns=["CountryName", "year_month"], inplace=True)
print(f"  ✓ dest_stringency_index merged  —  non-null values: {df['dest_stringency_index'].notna().sum()}")

# ─────────────────────────────────────────────────────────────────────────────
# STEP 4: Calculate and merge origin lockdown levels
# ─────────────────────────────────────────────────────────────────────────────

print("\n" + "=" * 60)
print("STEP 4: Computing origin Top-3 stringency index")
print("=" * 60)

# Create a lookup dictionary: (country, year_month) → stringency value
# using the *original* aggregated monthly OxCGRT data (rename column back)
ox_lookup = (
    ox.groupby(["CountryName", "year_month"], as_index=False)["StringencyIndex_Average"]
    .mean()
)
stringency_lookup = ox_lookup.set_index(["CountryName", "year_month"])[
    "StringencyIndex_Average"
].to_dict()


def get_origin_stringency(row):
    """Return the StringencyIndex_Average for each of the Top-3 origin
    countries plus their average, for the row's date."""
    origins = top_origins.get(row["country"])
    if origins is None:
        return pd.Series([None, None, None, None])
    vals = []
    for origin in origins:
        vals.append(stringency_lookup.get((origin, row["date"])))
    non_null = [v for v in vals if v is not None]
    avg = sum(non_null) / len(non_null) if non_null else None
    return pd.Series(vals + [avg])


# Apply and assign the 4 new columns at once
df[["origin1_stringency", "origin2_stringency", "origin3_stringency",
    "origin_top3_stringency_avg"]] = df.apply(get_origin_stringency, axis=1)

for col in ["origin1_stringency", "origin2_stringency", "origin3_stringency",
            "origin_top3_stringency_avg"]:
    print(f"  ✓ {col}  —  non-null values: {df[col].notna().sum()}")


# ─────────────────────────────────────────────────────────────────────────────
# STEP 5: Final cleanup and export
# ─────────────────────────────────────────────────────────────────────────────

print("\n" + "=" * 60)
print("STEP 5: Final cleanup & export")
print("=" * 60)

# Fill NaN stringency values with 0 (pre-2020 / post-pandemic rows)
stringency_cols = ["dest_stringency_index", "origin1_stringency",
                   "origin2_stringency", "origin3_stringency",
                   "origin_top3_stringency_avg"]
for col in stringency_cols:
    df[col] = df[col].fillna(0)
print("  ✓ NaN stringency values filled with 0")

# Save to CSV
df.to_csv(OUTPUT_FILE, index=False)
print(f"  ✓ Saved to {OUTPUT_FILE}")

# Verification output
print("\n" + "=" * 60)
print("VERIFICATION")
print("=" * 60)
print()
df.info()
print()
print(df.head())
print()
print("Done ✓")
