"""
add_seasonality.py
==================
Reads tourism_dataset_with_covid.csv (output of build_dataset.py) and enriches
it with seasonality and year-over-year features.

New columns created:
    - month                    : calendar month (1-12)
    - arrivals_last_year       : arrivals_country shifted 12 periods within
                                 each geographic unit
    - arrivals_yoy_pct_change  : percentage change between current and
                                 previous-year arrivals

Usage:
    python add_seasonality.py

Output:
    tourism_dataset_final.csv
"""

import pandas as pd

# ─────────────────────────────────────────────────────────────────────────────
# CONFIGURATION
# ─────────────────────────────────────────────────────────────────────────────

INPUT_FILE = "tourism_dataset_with_covid.csv"
OUTPUT_FILE = "tourism_dataset_final.csv"

# Columns that uniquely identify a geographic time-series.
# Grouping by all four prevents data from bleeding between
# regions/cities within the same country.
GEO_KEYS = ["country", "region", "county", "city"]

# ─────────────────────────────────────────────────────────────────────────────
# STEP 1: Load the data
# ─────────────────────────────────────────────────────────────────────────────

print("=" * 60)
print("STEP 1: Loading data")
print("=" * 60)

df = pd.read_csv(INPUT_FILE, parse_dates=["date"])
print(f"  ✓ Loaded {INPUT_FILE}: {df.shape[0]} rows × {df.shape[1]} columns")

# ─────────────────────────────────────────────────────────────────────────────
# STEP 2: Ensure date is datetime and extract month
# ─────────────────────────────────────────────────────────────────────────────

print("\n" + "=" * 60)
print("STEP 2: Adding month column")
print("=" * 60)

df["date"] = pd.to_datetime(df["date"])
df["month"] = df["date"].dt.month
print(f"  ✓ 'month' column created  —  unique values: {sorted(df['month'].unique())}")

# ─────────────────────────────────────────────────────────────────────────────
# STEP 3: Create lagged arrivals (arrivals_last_year)
# ─────────────────────────────────────────────────────────────────────────────

print("\n" + "=" * 60)
print("STEP 3: Creating arrivals_last_year (12-month lag)")
print("=" * 60)

# Sort to guarantee chronological order within each group
df.sort_values(by=GEO_KEYS + ["date"], inplace=True)

# Shift arrivals_country by 12 periods within each geographic unit.
# Each unit has monthly data, so a shift of 12 = same month of the prior year.
df["arrivals_last_year"] = (
    df.groupby(GEO_KEYS)["arrivals_country"].shift(12)
)

non_null = df["arrivals_last_year"].notna().sum()
total = len(df)
print(f"  ✓ 'arrivals_last_year' created  —  {non_null}/{total} non-null values")
print(f"    (first 12 months per location will be NaN by design)")

# ─────────────────────────────────────────────────────────────────────────────
# STEP 4: Year-over-year percentage change
# ─────────────────────────────────────────────────────────────────────────────

print("\n" + "=" * 60)
print("STEP 4: Computing arrivals_yoy_pct_change")
print("=" * 60)

# pct change = (current - last_year) / last_year * 100
# Division by zero (last_year == 0) is handled → produces NaN
df["arrivals_yoy_pct_change"] = (
    (df["arrivals_country"] - df["arrivals_last_year"])
    / df["arrivals_last_year"]
    * 100
)

non_null_pct = df["arrivals_yoy_pct_change"].notna().sum()
print(f"  ✓ 'arrivals_yoy_pct_change' created  —  {non_null_pct}/{total} non-null values")
print(f"    (NaN where arrivals_last_year is NaN or zero)")

# ─────────────────────────────────────────────────────────────────────────────
# STEP 5: Save and verify
# ─────────────────────────────────────────────────────────────────────────────

print("\n" + "=" * 60)
print("STEP 5: Save & verify")
print("=" * 60)

df.to_csv(OUTPUT_FILE, index=False)
print(f"  ✓ Saved to {OUTPUT_FILE}")

# Verification
print("\n" + "=" * 60)
print("VERIFICATION")
print("=" * 60)
print()
df.info()
print()

# Show a slice with the new columns for a single location
sample_cols = ["date", "country", "city", "month", "arrivals_country",
               "arrivals_last_year", "arrivals_yoy_pct_change"]
sample = df[df["city"] == df["city"].iloc[0]][sample_cols].head(15)
print(sample.to_string(index=False))
print()
print("Done ✓")
