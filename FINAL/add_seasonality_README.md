# add_seasonality.py — Documentation

## Purpose

This script enriches the COVID-augmented tourism dataset (`tourism_dataset_with_covid.csv`) with **seasonality and year-over-year (YoY) features** commonly used in tourism demand econometric models.

## Pipeline Position

```
master_tourism_dataset (1).csv
        │
        ▼
  build_dataset.py          ← merges OxCGRT stringency data
        │
        ▼
tourism_dataset_with_covid.csv
        │
        ▼
  add_seasonality.py        ← THIS SCRIPT
        │
        ▼
tourism_dataset_final.csv
```

## New Columns Created

| Column | Type | Description |
|--------|------|-------------|
| `month` | `int` (1–12) | Calendar month extracted from the `date` column. Useful as a categorical/cyclical feature to capture seasonal patterns. |
| `arrivals_last_year` | `float` | The `arrivals_country` value from **12 months prior**, for the same geographic unit. Enables direct year-over-year comparison. |
| `arrivals_yoy_pct_change` | `float` | Percentage change: `(arrivals_country − arrivals_last_year) / arrivals_last_year × 100`. Captures growth or decline relative to the same month of the previous year. |

## Grouping Logic

The 12-month shift is performed **within each unique geographic unit**, defined by the composite key:

```
(country, region, county, city)
```

This prevents data from one city's arrivals being incorrectly treated as the lagged value for a different city in the same country.

## Expected NaN Values

| Column | When NaN? |
|--------|-----------|
| `arrivals_last_year` | First 12 months of each location's time series (no prior year available) |
| `arrivals_yoy_pct_change` | When `arrivals_last_year` is NaN **or** zero (division by zero → NaN) |

These NaN values are **intentional** and should be handled during modelling (e.g., dropped, imputed, or the model's first usable observation starts from month 13).

## Usage

```bash
# Make sure the virtual environment is active
source .venv/bin/activate

# Run the previous step first (if not already done)
python build_dataset.py

# Then run this script
python add_seasonality.py
```

## Output

- **File**: `tourism_dataset_final.csv`
- **Verification**: The script prints `df.info()` and a 15-row sample showing the new columns for the first city in the dataset.
