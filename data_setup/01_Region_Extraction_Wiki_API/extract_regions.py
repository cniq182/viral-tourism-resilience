"""Extract all GEO (Labels) from the Eurostat xlsx file."""
import pandas as pd

XLSX = "/home/fred/viral-tourism-resilience/data_setup/tour_occ_nin2$defaultview_spreadsheet.xlsx"
OUT  = "/home/fred/viral-tourism-resilience/data_setup/regions_list.txt"

# Read Sheet 1
raw = pd.read_excel(XLSX, sheet_name="Sheet 1", header=0, dtype=str)
raw = raw.fillna("")

# Find the GEO (Labels) row
geo_row_idx = None
for i, row in raw.iterrows():
    if str(row.iloc[0]).strip() == "GEO (Labels)":
        geo_row_idx = i
        break

with open(OUT, "w", encoding="utf-8") as f:
    if geo_row_idx is not None:
        # All rows after GEO (Labels) header contain region names
        regions = raw.iloc[geo_row_idx + 1:, 0].dropna().tolist()
        regions = [r.strip() for r in regions if r.strip()]
        f.write(f"Found {len(regions)} regions\n")
        for r in regions:
            f.write(f"{r}\n")
    else:
        f.write("Could not find GEO (Labels) row\n")
        for i in range(min(20, len(raw))):
            vals = raw.iloc[i].dropna().tolist()
            if vals:
                f.write(f"Row {i}: {vals[:6]}\n")
print("Done")
