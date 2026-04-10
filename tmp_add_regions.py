import pandas as pd
import numpy as np

csv_path = "/home/fred/viral-tourism-resilience/data_setup/01_Region_Extraction_Wiki_API/wiki_api_ext.csv"
txt_path = "/home/fred/viral-tourism-resilience/data_setup/02_Data_Collection/selected_regions.txt"

try:
    with open(txt_path, "r", encoding="utf-8") as f:
        existing = [line.strip() for line in f if line.strip()]
except FileNotFoundError:
    existing = []

ext_df = pd.read_csv(csv_path)

valid_df = ext_df[~((ext_df["capital_city"].isin(["NULL", "", np.nan])) & 
                    (ext_df["attraction_1"].isin(["NULL", "", np.nan])))]

valid_regions = valid_df["region"].dropna().unique().tolist()
print(f"Found {len(valid_regions)} valid regions in wiki_api_ext.csv")

to_add = [r for r in valid_regions if r not in existing]
existing.extend(to_add)

print(f"Adding {len(to_add)} new regions. Total now: {len(existing)}")

with open(txt_path, "w", encoding="utf-8") as f:
    for r in existing:
        f.write(r + "\n")
