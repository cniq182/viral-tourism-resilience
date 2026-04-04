import pandas as pd
from pathlib import Path
import numpy as np

WIKI_EXT_PATH = Path(r"\\wsl.localhost\Ubuntu\home\fred\viral-tourism-resilience\data_setup\01_Region_Extraction_Wiki_API\wiki_api_ext.csv")
SELECTED_REGIONS_PATH = Path(r"\\wsl.localhost\Ubuntu\home\fred\viral-tourism-resilience\data_setup\02_Data_Collection\selected_regions.txt")

with open(SELECTED_REGIONS_PATH, "r", encoding="utf-8") as f:
    selected = [line.strip() for line in f if line.strip()]

print(f"Total selected regions: {len(selected)}")

ext_df = pd.read_csv(WIKI_EXT_PATH, dtype=str)
# Option 3 filter:
ext_df_opt3 = ext_df[~((ext_df["capital_city"].isin(["NULL", "", np.nan])) & 
                       (ext_df["attraction_1"].isin(["NULL", "", np.nan])))]

valid_opt3_regions = set(ext_df_opt3["region"].values)

not_in_opt3 = []
for r in selected:
    if r not in valid_opt3_regions:
        not_in_opt3.append(r)

print(f"Regions missing from Option 3 (null or missing in wiki_api_ext.csv): {len(not_in_opt3)}")
for r in not_in_opt3:
    print(f" - {r}")
