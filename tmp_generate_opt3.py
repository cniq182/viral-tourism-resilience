import pandas as pd
from pathlib import Path
import numpy as np

# Paths
XLSX_PATH = Path(r"\\wsl.localhost\Ubuntu\home\fred\viral-tourism-resilience\data_setup\00_Raw_Data\tour_occ_nin2$defaultview_spreadsheet.xlsx")
WIKI_EXT_PATH = Path(r"\\wsl.localhost\Ubuntu\home\fred\viral-tourism-resilience\data_setup\01_Region_Extraction_Wiki_API\wiki_api_ext.csv")
OUT_PATH = Path(r"\\wsl.localhost\Ubuntu\home\fred\viral-tourism-resilience\data_setup\02_Data_Collection\selected_regions.txt")

# 1. Get Option 3 Valid Regions
ext_df = pd.read_csv(WIKI_EXT_PATH, dtype=str).fillna("")
ext_df_opt3 = ext_df[~((ext_df["capital_city"].isin(["NULL", "", np.nan])) & 
                       (ext_df["attraction_1"].isin(["NULL", "", np.nan])))]
valid_opt3_regions = set(ext_df_opt3["region"].values)

# 2. Extract Data from Eurostat
raw = pd.read_excel(XLSX_PATH, sheet_name="Sheet 1", header=None, dtype=str).fillna("")
data = []
for row in raw.iloc[11:].values:
    geo = str(row[0]).strip()
    if not geo or geo.startswith("European") or geo.startswith("Euro area") or geo == ":":
        continue
    
    val_2023 = str(row[7]).strip().replace(",", "").replace(" ", "")
    val_2024 = str(row[9]).strip().replace(",", "").replace(" ", "")
    
    try:
        v23 = float(val_2023) if val_2023 and val_2023 != ":" else None
        v24 = float(val_2024) if val_2024 and val_2024 != ":" else None
    except ValueError:
        v23 = None
        v24 = None
        
    data.append({"region": geo, "2023": v23, "2024": v24})

df = pd.DataFrame(data)
df = df.dropna(subset=["2023", "2024"])

# Only keep regions that are valid under Option 3
df = df[df["region"].isin(valid_opt3_regions)]

# Filter out countries & NUTS-1 (same as before)
countries = ["Belgium", "Bulgaria", "Czechia", "Denmark", "Germany", "Estonia", "Ireland", "Greece", "Spain", "France", "Croatia", "Italy", "Cyprus", "Latvia", "Lithuania", "Luxembourg", "Hungary", "Malta", "Netherlands", "Austria", "Poland", "Portugal", "Romania", "Slovenia", "Slovakia", "Finland", "Sweden", "Iceland", "Liechtenstein", "Norway", "Switzerland", "United Kingdom", "Türkiye", "Montenegro", "North Macedonia", "Albania", "Serbia"]
large_regions = ["Nord-Est", "Este", "Centro (IT)", "Centro (ES)", "Sur", "Noroeste", "Sud", "Nord-Ovest", "Isole", "Mainland", "Baden-Württemberg", "Bayern", "Ile de France", "Canarias", "Andalucía", "Cataluña", "Comunidad de Madrid", "Comunitat Valenciana"]
df = df[~df["region"].isin(countries)]
df = df[~df["region"].isin(large_regions)]

# 1. Classic Locations
df["avg"] = (df["2023"] + df["2024"]) / 2
classic = df.sort_values("avg", ascending=False).head(7)

# 2. Spikes (min 500,000 nights so we have enough options after opt3 filter)
df_sig = df[df["2023"] >= 500000].copy()
df_sig["growth"] = (df_sig["2024"] - df_sig["2023"]) / df_sig["2023"]
spikes = df_sig.sort_values("growth", ascending=False).head(7)

# 3. Declines (min 500,000 nights)
declines = df_sig.sort_values("growth", ascending=True).head(7)

with open(OUT_PATH, "w", encoding="utf-8") as f:
    for _, row in classic.iterrows():
        f.write(f"{row['region']}\n")
    for _, row in spikes.iterrows():
        f.write(f"{row['region']}\n")
    for _, row in declines.iterrows():
        f.write(f"{row['region']}\n")

with open(Path(r"\\wsl.localhost\Ubuntu\home\fred\viral-tourism-resilience\data_setup\02_Data_Collection\debug_selected.txt"), "w", encoding="utf-8") as f:
    f.write("CLASSICS:\n")
    for _, row in classic.iterrows(): f.write(f"{row['region']} (avg: {row['avg']})\n")
    f.write("\nSPIKES:\n")
    for _, row in spikes.iterrows(): f.write(f"{row['region']} (growth: {row['growth']})\n")
    f.write("\nDECLINES:\n")
    for _, row in declines.iterrows(): f.write(f"{row['region']} (growth: {row['growth']})\n")

print("Regenerated 21 Option-3-compliant regions.")
