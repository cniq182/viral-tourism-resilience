import pandas as pd
from pathlib import Path

csv_path = Path("/home/fred/viral-tourism-resilience/data_setup/wiki_api_ext.csv")

COUNTRIES = {
    "Belgium", "Bulgaria", "Czechia", "Denmark", "Germany", "Estonia",
    "Ireland", "Greece", "Spain", "France", "Croatia", "Italy",
    "Cyprus", "Latvia", "Lithuania", "Luxembourg", "Hungary",
    "Malta", "Netherlands", "Austria", "Poland", "Portugal", "Romania",
    "Slovenia", "Slovakia", "Finland", "Sweden", "Iceland",
    "Liechtenstein", "Norway", "Switzerland", "United Kingdom", "Montenegro",
    "North Macedonia", "Albania", "Serbia", "Türkiye"
}

df = pd.read_csv(csv_path, keep_default_na=False)

# Make sure we don't duplicate if already present
if "country" not in df.columns:
    countries = []
    current_country = "Unknown"
    
    for row in df["region"]:
        # In Eurostat, sometimes country name might have footnotes or whitespace, 
        # but the builder exact-matches from this list.
        if row.strip() in COUNTRIES:
            current_country = row.strip()
        countries.append(current_country)
        
    df.insert(1, "country", countries)
    
    print(f"Assigning 'country' column. Rows starting with 'Unknown': {(df['country'] == 'Unknown').sum()}")
    if (df['country'] == 'Unknown').sum() > 0:
        print("First few Unknowns:")
        print(df[df['country'] == 'Unknown'].head(10))
        
    df.to_csv(csv_path, index=False)
    print("Updated wiki_api_ext.csv successfully.")
else:
    print("country column already exists.")
