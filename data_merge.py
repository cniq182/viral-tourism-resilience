import pandas as pd
import os

# --- Paths ---
BASE_DIR  = os.path.dirname(os.path.abspath(__file__))

df_base   = pd.read_csv(os.path.join(BASE_DIR, "eu_tourism_resilience_final2.csv"))
df_source = pd.read_csv(os.path.join(BASE_DIR, "eu_tourism_resilience_final.csv"))

# --- Countries to replace ---
REPLACE_COUNTRIES = [
    "France", "Croatia"
]

# --- Remove those countries from the base ---
df_base_clean = df_base[~df_base['country'].isin(REPLACE_COUNTRIES)]

# --- Pull those countries from the source file ---
df_replacements = df_source[df_source['country'].isin(REPLACE_COUNTRIES)]

# --- Check we actually found them ---
found   = df_replacements['country'].unique().tolist()
missing = [c for c in REPLACE_COUNTRIES if c not in found]
if missing:
    print(f"[WARN] Not found in final.csv: {missing}")

# --- Combine ---
df_merged = pd.concat([df_base_clean, df_replacements], ignore_index=True)

# --- Sort ---
df_merged['date'] = pd.to_datetime(df_merged['date'])
df_merged = df_merged.sort_values(['country', 'date']).reset_index(drop=True)

# --- Save to the subfolder ---
output_path = os.path.join(BASE_DIR, "eu_tourism_resilience_merged.csv")
df_merged.to_csv(output_path, index=False)

print(f"✓ Base rows:        {len(df_base)}")
print(f"✓ Replacement rows: {len(df_replacements)}")
print(f"✓ Merged rows:      {len(df_merged)}")
print(f"✓ Saved to:         {output_path}")