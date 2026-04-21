import pandas as pd

df = pd.read_csv("cleaned_tourism_data.csv")

it = df[
    (df["geo"] == "IT") &
    (df["c_resid"] == "TOTAL") &
    (df["unit"] == "NR") &
    (df["nace_r2"] == "I551")  # hotels only
][["geo", "date", "value"]].rename(columns={"value": "tourist_nights"})

it["date"] = it["date"].str.strip()
it = it.dropna(subset=["tourist_nights"]).sort_values("date").reset_index(drop=True)

it.to_csv("data/eurostat_italy.csv", index=False)
print(f"Saved {len(it)} rows to data/eurostat_italy.csv")
print(it.head(10))
