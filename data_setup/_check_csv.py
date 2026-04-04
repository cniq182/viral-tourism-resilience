import pandas as pd
df = pd.read_csv("/home/fred/viral-tourism-resilience/data_setup/wiki_api_ext.csv",
                 keep_default_na=False)  # don't convert NULL to NaN
total = len(df)
cap = (df["capital_city"] != "NULL").sum()
attr = (df["attraction_1"] != "NULL").sum()
both_null = ((df["capital_city"] == "NULL") & (df["attraction_1"] == "NULL")).sum()
print(f"Total: {total}")
print(f"With capital: {cap} ({100*cap/total:.1f}%)")
print(f"With attraction: {attr} ({100*attr/total:.1f}%)")
print(f"Both NULL: {both_null} ({100*both_null/total:.1f}%)")
print()
# Print some sample rows WITH data
has_data = df[(df["capital_city"] != "NULL") | (df["attraction_1"] != "NULL")]
print(f"Rows with some data: {len(has_data)}")
print(has_data.head(50).to_string(index=False))
