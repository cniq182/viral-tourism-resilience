import pandas as pd

# 1. Load both datasets
# Assuming eu_tourism_resilience_merged.csv is your current file
df_merged = pd.read_csv('eu_tourism_resilience_merged.csv')
df_final2 = pd.read_csv('eu_tourism_resilience_final2.csv')

# 2. Convert date columns to datetime objects to ensure proper sorting
df_merged['date'] = pd.to_datetime(df_merged['date'])
df_final2['date'] = pd.to_datetime(df_final2['date'])

# 3. Combine the dataframes
# 'outer' join keeps all rows from both files
# If there are overlapping columns, they will be combined or suffixed
df_combined = pd.concat([df_merged, df_final2]).drop_duplicates().reset_index(drop=True)

# 4. Sort by country and then by date for a logical structure
df_combined = df_combined.sort_values(by=['country', 'date'])

# 5. Overwrite the original merged file with the new combined data
df_combined.to_csv('eu_tourism_resilience_merged.csv', index=False)

print("Merge complete! 'eu_tourism_resilience_merged.csv' has been updated.")