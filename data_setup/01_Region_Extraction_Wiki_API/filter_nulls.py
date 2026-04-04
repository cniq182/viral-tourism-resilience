import pandas as pd
df = pd.read_csv('wiki_api_ext.csv', keep_default_na=False)

# Filters for rows containing no 'NULL' string in either capital_city or attraction_1
valid_df = df[(df['capital_city'] != 'NULL') & (df['attraction_1'] != 'NULL')]

# Save the filtered CSV
valid_df.to_csv('wiki_api_ext_no_null.csv', index=False)

# Keep a plain text list of the regions as well just in case
with open('regions_list_no_null.txt', 'w', encoding='utf-8') as f:
    for region in valid_df['region']:
        f.write(region + '\n')

print(f'Filtered from {len(df)} down to {len(valid_df)} regions with no NULLs.')
