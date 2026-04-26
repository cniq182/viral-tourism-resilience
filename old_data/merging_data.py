import pandas as pd
import glob
import os

# Folder where the CSV files are located
folder_path =  r"c:\Users\cesia\OneDrive\Escriptori\Adv_Buss_Analy\viral-tourism-resilience"

# Get all CSV files in the folder
csv_files = glob.glob(os.path.join(folder_path, "*.csv"))

# Columns to keep
selected_columns = [
    "date",
    "country",
    "city",
    "gt_airbnb",
    "gt_hotel",
    "gt_flights",
    "gt_attraction_1",
    "gt_attraction_2",
    "gt_attraction_3",
    "nights_spent",
    "gt_combined"
]

# List to store each filtered dataframe
dataframes = []

for file in csv_files:
    df = pd.read_csv(file)
    
    # Keep only the columns that exist in the file
    existing_columns = [col for col in selected_columns if col in df.columns]
    df = df[existing_columns]
    
    dataframes.append(df)

# Combine all dataframes into one
combined_df = pd.concat(dataframes, ignore_index=True)

# Save the final CSV
combined_df.to_csv("combined_filtered.csv", index=False)

print("CSV files combined successfully.")
print(f"Final shape: {combined_df.shape}")