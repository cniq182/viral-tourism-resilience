import pandas as pd

file_path = r"\\wsl.localhost\Ubuntu\home\fred\viral-tourism-resilience\data_setup\00_Raw_Data\tour_occ_nin2m$defaultview_spreadsheet.xlsx"
df = pd.read_excel(file_path, nrows=5)
print("Columns:")
print(df.columns.tolist())
print("\nData:")
print(df.head())
