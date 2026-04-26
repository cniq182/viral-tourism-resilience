import pandas as pd
import numpy as np
import torch
import torch.nn.functional as F
from torch_geometric.data import Data
from torch_geometric.nn import GCNConv
from sklearn.metrics import mean_squared_error, mean_absolute_error, r2_score

torch.manual_seed(42)
np.random.seed(42)
# 1. LOAD DATA
df = pd.read_csv('master_tourism_dataset.csv')
df['date'] = pd.to_datetime(df['date'])

# 2. DATA CLEANING (The "No-Zero" Rule)
initial_count = len(df)

# Replace 0 with NaN so we can drop them all at once
df['nights_spent_country'] = df['nights_spent_country'].replace(0, np.nan)

# Drop any row where the target or key features are NaN
cols_to_check = ['nights_spent_country', 'gt_hotel', 'gt_airbnb', 'gt_flights']
df = df.dropna(subset=cols_to_check)

# Optional: Remove values that are technically > 0 but effectively noise (e.g., < 10 guests)
df = df[df['nights_spent_country'] > 3]

print(f"Cleaning Complete: Removed {initial_count - len(df)} rows containing zeros or NaNs.")
print(f"Remaining rows: {len(df)}")

def train_and_evaluate_gnn(country_name):
    # Filter and sort
    country_df = df[df['country'] == country_name].sort_values(['date', 'city']).copy()
    
    # Check if we have enough data to even attempt a Graph
    cities = country_df['city'].unique()
    num_cities = len(cities)
    if num_cities < 2 or len(country_df) < 10:
        return None

    # 2. CREATE PREDICTIVE LAGS (Lags 1, 2, 3)
    features_to_lag = ['gt_hotel', 'gt_airbnb', 'gt_flights']
    lagged_cols = []
    for feat in features_to_lag:
        for l in range(1, 4):
            col_name = f'{feat}_lag{l}'
            country_df[col_name] = country_df.groupby('city')[feat].shift(l)
            lagged_cols.append(col_name)
    
    country_df = country_df.dropna(subset=lagged_cols + ['nights_spent_country'])
    
    # Final check after lagging
    if country_df.empty:
        return None

    # 3. NORMALIZATION
    for col in lagged_cols:
        max_val = country_df[col].max()
        country_df[col] = country_df[col] / (max_val if max_val > 0 else 1)
    
    target_max = country_df['nights_spent_country'].max()
    country_df['target_norm'] = country_df['nights_spent_country'] / target_max

    # 4. GRAPH STRUCTURE
    edge_index = torch.tensor([[i, j] for i in range(num_cities) for j in range(num_cities) if i != j], dtype=torch.long).t()

    # 5. MODEL DEFINITION
    class PredictiveResilienceGNN(torch.nn.Module):
        def __init__(self, in_channels):
            super().__init__()
            self.conv1 = GCNConv(in_channels, 16)
            self.conv2 = GCNConv(16, 1)
        def forward(self, x, edge_index):
            x = F.relu(self.conv1(x, edge_index))
            return self.conv2(x, edge_index)

    model = PredictiveResilienceGNN(in_channels=len(lagged_cols))
    optimizer = torch.optim.Adam(model.parameters(), lr=0.01)
    
    # 6. TRAINING
    model.train()
    dates = country_df['date'].unique()
    for epoch in range(250):
        for d in dates:
            month_data = country_df[country_df['date'] == d]
            if len(month_data) < num_cities: continue
            x = torch.tensor(month_data[lagged_cols].values, dtype=torch.float)
            y = torch.tensor(month_data['target_norm'].values, dtype=torch.float).view(-1, 1)
            optimizer.zero_grad()
            out = model(x, edge_index)
            loss = F.mse_loss(out, y)
            loss.backward()
            optimizer.step()

    # 7. INFERENCE & METRICS COLLECTION
    model.eval()
    all_preds = []
    all_actuals = []
    
    with torch.no_grad():
        for d in dates:
            month_data = country_df[country_df['date'] == d]
            x = torch.tensor(month_data[lagged_cols].values, dtype=torch.float)
            y = month_data['target_norm'].values
            out = model(x, edge_index).numpy().flatten()
            all_preds.extend(out)
            all_actuals.extend(y)

    all_preds = np.array(all_preds)
    all_actuals = np.array(all_actuals)

    # 8. CALCULATE METRICS
    mse = mean_squared_error(all_actuals, all_preds)
    rmse = np.sqrt(mse)
    mae = mean_absolute_error(all_actuals, all_preds)
    r2 = r2_score(all_actuals, all_preds)
    mape = np.mean(np.abs((all_actuals - all_preds) / (all_actuals + 1e-5))) * 100

    return {
        "MSE": mse,
        "RMSE": rmse,
        "MAE": mae,
        "R2": r2,
        "MAPE": mape
    }

# --- MODIFIED SECTION: RUN FOR ALL COUNTRIES ---
all_countries = df['country'].unique()
results = {}

print(f"Starting GNN analysis for {len(all_countries)} countries...")

for c in all_countries:
    try:
        print(f"Training GNN for: {c}...")
        res = train_and_evaluate_gnn(c)
        if res is not None:
            results[c] = res
        else:
            print(f"Skipping {c}: Not enough cities or temporal data.")
    except Exception as e:
        print(f"Error processing {c}: {e}")

# PRINTING FINAL TABLE
print("\n" + "="*85)
print(f"{'Country':<20} | {'MSE':<10} | {'RMSE':<10} | {'MAE':<10} | {'R2':<10} | {'MAPE':<10}")
print("-" * 85)

# Sort by R2 Score to show the best/worst resilience
sorted_countries = sorted(results.keys(), key=lambda x: results[x]['R2'], reverse=True)

for c in sorted_countries:
    r = results[c]
    print(f"{c:<20} | {r['MSE']:<10.4f} | {r['RMSE']:<10.4f} | {r['MAE']:<10.4f} | {r['R2']:<10.4f} | {r['MAPE']:<10.2f}")

print("="*85)