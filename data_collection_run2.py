import pandas as pd
import numpy as np
import time
import requests
import urllib3
from datetime import datetime
from sklearn.preprocessing import MinMaxScaler
import random 
import os

# Add this near the top of the file
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
# --- 1. COMPATIBILITY PATCH ---
try:
    from urllib3.util import Retry
    if not hasattr(Retry, 'method_whitelist'):
        Retry.method_whitelist = property(
            lambda self: self.allowed_methods,
            lambda self, value: setattr(self, 'allowed_methods', value)
        )
except Exception:
    pass

from pytrends.request import TrendReq

# --- 2. CONFIGURATION & DATA LOADING ---
EU_27 = [
      "Croatia",  "France",  "Luxembourg", 
    "Romania", "Slovakia", "Slovenia", "Spain", "Sweden"
]

START_DATE = '2020-01-01'
END_DATE = '2026-03-27'

def load_country_attractions(file_path):
    """
    Corrected loader for european_destinations_extracted.csv.
    Uses 'Country' (capital C) and splits the 'Destinations/Attractions' string.
    """
    full_path = os.path.join(BASE_DIR, file_path)
    df = pd.read_csv(full_path)
    attr_map = {}
    
    # Fix Case Sensitivity: The CSV uses 'Country' and 'Destinations/Attractions'
    for country in EU_27:
        # Filter for the specific country row
        country_row = df[df['Country'] == country]
        
        if not country_row.empty:
            # Extract the string of attractions and split by comma
            attr_string = str(country_row.iloc[0]['Destinations/Attractions'])
            if attr_string and attr_string != 'nan':
                # Clean up whitespace for each attraction name
                attrs = [a.strip() for a in attr_string.split(',') if a.strip()]
                attr_map[country] = attrs
            else:
                attr_map[country] = []
        else:
            attr_map[country] = []
            
    return attr_map

# Load mapping from your specific attached file
country_attractions = load_country_attractions('european_destinations_extracted.csv')

# --- 3. DATA ACQUISITION FUNCTIONS ---

def get_aggregated_trends(country, attractions):
    """
    Fetches Google Trends for hotels, flights, and all attractions.
    """
    try:
        pt = TrendReq(hl='en-US', tz=360)
        # Base terms + specific attractions
        base_kws = [f"{country} hotel", f"{country} flights"]
        all_keywords = list(set(base_kws + attractions))
        
        # Batching (max 5 keywords per request)
        batch_results = []
        # Limit to top 10 keywords per country to avoid excessive API calls/timeouts
        keywords_to_query = all_keywords[:10] 
        
        for i in range(0, len(keywords_to_query), 5):
            batch = keywords_to_query[i:i+5]
            pt.build_payload(batch, cat=67, timeframe=f'{START_DATE} {END_DATE}', geo="")
            df = pt.interest_over_time()
            if not df.empty:
                if 'isPartial' in df.columns: df = df.drop(columns=['isPartial'])
                batch_results.append(df[batch].mean(axis=1))
            time.sleep(2)
            
        if batch_results:
            final_df = pd.concat(batch_results, axis=1).mean(axis=1).to_frame(name='google_trends')
            return final_df.resample('MS').mean()
            
    except Exception as e:
        print(f"   ! Trends error for {country}: {e}")
    return pd.DataFrame()

def get_wiki_views(article):
    """Fetch Wikipedia Pageviews."""
    start = START_DATE.replace('-', '')
    end = END_DATE.replace('-', '')
    url = f"https://wikimedia.org/api/rest_v1/metrics/pageviews/per-article/en.wikipedia.org/all-access/user/{article}/monthly/{start}/{end}"
    headers = {"User-Agent": "TourismResilience/1.0 (contact: dtu_student)"}
    try:
        r = requests.get(url, headers=headers)
        if r.status_code == 200:
            df = pd.DataFrame(r.json()['items'])
            df['date'] = pd.to_datetime(df['timestamp'], format='%Y%m%d%H')
            return df[['date', 'views']].rename(columns={'views': 'wiki_views'}).set_index('date')
    except Exception:
        pass
    return pd.DataFrame()

# --- 4. EXECUTION LOOP ---

all_data = []
date_spine = pd.DataFrame({'date': pd.date_range(start=START_DATE, end=END_DATE, freq='MS')})
scaler = MinMaxScaler()

print(f"Starting data collection for {len(EU_27)} countries...")

for country in EU_27:
    attrs = country_attractions.get(country, [])
    print(f"--- Processing {country} ---")
    
    dest_df = date_spine.copy()
    dest_df['country'] = country
    
    # 1. Fetch Signals with internal batch delays
    df_trends = get_aggregated_trends(country, attrs)
    
    # 2. Fetch Wiki (Wikimedia is usually more lenient than Google)
    df_wiki = get_wiki_views(country.replace(" ", "_"))
    
    # 3. Merge and Clean
    if not df_trends.empty:
        dest_df = dest_df.merge(df_trends.reset_index(), on='date', how='left')
    else:
        dest_df['google_trends'] = 0.0

    if not df_wiki.empty:
        dest_df = dest_df.merge(df_wiki.reset_index(), on='date', how='left')
    else:
        dest_df['wiki_views'] = 0.0
    
    dest_df = dest_df.fillna(0)
    
    # Scaling...
    if dest_df['google_trends'].max() > 0:
        dest_df['google_trends'] = scaler.fit_transform(dest_df[['google_trends']])
    
    all_data.append(dest_df)
    
    # CRITICAL: Longer, randomized wait to avoid 429
    wait_time = random.uniform(20, 45) 
    print(f"   Waiting {wait_time:.2f}s to avoid 429 block...")
    time.sleep(wait_time)
# --- 5. FINAL EXPORT ---
final_eu_panel = pd.concat(all_data, ignore_index=True)
final_eu_panel.to_csv("eu_tourism_resilience_final2.csv", index=False)

print("\nSuccess! Dataset created: eu_tourism_resilience_final.csv")
print(final_eu_panel.head())