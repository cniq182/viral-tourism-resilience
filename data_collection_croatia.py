import pandas as pd
import numpy as np
import time
import requests
import random
import os

# --- COMPATIBILITY PATCH FOR URLLIB3 ---
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
from sklearn.preprocessing import MinMaxScaler

# --- CONFIGURATION ---
START_DATE = '2020-01-01'
END_DATE = '2026-03-27'
OUT_PATH = "croatia_regional_top_cities.csv"

# --- TOP 2 CITIES PER REGION ---
# Format: (Region, County, City, [Attractions])
CROATIA_COUNTIES = [
    # ADRIATIC CROATIA
    ("Adriatic Croatia", "Splitsko-dalmatinska županija", "Split", 
     ["Diocletian Palace", "Marjan Hill", "Split Riva"]),
    ("Adriatic Croatia", "Dubrovačko-neretvanska županija", "Dubrovnik", 
     ["Dubrovnik Old Town", "Walls of Dubrovnik", "Lokrum Island"]),

    # PANNONIAN CROATIA
    ("Pannonian Croatia", "Osječko-baranjska županija", "Osijek", 
     ["Tvrđa Osijek", "Kopački Rit", "Drava River"]),
    ("Pannonian Croatia", "Karlovačka županija", "Karlovac", 
     ["Aquatika Karlovac", "Karlovac Star", "Mrežnica River"]),

    # NORTHERN CROATIA
    ("Northern Croatia", "Varaždinska županija", "Varaždin", 
     ["Varaždin Castle", "Špancirfest", "Varaždin Cemetery"]),
    ("Northern Croatia", "Međimurska županija", "Čakovec", 
     ["Čakovec Castle", "Međimurje Wine", "Mura-Drava Park"]),

    # CITY OF ZAGREB
    ("City of Zagreb", "Grad Zagreb", "Zagreb", 
     ["Zagreb Cathedral", "Ban Jelačić Square", "Maksimir Park"]),
]

scaler = MinMaxScaler()

def fetch_trends_keyword(keyword, retries=3):
    """Fetches Google Trends data with retry logic."""
    for attempt in range(retries):
        try:
            # We initialize TrendReq inside the loop to refresh the session
            pt = TrendReq(hl='en-US', tz=360)
            pt.build_payload([keyword], cat=67, timeframe=f'{START_DATE} {END_DATE}', geo="")
            df = pt.interest_over_time()
            if not df.empty:
                if 'isPartial' in df.columns:
                    df = df.drop(columns=['isPartial'])
                # Resample to Monthly Start to match Wikipedia and spine
                series = df[keyword].resample('MS').mean()
                return series
            time.sleep(5)
        except Exception as e:
            print(f"   ! Trends error for '{keyword}' (attempt {attempt+1}): {e}")
            time.sleep(random.uniform(20, 40)) # Longer wait on error
    return pd.Series(dtype=float)

def get_wiki_views(article):
    """Fetches Wikipedia pageviews from the Wikimedia API."""
    start = START_DATE.replace('-', '')
    end = END_DATE.replace('-', '')
    url = (
        f"https://wikimedia.org/api/rest_v1/metrics/pageviews/per-article/"
        f"en.wikipedia.org/all-access/user/{article}/monthly/{start}/{end}"
    )
    headers = {"User-Agent": "TourismDataBot/1.0 (contact: researcher_email@example.com)"}
    try:
        r = requests.get(url, headers=headers, timeout=15)
        if r.status_code == 200:
            items = r.json().get('items', [])
            if items:
                df = pd.DataFrame(items)
                df['date'] = pd.to_datetime(df['timestamp'], format='%Y%m%d%H')
                return df[['date', 'views']].set_index('date')['views']
    except Exception as e:
        print(f"   ! Wiki error for '{article}': {e}")
    return pd.Series(dtype=float)

def scale_series(s):
    """Normalizes series values between 0 and 1."""
    if not s.empty and s.max() > 0:
        arr = scaler.fit_transform(s.values.reshape(-1, 1)).flatten()
        return pd.Series(arr, index=s.index)
    return s

# --- MAIN EXECUTION ---
date_spine = pd.date_range(start=START_DATE, end=END_DATE, freq='MS')

print(f"🚀 Starting collection for {len(CROATIA_COUNTIES)} key cities across Croatia regions.")
print(f"📊 Saving incrementally to: {OUT_PATH}\n")

# Start fresh by removing the old file if it exists
if os.path.exists(OUT_PATH):
    os.remove(OUT_PATH)

for region, county, main_city, attractions in CROATIA_COUNTIES:
    print(f"--- 🏙️ Processing: {main_city} ({region}) ---")
    
    # 1. Collect Google Trends
    # Adding 'Croatia' to queries can help filter out global city name duplicates
    s_airbnb  = fetch_trends_keyword(f"{main_city} airbnb")
    time.sleep(random.uniform(5, 10))
    
    s_hotel   = fetch_trends_keyword(f"{main_city} hotel")
    time.sleep(random.uniform(5, 10))
    
    s_flights = fetch_trends_keyword(f"{main_city} flights")
    time.sleep(random.uniform(5, 10))
    
    # Collect Attraction Trends
    s_attr1 = fetch_trends_keyword(attractions[0])
    time.sleep(random.uniform(5, 10))
    s_attr2 = fetch_trends_keyword(attractions[1])
    time.sleep(random.uniform(5, 10))
    s_attr3 = fetch_trends_keyword(attractions[2])
    time.sleep(random.uniform(5, 10))

    # 2. Collect Wikipedia Views
    s_wiki = get_wiki_views(main_city.replace(" ", "_"))
    time.sleep(3)

    # 3. Scale the Data (0 to 1)
    scaled_data = {
        'gt_airbnb': scale_series(s_airbnb),
        'gt_hotel': scale_series(s_hotel),
        'gt_flights': scale_series(s_flights),
        'gt_attr1': scale_series(s_attr1),
        'gt_attr2': scale_series(s_attr2),
        'gt_attr3': scale_series(s_attr3),
        'wiki': scale_series(s_wiki)
    }

    # 4. Create DataFrame for THIS city
    city_rows = []
    for d in date_spine:
        city_rows.append({
            'date': d,
            'region': region,
            'county': county,
            'city': main_city,
            'gt_airbnb': scaled_data['gt_airbnb'].get(d, 0),
            'gt_hotel': scaled_data['gt_hotel'].get(d, 0),
            'gt_flights': scaled_data['gt_flights'].get(d, 0),
            'gt_attraction1': scaled_data['gt_attr1'].get(d, 0),
            'gt_attraction2': scaled_data['gt_attr2'].get(d, 0),
            'gt_attraction3': scaled_data['gt_attr3'].get(d, 0),
            'wiki_views': scaled_data['wiki'].get(d, 0)
        })
    
    # 5. Append to CSV immediately
    temp_df = pd.DataFrame(city_rows)
    file_exists = os.path.isfile(OUT_PATH)
    temp_df.to_csv(OUT_PATH, mode='a', index=False, header=not file_exists)
    
    print(f"✅ Saved {main_city} data. Moving to next...")
    
    # Wait to avoid Google Trends 429 "Too Many Requests" error
    cooldown = random.uniform(50, 80)
    print(f"💤 Cooling down for {int(cooldown)}s...\n")
    time.sleep(cooldown)

print(f"✨ ALL DONE! Your final dataset is at {OUT_PATH}")