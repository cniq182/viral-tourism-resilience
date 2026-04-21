import pandas as pd
import numpy as np
import time
import requests
import random

# --- COMPATIBILITY PATCH ---
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

# --- SPAIN REGIONS DATA ---
# Structure: (nuts_group, region_name, main_city, [attraction1, attraction2, attraction3])
SPAIN_REGIONS = [
    # Noroeste
    ("Noroeste", "Galicia", "Santiago de Compostela",
     ["Santiago de Compostela Cathedral", "Rías Baixas", "Tower of Hercules"]),

    ("Noroeste", "Principado de Asturias", "Oviedo",
     ["Picos de Europa", "Covadonga Sanctuary", "Oviedo Cathedral"]),

    ("Noroeste", "Cantabria", "Santander",
     ["Altamira Cave", "Picos de Europa Cantabria", "El Sardinero Beach"]),

    # Noreste
    ("Noreste", "País Vasco", "Bilbao",
     ["Guggenheim Bilbao", "San Sebastián Old Town", "Rioja Alavesa wine"]),

    ("Noreste", "Comunidad Foral de Navarra", "Pamplona",
     ["Running of the Bulls Pamplona", "Camino de Santiago Navarra", "Bardenas Reales"]),

    ("Noreste", "La Rioja", "Logroño",
     ["La Rioja wine region", "Logroño Calle Laurel", "Monasteries of La Rioja"]),

    ("Noreste", "Aragón", "Zaragoza",
     ["Basílica del Pilar Zaragoza", "Ordesa National Park", "Alhambra de Teruel"]),

    # Comunidad de Madrid
    ("Centro (ES)", "Comunidad de Madrid", "Madrid",
     ["Prado Museum Madrid", "Royal Palace Madrid", "Retiro Park Madrid"]),

    # Centro
    ("Centro (ES)", "Castilla y León", "Salamanca",
     ["Salamanca University", "Segovia Aqueduct", "Ávila city walls"]),

    ("Centro (ES)", "Castilla-La Mancha", "Toledo",
     ["Toledo Cathedral", "Don Quixote La Mancha", "Cuenca Hanging Houses"]),

    ("Centro (ES)", "Extremadura", "Cáceres",
     ["Cáceres Old Town", "Mérida Roman Theatre", "Guadalupe Monastery"]),

    # Este
    ("Este", "Cataluña", "Barcelona",
     ["Sagrada Familia Barcelona", "Park Güell Barcelona", "Costa Brava"]),

    ("Este", "Comunitat Valenciana", "Valencia",
     ["City of Arts and Sciences Valencia", "Valencia Paella", "Albufera Valencia"]),

    ("Este", "Illes Balears", "Palma de Mallorca",
     ["Mallorca beaches", "Palma Cathedral", "Formentera island"]),

    # Sur
    ("Sur", "Andalucía", "Seville",
     ["Alhambra Granada", "Seville Alcázar", "Córdoba Mezquita"]),

    ("Sur", "Región de Murcia", "Murcia",
     ["Cartagena Roman Theatre", "Mar Menor", "Murcia Cathedral"]),

    # Canarias
    ("Canarias", "Canarias", "Las Palmas de Gran Canaria",
     ["Teide National Park", "Maspalomas Dunes", "Lanzarote Timanfaya"]),
]

scaler = MinMaxScaler()

# --- HELPER: Fetch Google Trends for a single keyword ---
def fetch_trends_keyword(keyword, retries=3):
    for attempt in range(retries):
        try:
            pt = TrendReq(hl='en-US', tz=360)
            pt.build_payload([keyword], cat=67, timeframe=f'{START_DATE} {END_DATE}', geo="")
            df = pt.interest_over_time()
            if not df.empty:
                if 'isPartial' in df.columns:
                    df = df.drop(columns=['isPartial'])
                series = df[keyword].resample('MS').mean()
                return series
            time.sleep(3)
        except Exception as e:
            print(f"   ! Trends error for '{keyword}' (attempt {attempt+1}): {e}")
            time.sleep(random.uniform(10, 20))
    return pd.Series(dtype=float)

# --- HELPER: Fetch Wikipedia pageviews ---
def get_wiki_views(article):
    start = START_DATE.replace('-', '')
    end = END_DATE.replace('-', '')
    url = (
        f"https://wikimedia.org/api/rest_v1/metrics/pageviews/per-article/"
        f"en.wikipedia.org/all-access/user/{article}/monthly/{start}/{end}"
    )
    headers = {"User-Agent": "TourismResilience/1.0 (contact: dtu_student)"}
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

# --- HELPER: Scale a series 0-1 ---
def scale_series(s):
    if not s.empty and s.max() > 0:
        arr = scaler.fit_transform(s.values.reshape(-1, 1)).flatten()
        return pd.Series(arr, index=s.index)
    return s

# --- MAIN LOOP ---
date_spine = pd.date_range(start=START_DATE, end=END_DATE, freq='MS')
all_rows = []

print(f"Starting data collection for {len(SPAIN_REGIONS)} Spanish regions...\n")

for nuts_group, region, main_city, attractions in SPAIN_REGIONS:
    print(f"--- Processing: {region} (city: {main_city}) ---")

    kw_airbnb  = f"{main_city} airbnb"
    kw_hotel   = f"{main_city} hotel"
    kw_flights = f"{main_city} flights"
    kw_attr1   = attractions[0]
    kw_attr2   = attractions[1]
    kw_attr3   = attractions[2]

    print(f"   Fetching: {kw_airbnb}")
    s_airbnb = fetch_trends_keyword(kw_airbnb)
    time.sleep(random.uniform(8, 15))

    print(f"   Fetching: {kw_hotel}")
    s_hotel = fetch_trends_keyword(kw_hotel)
    time.sleep(random.uniform(8, 15))

    print(f"   Fetching: {kw_flights}")
    s_flights = fetch_trends_keyword(kw_flights)
    time.sleep(random.uniform(8, 15))

    print(f"   Fetching attraction 1: {kw_attr1}")
    s_attr1 = fetch_trends_keyword(kw_attr1)
    time.sleep(random.uniform(8, 15))

    print(f"   Fetching attraction 2: {kw_attr2}")
    s_attr2 = fetch_trends_keyword(kw_attr2)
    time.sleep(random.uniform(8, 15))

    print(f"   Fetching attraction 3: {kw_attr3}")
    s_attr3 = fetch_trends_keyword(kw_attr3)
    time.sleep(random.uniform(8, 15))

    print(f"   Fetching Wikipedia: {main_city.replace(' ', '_')}")
    s_wiki = get_wiki_views(main_city.replace(" ", "_"))
    time.sleep(random.uniform(3, 6))

    # Scale each signal
    s_airbnb  = scale_series(s_airbnb)
    s_hotel   = scale_series(s_hotel)
    s_flights = scale_series(s_flights)
    s_attr1   = scale_series(s_attr1)
    s_attr2   = scale_series(s_attr2)
    s_attr3   = scale_series(s_attr3)
    s_wiki    = scale_series(s_wiki)

    # Build one row per date
    for d in date_spine:
        row = {
            'date':           d,
            'country':        'Spain',
            'nuts_group':     nuts_group,
            'region':         region,
            'city':           main_city,
            'attraction1':    attractions[0],
            'attraction2':    attractions[1],
            'attraction3':    attractions[2],
            'gt_airbnb':      s_airbnb.get(d, np.nan)  if not s_airbnb.empty  else np.nan,
            'gt_hotel':       s_hotel.get(d, np.nan)   if not s_hotel.empty   else np.nan,
            'gt_flights':     s_flights.get(d, np.nan) if not s_flights.empty else np.nan,
            'gt_attraction1': s_attr1.get(d, np.nan)   if not s_attr1.empty   else np.nan,
            'gt_attraction2': s_attr2.get(d, np.nan)   if not s_attr2.empty   else np.nan,
            'gt_attraction3': s_attr3.get(d, np.nan)   if not s_attr3.empty   else np.nan,
            'wiki_views':     s_wiki.get(d, np.nan)    if not s_wiki.empty    else np.nan,
        }
        all_rows.append(row)

    wait = random.uniform(30, 50)
    print(f"   Done. Waiting {wait:.1f}s before next region...\n")
    time.sleep(wait)

# --- EXPORT ---
final_df = pd.DataFrame(all_rows)
final_df = final_df.fillna(0)

out_path = "spain_regions_tourism_trends.csv"
final_df.to_csv(out_path, index=False)

print(f"\n✅ Done! Saved to: {out_path}")
print(f"   Shape: {final_df.shape}")
print(final_df.head(10).to_string())