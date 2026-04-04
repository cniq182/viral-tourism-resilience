"""
Build wiki_api_ext.csv  (v2 — improved parsing)
-------------------------------------------------
For every GEO (Labels) region in the Eurostat xlsx, use the Wikipedia API
to find:
  • capital_city   – the capital / main city of the region
  • attraction_1   – one well-known tourist attraction

Regions with limited data get NULL values.

Improvements over v1:
  - Strips suffixes like ", Kreisfreie Stadt", ", Landkreis", "Arr. " etc. for
    a cleaner Wikipedia search
  - Recognises city-like entries and sets capital = region name
  - Better infobox + summary parsing
  - Writes a FULL fresh file (no append) — always idempotent
"""

import os, re, time, csv, sys, json
from pathlib import Path

import pandas as pd
import requests

# ── Paths ────────────────────────────────────────────────────────────────────
BASE    = Path(__file__).resolve().parent.parent
XLSX    = BASE / "00_Raw_Data" / "tour_occ_nin2$defaultview_spreadsheet.xlsx"
OUT_CSV = BASE / "01_Region_Extraction_Wiki_API" / "wiki_api_ext.csv"
CACHE   = BASE / "01_Region_Extraction_Wiki_API" / ".wiki_cache.json"          # simple disk cache

# ── Wikipedia helpers ────────────────────────────────────────────────────────
WIKI_HEADERS = {"User-Agent": "tourism-resilience-bot/1.0 (academic research)"}
WIKI_API     = "https://en.wikipedia.org/w/api.php"
WIKI_REST    = "https://en.wikipedia.org/api/rest_v1/page/summary/"
RATE_DELAY   = 0.25          # seconds between API calls

# ── Simple disk cache for Wikipedia responses ────────────────────────────────
_cache: dict = {}
if CACHE.exists():
    with open(CACHE, "r", encoding="utf-8") as fp:
        _cache = json.load(fp)

def _save_cache():
    with open(CACHE, "w", encoding="utf-8") as fp:
        json.dump(_cache, fp, ensure_ascii=False)


def wiki_search(query: str) -> str | None:
    """Return the title of the best-matching Wikipedia article."""
    key = f"search:{query}"
    if key in _cache:
        return _cache[key]
    params = {
        "action": "query", "list": "search", "srsearch": query,
        "srlimit": 1, "format": "json",
    }
    try:
        r = requests.get(WIKI_API, params=params, headers=WIKI_HEADERS, timeout=10)
        r.raise_for_status()
        hits = r.json().get("query", {}).get("search", [])
        result = hits[0]["title"] if hits else None
    except Exception:
        result = None
    _cache[key] = result
    return result


def wiki_summary(title: str) -> str:
    """Return the plain-text extract from the Wikipedia REST summary endpoint."""
    key = f"summary:{title}"
    if key in _cache:
        return _cache[key]
    url = WIKI_REST + requests.utils.quote(title.replace(" ", "_"))
    try:
        r = requests.get(url, headers=WIKI_HEADERS, timeout=10)
        r.raise_for_status()
        result = r.json().get("extract", "")
    except Exception:
        result = ""
    _cache[key] = result
    return result


def wiki_infobox(title: str) -> str:
    """Return section-0 wikitext (where the infobox lives)."""
    key = f"wikitext:{title}"
    if key in _cache:
        return _cache[key]
    params = {
        "action": "parse", "page": title, "prop": "wikitext",
        "format": "json", "section": 0,
    }
    try:
        r = requests.get(WIKI_API, params=params, headers=WIKI_HEADERS, timeout=12)
        r.raise_for_status()
        result = r.json().get("parse", {}).get("wikitext", {}).get("*", "")
    except Exception:
        result = ""
    _cache[key] = result
    return result


# ── Cleaning / search-term preparation ───────────────────────────────────────
def clean_region_name(raw: str) -> str:
    """Turn an Eurostat GEO label into a cleaner Wikipedia search term."""
    s = raw.strip()
    # Remove parenthetical qualifiers
    s = re.sub(r'\s*\((?:NUTS\s*\d{4}|DE|BE|NL|FR|IT|PT|ES|UK)\)', '', s)
    # Strip common administrative suffixes (German)
    s = re.sub(r',\s*(?:Kreisfreie Stadt|Stadtkreis|Landkreis)$', '', s)
    # Strip "Arr. " prefix (Belgian arrondissements)
    s = re.sub(r'^Arr\.\s+', '', s)
    # Strip "Prov. " prefix
    s = re.sub(r'^Prov\.\s+', '', s)
    # Strip "Région de " / "Communauté " etc.
    s = re.sub(r'^Région de\s+', '', s)
    # Strip "Region " prefix if followed by a proper name
    s = re.sub(r'^Region\s+(?=\w)', '', s)
    # Strip "Bezirk " prefix
    s = re.sub(r'^Bezirk\s+', '', s)
    return s.strip()


def is_city_like(summary: str) -> bool:
    """Heuristic: does the summary describe a city/town/municipality?"""
    if not summary:
        return False
    first_200 = summary[:250].lower()
    return any(word in first_200 for word in
               ["is a city", "is the capital", "is a town", "is a municipality",
                "is a commune", "is an urban", "is the largest city",
                "city in", "town in", "municipality in"])


# ── Capital extraction ───────────────────────────────────────────────────────
def extract_capital_infobox(wikitext: str) -> str | None:
    """Try to pull a 'capital' / 'seat' field from infobox wikitext."""
    for pattern in [
        r'\|\s*(?:capital|seat|seat_of_government|admin_center|county seat)\s*=\s*\[\[([^\]\|]+)',
        r'\|\s*(?:capital|seat|seat_of_government|admin_center|county seat)\s*=\s*([A-ZÀ-Ž][a-zà-ž]+(?:[\s\-][A-ZÀ-Ž][a-zà-ž]+)*)',
    ]:
        m = re.search(pattern, wikitext, re.IGNORECASE)
        if m:
            val = m.group(1).strip()
            # Reject if it's a number, flag, or too short
            if len(val) > 1 and not val.isdigit() and val.lower() not in ("yes", "no", "none"):
                return val
    return None


def extract_capital_text(summary: str) -> str | None:
    """Try to extract capital from summary prose."""
    if not summary:
        return None
    patterns = [
        r'(?:capital|administrative (?:centre|center|seat)|county seat)\s+(?:is|of)\s+(?:the city of\s+)?([A-ZÀ-Ž][a-zà-ž]+(?:[\s\-][A-ZÀ-Ž]?[a-zà-ž]+)*)',
        r'(?:capital|administrative (?:centre|center))\s*,?\s*([A-ZÀ-Ž][a-zà-ž]+(?:[\s\-][A-ZÀ-Ž]?[a-zà-ž]+)*)',
        r'(?:seat|headquartered?)\s+(?:is\s+)?(?:in|at)\s+([A-ZÀ-Ž][a-zà-ž]+(?:[\s\-][A-ZÀ-Ž]?[a-zà-ž]+)*)',
    ]
    for pat in patterns:
        m = re.search(pat, summary, re.IGNORECASE)
        if m:
            val = m.group(1).strip()
            if len(val) > 1:
                return val
    return None


# ── Attraction extraction ────────────────────────────────────────────────────
SKIP_WORDS = frozenset({
    "The", "This", "In", "Its", "It", "As", "An", "One",
    "European", "United", "World", "New", "Great", "Old",
    "National", "Central", "Western", "Eastern", "Northern", "Southern",
    "Republic", "Kingdom", "State", "Holy", "Second", "First",
    "German", "French", "Spanish", "Italian", "Polish", "Czech",
    "Human Development Index", "Nomenclature of Territorial Units",
    "Oil Campaign", "International Organization",
})

SKIP_ATTRACTION_PHRASES = frozenset({
    "human development index", "nomenclature of territorial",
    "oil campaign of world war", "international organization",
    "hungarian romantic", "french revolution", "british army",
    "general staff", "island records",
})


def extract_attraction(text: str, region_name: str) -> str | None:
    """Extract one notable attraction / landmark from an article extract."""
    if not text or len(text) < 50:
        return None

    # Pattern 1: "the <Proper Noun Phrase>"
    candidates = re.findall(r'\bthe ([A-Z][a-zà-ž]+(?: (?:of |de |di |del |van )?[A-Z][a-zà-ž]+){0,4})', text)
    for c in candidates:
        first_word = c.split()[0]
        if (first_word not in SKIP_WORDS
                and c.lower() not in SKIP_ATTRACTION_PHRASES
                and c.lower() != region_name.lower()
                and len(c) > 3):
            return c

    # Pattern 2: Title Case named entities (2+ words)
    named = re.findall(r'\b([A-Z][a-zà-ž]+(?: [A-Z][a-zà-ž]+){1,4})', text)
    for c in named:
        first_word = c.split()[0]
        if (first_word not in SKIP_WORDS
                and c.lower() not in SKIP_ATTRACTION_PHRASES
                and c.lower() != region_name.lower()
                and len(c) > 4
                and not any(w in c for w in ["County", "Region", "Province", "District",
                                              "Municipality", "Development", "Territorial"])):
            return c

    return None


# ── Regions to skip (aggregates, duplicated historical NUTS) ─────────────────
SKIP_RE = re.compile("|".join([
    r"^European Union",
    r"^Euro area",
    r"^Special value",
    r"^Observation flags",
    r"^Confidentiality flags",
    r"^:\s*$",
    r"^e\s*$",
    r"^u\s*$",
    r"^C\s*$",
    r"NUTS 2013\)",
    r"NUTS 2016\)",
    r"NUTS 2021\)",
    r"NUTS 2010\)",
]))


def should_skip(region: str) -> bool:
    return bool(SKIP_RE.search(region))


# ── Country-level shortcuts ──────────────────────────────────────────────────
COUNTRY_CAPITALS = {
    "Belgium": "Brussels", "Bulgaria": "Sofia", "Czechia": "Prague",
    "Denmark": "Copenhagen", "Germany": "Berlin", "Estonia": "Tallinn",
    "Ireland": "Dublin", "Greece": "Athens", "Spain": "Madrid",
    "France": "Paris", "Croatia": "Zagreb", "Italy": "Rome",
    "Cyprus": "Nicosia", "Latvia": "Riga", "Lithuania": "Vilnius",
    "Luxembourg": "Luxembourg City", "Hungary": "Budapest",
    "Malta": "Valletta", "Netherlands": "Amsterdam", "Austria": "Vienna",
    "Poland": "Warsaw", "Portugal": "Lisbon", "Romania": "Bucharest",
    "Slovenia": "Ljubljana", "Slovakia": "Bratislava",
    "Finland": "Helsinki", "Sweden": "Stockholm", "Iceland": "Reykjavik",
    "Liechtenstein": "Vaduz", "Norway": "Oslo", "Switzerland": "Bern",
    "United Kingdom": "London", "Montenegro": "Podgorica",
    "North Macedonia": "Skopje", "Albania": "Tirana", "Serbia": "Belgrade",
    "Türkiye": "Ankara",
}

COUNTRY_ATTRACTIONS = {
    "Belgium": "Grand Place", "Bulgaria": "Alexander Nevsky Cathedral",
    "Czechia": "Prague Castle", "Denmark": "Tivoli Gardens",
    "Germany": "Brandenburg Gate", "Estonia": "Tallinn Old Town",
    "Ireland": "Cliffs of Moher", "Greece": "Acropolis of Athens",
    "Spain": "Sagrada Familia", "France": "Eiffel Tower",
    "Croatia": "Plitvice Lakes", "Italy": "Colosseum",
    "Cyprus": "Tombs of the Kings", "Latvia": "Riga Old Town",
    "Lithuania": "Gediminas Tower", "Luxembourg": "Casemates du Bock",
    "Hungary": "Parliament Building", "Malta": "St John Co-Cathedral",
    "Netherlands": "Rijksmuseum", "Austria": "Schoenbrunn Palace",
    "Poland": "Wawel Castle", "Portugal": "Tower of Belem",
    "Romania": "Bran Castle", "Slovenia": "Lake Bled",
    "Slovakia": "Bratislava Castle", "Finland": "Suomenlinna",
    "Sweden": "Vasa Museum", "Iceland": "Blue Lagoon",
    "Liechtenstein": "Vaduz Castle", "Norway": "Geirangerfjord",
    "Switzerland": "Matterhorn", "United Kingdom": "Tower of London",
    "Montenegro": "Bay of Kotor", "North Macedonia": "Lake Ohrid",
    "Albania": "Butrint", "Serbia": "Belgrade Fortress",
    "Türkiye": "Hagia Sophia",
}


def process_region(region: str) -> dict:
    """Look up one region and return a row dict."""
    row = {"region": region, "capital_city": "NULL", "attraction_1": "NULL"}

    # 1) Country-level shortcut
    if region in COUNTRY_CAPITALS:
        row["capital_city"]  = COUNTRY_CAPITALS[region]
        row["attraction_1"]  = COUNTRY_ATTRACTIONS.get(region, "NULL")
        return row

    # 2) Clean the region name for a better Wikipedia search
    search_term = clean_region_name(region)
    if not search_term or len(search_term) < 2:
        return row

    # 3) Search Wikipedia
    title = wiki_search(search_term)
    if not title:
        return row
    time.sleep(RATE_DELAY)

    # 4) Get summary
    summary = wiki_summary(title)
    time.sleep(RATE_DELAY)

    # 5) Get infobox wikitext
    wikitext = wiki_infobox(title)
    time.sleep(RATE_DELAY)

    # 6) Determine capital city
    capital = extract_capital_infobox(wikitext)
    if not capital:
        capital = extract_capital_text(summary)
    if not capital and is_city_like(summary):
        # The region IS a city
        capital = search_term
    row["capital_city"] = capital if capital else "NULL"

    # 7) Extract one attraction
    attraction = extract_attraction(summary, region)
    row["attraction_1"] = attraction if attraction else "NULL"

    return row


# ── Main ─────────────────────────────────────────────────────────────────────
def main():
    # Load regions from xlsx
    raw = pd.read_excel(XLSX, sheet_name="Sheet 1", header=0, dtype=str).fillna("")
    geo_row_idx = None
    for i, r in raw.iterrows():
        if str(r.iloc[0]).strip() == "GEO (Labels)":
            geo_row_idx = i
            break
    if geo_row_idx is None:
        print("ERROR: Could not find GEO (Labels) in the xlsx.")
        sys.exit(1)

    all_regions = raw.iloc[geo_row_idx + 1:, 0].dropna().tolist()
    all_regions = [r.strip() for r in all_regions if r.strip()]

    # Filter out aggregates / historical NUTS duplicates
    regions = [r for r in all_regions if not should_skip(r)]
    print(f"Total raw regions: {len(all_regions)}")
    print(f"After filtering: {len(regions)}")

    # Process all
    rows = []
    for i, region in enumerate(regions, 1):
        row = process_region(region)
        rows.append(row)

        c = row["capital_city"][:28]
        a = row["attraction_1"][:32]
        print(f"[{i:>4}/{len(regions)}]  {region[:48]:<50}  cap={c:<30}  attr={a}")

        # Save cache periodically
        if i % 50 == 0:
            _save_cache()
            print(f"  ── cache saved ({i} done) ──")

    # Save final cache
    _save_cache()

    # Write CSV
    df = pd.DataFrame(rows)

    # ── Add country column using cascading logic ──
    COUNTRIES = {
        "Belgium", "Bulgaria", "Czechia", "Denmark", "Germany", "Estonia",
        "Ireland", "Greece", "Spain", "France", "Croatia", "Italy",
        "Cyprus", "Latvia", "Lithuania", "Luxembourg", "Hungary",
        "Malta", "Netherlands", "Austria", "Poland", "Portugal", "Romania",
        "Slovenia", "Slovakia", "Finland", "Sweden", "Iceland",
        "Liechtenstein", "Norway", "Switzerland", "United Kingdom", "Montenegro",
        "North Macedonia", "Albania", "Serbia", "Türkiye"
    }
    countries_col = []
    curr_country = "Unknown"
    for r in df["region"]:
        r_str = str(r).strip()
        if r_str in COUNTRIES:
            curr_country = r_str
        countries_col.append(curr_country)
    df.insert(1, "country", countries_col)

    df.to_csv(OUT_CSV, index=False, encoding="utf-8")

    # Stats
    n_total = len(df)
    n_cap   = (df["capital_city"] != "NULL").sum()
    n_attr  = (df["attraction_1"] != "NULL").sum()
    print(f"\n✅  Done!")
    print(f"   Total regions:           {n_total}")
    print(f"   With capital_city:       {n_cap} ({100*n_cap/n_total:.1f}%)")
    print(f"   With attraction_1:       {n_attr} ({100*n_attr/n_total:.1f}%)")
    print(f"   Saved to: {OUT_CSV}")


if __name__ == "__main__":
    main()
