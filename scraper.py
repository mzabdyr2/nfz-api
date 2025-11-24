import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import pandas as pd
import os
import re
from tqdm import tqdm
import random
import time


# ============================================================
#   STABILNA SESJA REQUESTS (do chmury / GitHub Actions)
# ============================================================

session = requests.Session()
session.headers.update({"Accept": "application/json"})

retries = Retry(
    total=7,
    backoff_factor=0.4,
    status_forcelist=[400, 429, 500, 502, 503, 504],
    allowed_methods=["GET"]
)

session.mount("https://", HTTPAdapter(max_retries=retries))


# ============================================================
#   FUNKCJE POMOCNICZE
# ============================================================

def clean_for_excel(value):
    if isinstance(value, str):
        ILLEGAL_CHARACTERS_RE = re.compile(r'[\000-\010]|[\013-\014]|[\016-\037]')
        value = ILLEGAL_CHARACTERS_RE.sub('', value)
        return value[:32767]
    return value


def get_json(url, params=None):
    """Bezpieczne pobieranie JSON."""
    time.sleep(random.uniform(0.05, 0.15))  # Cloud throttling
    response = session.get(url, params=params, timeout=20)
    response.raise_for_status()
    return response.json()


def get_all_pages(url, base_params):
    """Generuje wszystkie strony paginacji."""
    page = 1
    while True:
        params = dict(base_params)
        params["page"] = page

        data = get_json(url, params)
        yield data

        next_link = data.get("links", {}).get("next")
        if not next_link:
            break

        page += 1


# ============================================================
#   MAPOWANIA
# ============================================================

voivodeships_map = {
    "01": "Dolnośląski", "02": "Kujawsko-Pomorski", "03": "Lubelski",
    "04": "Lubuski", "05": "Łódzki", "06": "Małopolski", "07": "Mazowiecki",
    "08": "Opolski", "09": "Podkarpacki", "10": "Podlaski", "11": "Pomorski",
    "12": "Śląski", "13": "Świętokrzyski", "14": "Warmińsko-Mazurski",
    "15": "Wielkopolski", "16": "Zachodniopomorski"
}

hospital_type_map = {
    "1": "Gminny, powiatowy, miejski",
    "2": "Niepubliczny",
    "3": "Kliniczny",
    "4": "Wojewódzki",
    "5": "Inny"
}

endpoint_map = {
    "general-data": "basic-data",
    "hospitalization-by-gender": "hospitalizations-by-patient-gender",
    "hospitalization-by-admission": "hospitalizations-by-admission-type",
    "hospitalization-by-admission-nfz": "hospitalizations-by-admission-type-nfz-categorized",
    "hospitalization-by-discharge": "hospitalizations-by-discharge-type",
    "hospitalization-by-age": "hospitalizations-by-patient-age",
    "icd-9-procedures": "icd9-procedures",
    "icd-10-diseases": "icd10-diseases",
    "product-categories": "hospitalizations-by-product-category",
    "hospitalization-by-service": "hospitalizations-by-healthcare-service"
}

param_modes = {
    "default": {},
    "branch": {"branch": "true"},
    "hospitalType": {"hospitalType": "true"}
}

# Tryby obsługujące parametry branżowe:
endpoint_supports_modes = {
    "hospitalizations-by-product-category": ["branch"],
    "hospitalizations-by-admission-type-nfz-categorized": ["branch"],
    "hospitalizations-by-healthcare-service": ["branch", "hospitalType"],
}


# ============================================================
#   FUNKCJE POBIERAJĄCE
# ============================================================

def get_sections():
    url = "https://api.nfz.gov.pl/app-stat-api-jgp/sections"
    first = get_json(url)
    last_page = int(first["links"]["last"][-1])

    sections = []
    for p in range(1, last_page + 1):
        data = get_json(url, params={"page": p})
        sections.extend(data["data"])

    return sections


def get_jgp_codes(sections):
    jgp_codes = []

    for s in tqdm(sections, desc="Sekcje"):
        base_params = {
            "section": s,
            "catalog": "1a",
            "limit": 25,
            "format": "json"
        }

        for page in get_all_pages("https://api.nfz.gov.pl/app-stat-api-jgp/benefits", base_params):
            for row in page.get("data", []):
                if "attributes" in row and "code" in row["attributes"]:
                    jgp_codes.append(row["attributes"]["code"])
                elif "code" in row:
                    jgp_codes.append(row["code"])

    return jgp_codes


def download_table(table_id, endpoint, mode_name, jgp_code, year):
    url = f"https://api.nfz.gov.pl/app-stat-api-jgp/{endpoint}/{table_id}"

    # wybierz tryb TYLKO jeśli endpoint go obsługuje
    if endpoint in endpoint_supports_modes:
        if mode_name not in endpoint_supports_modes[endpoint] and mode_name != "default":
            return []  # pomiń tryb nieobsługiwany

    base_params = {
        "limit": 25,
        "format": "json",
        "api-version": "1.1"
    }

    if mode_name in param_modes:
        base_params.update(param_modes[mode_name])

    rows = []

    for page in get_all_pages(url, base_params):
        attributes = page["data"].get("attributes", {})
        data_rows = attributes.get("data", [])

        for row in data_rows:

            if mode_name == "branch" and "branch" in row:
                row["branch_name"] = voivodeships_map.get(str(row["branch"]).zfill(2), row["branch"])

            if mode_name == "hospitalType" and "hospitalType" in row:
                row["hospitalType_name"] = hospital_type_map.get(str(row["hospitalType"]), row["hospitalType"])

            row["year"] = year
            row["jgp_code"] = jgp_code
            row["name"] = attributes.get("name")
            row["table_id"] = table_id

            rows.append(row)

    return rows


# ============================================================
#   GŁÓWNY PROGRAM
# ============================================================

print("📥 Pobieram sekcje...")
sections = get_sections()

print("📥 Pobieram kody JGP...")
jgp_codes = get_jgp_codes(sections)
print(f"✅ Znaleziono {len(jgp_codes)} kodów JGP")

# struktury wynikowe
table_data = {}
for table_name in endpoint_map.keys():
    for mode_name in ["default", "branch", "hospitalType"]:
        key = f"{table_name}_{mode_name}" if mode_name != "default" else table_name
        table_data[key] = []

print("\n=== START POBIERANIA ===")

index_of_tables_url = "https://api.nfz.gov.pl/app-stat-api-jgp/index-of-tables"

for year in [2022]:
    print(f"\n📅 Rok: {year}")

    for jgp_code in tqdm(jgp_codes, desc="Kody JGP"):

        # pobierz index tabel
        try:
            table_index = get_json(index_of_tables_url, {
                "catalog": "1a",
                "name": jgp_code,
                "year": year,
                "format": "json"
            })
        except:
            continue

        try:
            tables = table_index["data"]["attributes"]["years"][0]["tables"]
        except:
            continue

        for table in tables:
            table_id = table["id"]
            table_name = table["type"]

            if table_name not in endpoint_map:
                continue

            endpoint = endpoint_map[table_name]

            for mode_name in param_modes.keys():
                key = f"{table_name}_{mode_name}" if mode_name != "default" else table_name

                rows = download_table(table_id, endpoint, mode_name, jgp_code, year)
                table_data[key].extend(rows)


# ============================================================
#   ZAPIS DO PLIKÓW
# ============================================================

output_dir = "output_tables_complete_2022"
os.makedirs(output_dir, exist_ok=True)

for key, rows in table_data.items():
    if not rows:
        print(f"⚠️ Brak danych: {key}")
        continue

    df = pd.DataFrame(rows)
    df = df.applymap(clean_for_excel)

    path = f"{output_dir}/{key}.xlsx"
    df.to_excel(path, index=False)

    print(f"💾 Zapisano: {path}")

print("\n✅ ZAKOŃCZONO!")
