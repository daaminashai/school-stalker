import csv
import os
import requests
import time
from bs4 import BeautifulSoup
from urllib.parse import urlencode
from concurrent.futures import ThreadPoolExecutor

INPUT_CSV = "uscities.csv"
BASE_URL = "https://nces.ed.gov/globallocator/index.asp"

START_INDEX = 0  # <-- change to resume

# ------------------------
# Helpers
# ------------------------

def normalize_city(city):
    return city.lower().replace(" ", "_")

def build_url(city):
    params = {
        "search": 1,
        "itemname": "",
        "city": city,
        "State": "",
        "zipcode": "",
        "miles": "",
        "School": 1,
        "PrivSchool": 1,
        "College": 1,
        "sortby": "name",
    }
    return f"{BASE_URL}?{urlencode(params)}"

def extract_rows(container):
    rows = []
    if not container:
        return rows

    for result in container.find_all("div", class_="resultRow global"):
        inst_desc = result.find("div", class_="InstDesc")
        if not inst_desc:
            continue

        # Name
        name_tag = inst_desc.find("a")
        name = name_tag.get_text(strip=True) if name_tag else ""

        # Address
        address = ""
        span = inst_desc.find("span")
        if span:
            lines = list(span.stripped_strings)
            if lines:
                address = lines[0]

        # Grades
        grades = ""
        inst_detail = result.find("div", class_="InstDetail")
        form_col = result.find("div", class_="formCol")

        if inst_detail:
            grades = inst_detail.get_text(" ", strip=True)
        elif form_col:
            grades = form_col.get_text(" ", strip=True).replace("Coed", "").strip()

        rows.append([name, address, grades])

    return rows

def write_csv(path, rows):
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["Name", "Address", "Grades"])
        writer.writerows(rows)

# ------------------------
# Load + sort cities
# ------------------------

cities = []

with open(INPUT_CSV, encoding="utf-8") as f:
    reader = csv.DictReader(f)
    for row in reader:
        city = row.get("city") or row.get("city_ascii")
        population = int(float(row.get("population", 0)))
        if city:
            cities.append((city, population))

cities.sort(key=lambda x: x[1], reverse=True)

print(f"Loaded {len(cities)} cities")

# ------------------------
# Main loop
# ------------------------

def process_city(i, city, pop):
    print(f"[{i}] {city} (pop: {pop})")
    
    city_slug = normalize_city(city)
    city_dir = os.path.join("schools", city_slug)
    os.makedirs(city_dir, exist_ok=True)

    url = build_url(city)
    print(url)
    try:
        response = requests.get(url, timeout=15)
        response.raise_for_status()

        # Save HTML
        html_path = os.path.join(city_dir, "page.html")
        with open(html_path, "w", encoding="utf-8") as f:
            f.write(response.text)

        soup = BeautifulSoup(response.text, "html.parser")

        public_container = soup.find(id="hiddenitems_school")
        private_container = soup.find(id="hiddenitems_privschool")

        public_rows = extract_rows(public_container)
        private_rows = extract_rows(private_container)

        write_csv(os.path.join(city_dir, "public_schools.csv"), public_rows)
        write_csv(os.path.join(city_dir, "private_schools.csv"), private_rows)

        print(f"  → {len(public_rows)} public, {len(private_rows)} private")

    except Exception as e:
        print(f"  ❌ Error: {e}")

        with open(os.path.join(city_dir, "error.txt"), "w") as f:
            f.write(str(e))


with ThreadPoolExecutor(max_workers=20) as executor:
    future_to_city = {
        executor.submit(process_city, i, city, pop): city for i, (city, pop) in enumerate(cities[START_INDEX:], start=START_INDEX)
    }