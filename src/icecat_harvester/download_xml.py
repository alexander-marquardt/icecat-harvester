import requests
import gzip
import xml.etree.ElementTree as ET
from requests.auth import HTTPBasicAuth
import os
import csv
from dotenv import load_dotenv
import sys

# --- PATH CONFIGURATION ---
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(os.path.dirname(SCRIPT_DIR))
DATA_DIR = os.path.join(PROJECT_ROOT, "data")
XML_SAVE_DIR = os.path.join(DATA_DIR, "xml_source")
TARGETS_FILE = os.path.join(PROJECT_ROOT, "targets.txt")
CATEGORIES_CSV = os.path.join(DATA_DIR, "categories.csv")
FILES_INDEX_XML = os.path.join(DATA_DIR, "files.index.xml.gz")

load_dotenv(os.path.join(PROJECT_ROOT, ".env"))

ICECAT_USER = os.getenv('ICECAT_USER')
ICECAT_PASS = os.getenv('ICECAT_PASS')
FILES_INDEX_URL = "https://data.icecat.biz/export/freexml/EN/files.index.xml.gz"

def get_auth():
    return HTTPBasicAuth(ICECAT_USER, ICECAT_PASS)

def load_targets():
    if not os.path.exists(TARGETS_FILE):
        print("Error: targets.txt not found.")
        return []
    targets = []
    with open(TARGETS_FILE, "r", encoding="utf-8") as f:
        for line in f:
            if line.strip() and not line.strip().startswith("#"):
                targets.append(line.strip())
    return targets

def download_files_index():
    os.makedirs(DATA_DIR, exist_ok=True)
    if not os.path.exists(FILES_INDEX_XML):
        print(f"Downloading Index...")
        r = requests.get(FILES_INDEX_URL, auth=get_auth(), stream=True)
        with open(FILES_INDEX_XML, 'wb') as f:
            for chunk in r.iter_content(chunk_size=8192):
                f.write(chunk)

def load_category_map():
    cat_map = {}
    if os.path.exists(CATEGORIES_CSV):
        with open(CATEGORIES_CSV, mode='r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                cat_map[row['ID']] = row['Name']
    return cat_map

def get_target_category_ids(cat_map, target_names):
    target_ids = []
    target_names_lower = [t.lower() for t in target_names]
    for cat_id, name in cat_map.items():
        for target in target_names_lower:
            if target in name.lower():
                target_ids.append(cat_id)
                break
    return list(set(target_ids))

def download_xml_only(product_path, cat_name):
    """Downloads XML and saves it to disk. Does NOT parse."""
    product_url = f"https://data.icecat.biz/{product_path}"
    
    # Calculate destination
    safe_cat_name = cat_name.replace(" ", "_").replace("/", "-").replace("&", "and")
    xml_cat_dir = os.path.join(XML_SAVE_DIR, safe_cat_name)
    filename = os.path.basename(product_path)
    local_path = os.path.join(xml_cat_dir, filename)

    # SKIP if already exists
    if os.path.exists(local_path):
        return False # Skipped

    try:
        os.makedirs(xml_cat_dir, exist_ok=True)
        response = requests.get(product_url, auth=get_auth(), timeout=10)
        if response.status_code == 200:
            with open(local_path, 'wb') as f:
                f.write(response.content)
            return True # Downloaded
    except Exception as e:
        print(f"Error downloading {product_url}: {e}")
    return False

def main():
    if not ICECAT_USER:
        print("Error: Credentials missing.")
        return

    targets = load_targets()
    if not targets: return

    download_files_index()
    cat_map = load_category_map()
    target_ids = get_target_category_ids(cat_map, targets)
    
    print(f"Checking for new XMLs in {len(target_ids)} categories...")

    total_new = 0
    total_skipped = 0
    
    with gzip.open(FILES_INDEX_XML, 'rb') as f:
        context = ET.iterparse(f, events=('end',))
        for _, elem in context:
            if elem.tag == 'file':
                cid = elem.get('Catid')
                if cid in target_ids:
                    path = elem.get('path')
                    if path:
                        cat_name = cat_map.get(cid, "Unknown")
                        downloaded = download_xml_only(path, cat_name)
                        if downloaded:
                            total_new += 1
                            if total_new % 100 == 0: print(f"Downloaded {total_new} new XMLs...")
                        else:
                            total_skipped += 1
                elem.clear()

    print(f"Done. Downloaded: {total_new}. Skipped (Already existed): {total_skipped}.")

if __name__ == "__main__":
    main()