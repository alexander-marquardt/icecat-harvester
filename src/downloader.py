import requests
import gzip
import xml.etree.ElementTree as ET
from requests.auth import HTTPBasicAuth
import os
import csv
from dotenv import load_dotenv
import json
import sys

# --- PATH CONFIGURATION ---
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(SCRIPT_DIR)

# Everything now lives inside 'data/'
DATA_DIR = os.path.join(PROJECT_ROOT, "data")
OUTPUT_DIR = os.path.join(DATA_DIR, "products")      # data/products
XML_SAVE_DIR = os.path.join(DATA_DIR, "xml_source")  # data/xml_source
TARGETS_FILE = os.path.join(PROJECT_ROOT, "targets.txt")

# Load .env from project root
load_dotenv(os.path.join(PROJECT_ROOT, ".env"))

# --- CONFIGURATION ---
ICECAT_USER = os.getenv('ICECAT_USER')
ICECAT_PASS = os.getenv('ICECAT_PASS')

CATS_URL = "https://data.icecat.biz/export/freexml/refs/CategoriesList.xml.gz"
FILES_INDEX_URL = "https://data.icecat.biz/export/freexml/EN/files.index.xml.gz"

CATEGORIES_CSV = os.path.join(DATA_DIR, "categories.csv")
FILES_INDEX_XML = os.path.join(DATA_DIR, "files.index.xml.gz")

# Set to 0 to download EVERYTHING
LIMIT_PER_CATEGORY = 0  

def get_auth():
    return HTTPBasicAuth(ICECAT_USER, ICECAT_PASS)

def load_targets():
    """Reads target categories from targets.txt"""
    if not os.path.exists(TARGETS_FILE):
        print(f"Creating default targets file at {TARGETS_FILE}...")
        defaults = ["Smartphones", "Laptops", "Shoes"]
        with open(TARGETS_FILE, "w", encoding="utf-8") as f:
            f.write("\n".join(defaults))
        return defaults

    targets = []
    with open(TARGETS_FILE, "r", encoding="utf-8") as f:
        for line in f:
            clean_line = line.strip()
            if clean_line and not clean_line.startswith("#"):
                targets.append(clean_line)
    
    print(f"Loaded {len(targets)} target categories from {os.path.basename(TARGETS_FILE)}")
    return targets

def ensure_categories_csv():
    os.makedirs(DATA_DIR, exist_ok=True)
    if not os.path.exists(CATEGORIES_CSV):
        print(f"Downloading Categories List to {CATEGORIES_CSV}...")
        try:
            response = requests.get(CATS_URL, auth=get_auth(), stream=True)
            if response.status_code != 200:
                print(f"Error: Failed to download CategoriesList (Status {response.status_code})")
                return

            with gzip.open(response.raw, 'rb') as f, open(CATEGORIES_CSV, 'w', newline='', encoding='utf-8') as csvfile:
                writer = csv.writer(csvfile)
                writer.writerow(['ID', 'Name'])
                context = ET.iterparse(f, events=('start', 'end'))
                for event, elem in context:
                    if event == 'end':
                        if elem.tag == 'Category' or elem.tag.endswith('}Category'):
                            cat_id = elem.get('ID')
                            name = "Unknown"
                            for child in elem:
                                if child.tag.endswith('Name') and child.get('ID') == '1':
                                    name = child.get('Value')
                                    break
                            if name == "Unknown": name = elem.get('Name')
                            if cat_id and name and name != "Unknown":
                                writer.writerow([cat_id, name])
                            elem.clear()
                        elif 'VirtualCategory' in elem.tag:
                            elem.clear()
        except Exception as e:
            print(f"Error processing categories XML: {e}")

def download_files_index():
    os.makedirs(DATA_DIR, exist_ok=True)
    if os.path.exists(FILES_INDEX_XML):
        size_mb = os.path.getsize(FILES_INDEX_XML) / (1024 * 1024)
        if size_mb < 5: 
            print(f"Warning: Index file is too small ({size_mb:.2f} MB). Deleting...")
            os.remove(FILES_INDEX_XML)
    
    if not os.path.exists(FILES_INDEX_XML):
        print(f"Downloading {FILES_INDEX_XML} (Please wait)...")
        try:
            response = requests.get(FILES_INDEX_URL, auth=get_auth(), stream=True)
            if response.status_code != 200:
                print(f"Error: Failed to download index (Status {response.status_code})")
                return
            with open(FILES_INDEX_XML, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)
            print(f"Success! Saved {FILES_INDEX_XML}")
        except Exception as e:
            print(f"Error downloading index: {e}")

def load_category_map():
    cat_map = {}
    ensure_categories_csv()
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

def load_existing_product_ids():
    existing = set()
    if not os.path.exists(OUTPUT_DIR): return existing
    print("Scanning existing JSON files to resume downloads...")
    for filename in os.listdir(OUTPUT_DIR):
        if filename.endswith(".ndjson"):
            path = os.path.join(OUTPUT_DIR, filename)
            try:
                with open(path, 'r', encoding='utf-8') as f:
                    for line in f:
                        if line.strip():
                            data = json.loads(line)
                            existing.add(data.get("id"))
            except: pass
    print(f"Found {len(existing)} products already downloaded. Skipping them.")
    return existing

def parse_xml_to_json(xml_content, cat_id, cat_name):
    try:
        root = ET.fromstring(xml_content)
        product = root.find("Product")
        if product is None: return None
        item = {
            "id": product.get("ID"),
            "title": product.get("Name") or product.get("Title"),
            "brand": product.find("Supplier").get("Name") if product.find("Supplier") is not None else "Unknown",
            "category": cat_name,
            "category_id": cat_id,
            "description": product.find(".//ProductDescription").get("LongDesc") if product.find(".//ProductDescription") is not None else "",
            "image_url": None,
            "images": [],
            "specs": {}
        }
        image_list = []
        if product.get("HighPic"): image_list.append(product.get("HighPic"))
        for pic in product.findall(".//ProductGallery/ProductPicture"):
            if pic.get("Pic"): image_list.append(pic.get("Pic"))
        clean_images = []
        for img in image_list:
            secure = img.replace("http://", "https://")
            if secure not in clean_images: clean_images.append(secure)
        item["images"] = clean_images
        if clean_images: item["image_url"] = clean_images[0]
        for feature in product.findall(".//ProductFeature"):
            try:
                val = feature.get("Presentation_Value")
                name = None
                local = feature.find(".//LocalFeature")
                if local and local.find("Feature") is not None:
                    name = local.find("Feature").get("Value")
                if not name: name = f"Feature_{feature.get('CategoryFeature_ID')}"
                if name and val: item["specs"][name] = val
            except: continue
        return item
    except: return None

def fetch_product_data(product_path, cat_id, cat_name):
    product_url = f"https://data.icecat.biz/{product_path}"
    try:
        response = requests.get(product_url, auth=get_auth(), timeout=10)
        if response.status_code == 200:
            # 1. SAVE RAW XML
            safe_cat_name = cat_name.replace(" ", "_").replace("/", "-").replace("&", "and")
            xml_cat_dir = os.path.join(XML_SAVE_DIR, safe_cat_name)
            os.makedirs(xml_cat_dir, exist_ok=True)
            
            filename = os.path.basename(product_path)
            with open(os.path.join(xml_cat_dir, filename), 'wb') as f:
                f.write(response.content)

            # 2. PARSE TO JSON
            return parse_xml_to_json(response.content, cat_id, cat_name)
    except Exception as e:
        print(f"Error downloading {product_url}: {e}")
    return None

def main():
    if not ICECAT_USER or not ICECAT_PASS:
        print("Error: ICECAT_USER/PASS not set in .env")
        return

    target_names = load_targets()
    if not target_names:
        print("No targets found in targets.txt. Exiting.")
        return

    ensure_categories_csv()
    download_files_index()

    cat_map = load_category_map()
    target_ids = get_target_category_ids(cat_map, target_names)
    existing_ids = load_existing_product_ids() 
    
    if not target_ids:
        print("No matching categories found in Icecat index for your targets.")
        return

    # Ensure output directories exist inside DATA_DIR
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    os.makedirs(XML_SAVE_DIR, exist_ok=True)
    
    print(f"Starting downloads (Limit: {'Unlimited' if LIMIT_PER_CATEGORY == 0 else LIMIT_PER_CATEGORY})...")
    print(f"NDJSON files saving to: {OUTPUT_DIR}")
    print(f"XML files saving to: {XML_SAVE_DIR}")

    cat_counts = {cid: 0 for cid in target_ids}
    total_saved = 0
    total_skipped = 0
    
    try:
        with gzip.open(FILES_INDEX_XML, 'rb') as f:
            context = ET.iterparse(f, events=('end',))
            for _, elem in context:
                if elem.tag == 'file':
                    cid = elem.get('Catid')
                    if cid in target_ids:
                        pid = elem.get('Product_ID')
                        if not pid and elem.get('path'): pid = elem.get('path').split('/')[-1].replace('.xml','')
                        
                        if pid in existing_ids:
                            total_skipped += 1
                            elem.clear()
                            continue

                        if LIMIT_PER_CATEGORY > 0 and cat_counts[cid] >= LIMIT_PER_CATEGORY:
                            elem.clear()
                            continue
                            
                        path = elem.get('path')
                        if path:
                            raw_cat_name = cat_map.get(cid, "Unknown")
                            product_data = fetch_product_data(path, cid, raw_cat_name)
                            
                            if product_data:
                                safe_name = raw_cat_name.replace(" ", "_").replace("/", "-").replace("&", "and")
                                ndjson_path = os.path.join(OUTPUT_DIR, f"{safe_name}.ndjson")
                                
                                with open(ndjson_path, 'a', encoding='utf-8') as outfile:
                                    outfile.write(json.dumps(product_data) + "\n")
                                
                                existing_ids.add(product_data['id'])
                                cat_counts[cid] += 1
                                total_saved += 1
                                
                                if total_saved % 10 == 0:
                                    print(f"   Saved {total_saved} new products... (Skipped {total_skipped})")

                    elem.clear()
    except Exception as e:
        print(f"Error reading index: {e}")
        
    print(f"\nDone! Saved {total_saved} products (XML + JSON).")

if __name__ == "__main__":
    main()