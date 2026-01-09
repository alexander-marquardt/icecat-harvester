import requests
import gzip
import xml.etree.ElementTree as ET
from requests.auth import HTTPBasicAuth
import os
import csv
from dotenv import load_dotenv
import json

# Load variables from .env file
load_dotenv()

# --- CONFIGURATION ---
ICECAT_USER = os.getenv('ICECAT_USER')
ICECAT_PASS = os.getenv('ICECAT_PASS')

CATS_URL = "https://data.icecat.biz/export/freexml/refs/CategoriesList.xml.gz"
FILES_INDEX_URL = "https://data.icecat.biz/export/freexml/EN/files.index.xml.gz"

CATEGORIES_CSV = "categories.csv"
FILES_INDEX_XML = "files.index.xml.gz"
OUTPUT_DIR = "products"  # Folder where category files will go

# Limit downloads per category (Set to 0 for unlimited)
LIMIT_PER_CATEGORY = 1 

# --- CATEGORIES TO DOWNLOAD ---
TARGET_CATEGORIES = [
    "Overshoes & Overboots", 
    "Men's Shoes",
    "Women's Shoes",
    "Shoes",
    "Racket Sports Shoes",
    "Athletic Shoes",
    "Cycling Shoes",
]

def get_auth():
    return HTTPBasicAuth(ICECAT_USER, ICECAT_PASS)

def ensure_categories_csv():
    if not os.path.exists(CATEGORIES_CSV):
        download_categories_list()

def download_categories_list():
    print(f"Downloading Categories List to {CATEGORIES_CSV}...")
    try:
        response = requests.get(CATS_URL, auth=get_auth(), stream=True)
        if response.status_code != 200:
            print(f"Error: Failed to download CategoriesList (Status {response.status_code})")
            return

        with gzip.open(response.raw, 'rb') as f, open(CATEGORIES_CSV, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(['ID', 'Name'])
            
            context = ET.iterparse(f, events=('end',))
            count = 0
            for _, elem in context:
                if elem.tag.endswith('Category'):
                    cat_id = elem.get('ID')
                    name = "Unknown"
                    for child in elem:
                        if child.tag.endswith('Name') and child.get('ID') == '1':
                            name = child.get('Value')
                            break
                    if cat_id and name != "Unknown":
                        writer.writerow([cat_id, name])
                        count += 1
                    elem.clear()
            print(f"Success! Saved {count} categories.")

    except Exception as e:
        print(f"Error processing categories XML: {e}")

def download_files_index():
    if not os.path.exists(FILES_INDEX_XML):
        print(f"Downloading {FILES_INDEX_XML} (this is large)...")
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

def get_target_category_ids(cat_map):
    target_ids = []
    print(f"Searching for categories matching: {TARGET_CATEGORIES}")
    for cat_id, name in cat_map.items():
        for target in TARGET_CATEGORIES:
            if target.lower() in name.lower():
                target_ids.append(cat_id)
                
    unique_ids = list(set(target_ids))
    print(f"Found {len(unique_ids)} matching category IDs.")
    return unique_ids

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
            "description": "",
            "image_url": None,
            "images": [],
            "specs": {}
        }

        # Description
        desc_node = product.find(".//ProductDescription")
        if desc_node is not None:
            item["description"] = desc_node.get("LongDesc")

        # Images
        image_list = []
        main_pic = product.get("HighPic")
        if main_pic: image_list.append(main_pic)
            
        for pic in product.findall(".//ProductGallery/ProductPicture"):
            url = pic.get("Pic")
            if url: image_list.append(url)
        
        # Clean Images (HTTPS + Unique)
        clean_images = []
        for img in image_list:
            if img:
                secure_url = img.replace("http://", "https://")
                if secure_url not in clean_images:
                    clean_images.append(secure_url)
        
        item["images"] = clean_images
        if clean_images:
            item["image_url"] = clean_images[0]

        # Specs
        for feature in product.findall(".//ProductFeature"):
            try:
                value = feature.get("Presentation_Value")
                name = None
                local_feat = feature.find(".//LocalFeature")
                if local_feat is not None:
                    feat_node = local_feat.find("Feature")
                    if feat_node is not None:
                        name = feat_node.get("Value")
                
                if not name:
                    name = f"Feature_{feature.get('CategoryFeature_ID')}"

                if name and value:
                    item["specs"][name] = value
            except:
                continue

        return item

    except Exception as e:
        return None

def fetch_product_data(product_path, cat_id, cat_name):
    product_url = f"https://data.icecat.biz/{product_path}"
    try:
        response = requests.get(product_url, auth=get_auth(), timeout=10)
        if response.status_code == 200:
            return parse_xml_to_json(response.content, cat_id, cat_name)
        else:
            return None
    except Exception as e:
        print(f"Error downloading {product_url}: {e}")
        return None

def main():
    if not ICECAT_USER or not ICECAT_PASS:
        print("Error: ICECAT_USER/PASS not set in .env")
        return

    ensure_categories_csv()
    download_files_index()

    cat_map = load_category_map()
    target_ids = get_target_category_ids(cat_map)
    
    if not target_ids:
        print("No target categories found.")
        return

    # Create output directory
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    
    print(f"Starting downloads into '{OUTPUT_DIR}/' folder...")
    
    cat_counts = {cid: 0 for cid in target_ids}
    total_saved = 0
    
    try:
        with gzip.open(FILES_INDEX_XML, 'rb') as f:
            context = ET.iterparse(f, events=('end',))
            
            for _, elem in context:
                if elem.tag == 'file':
                    cid = elem.get('Catid')
                    
                    if cid in target_ids:
                        if LIMIT_PER_CATEGORY > 0 and cat_counts[cid] >= LIMIT_PER_CATEGORY:
                            elem.clear()
                            continue
                            
                        product_path = elem.get('path')
                        if product_path:
                            # 1. Get readable Category Name
                            raw_cat_name = cat_map.get(cid, "Unknown")
                            
                            # 2. Fetch Data
                            product_data = fetch_product_data(product_path, cid, raw_cat_name)
                            
                            if product_data:
                                # 3. Generate Filename: products/Mens_Shoes.ndjson
                                safe_name = raw_cat_name.replace(" ", "_").replace("/", "-").replace("&", "and")
                                file_path = os.path.join(OUTPUT_DIR, f"{safe_name}.ndjson")
                                
                                # 4. Append to that specific file
                                with open(file_path, 'a', encoding='utf-8') as outfile:
                                    outfile.write(json.dumps(product_data) + "\n")
                                
                                cat_counts[cid] += 1
                                total_saved += 1
                                
                                if total_saved % 5 == 0:
                                    print(f"   Saved {total_saved} products... (Last: {raw_cat_name})")

                    elem.clear()
                    
    except FileNotFoundError:
        print(f"Error: {FILES_INDEX_XML} missing.")
        
    print(f"\nDone! Total {total_saved} products saved.")

if __name__ == "__main__":
    main()