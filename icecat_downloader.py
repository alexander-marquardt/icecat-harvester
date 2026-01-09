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
OUTPUT_DIR = "products"

# Limit downloads per category for testing (Set to 0 for unlimited)
LIMIT_PER_CATEGORY = 20 

# --- CATEGORIES TO DOWNLOAD ---
# (I kept your Shoe list, but this works for Electronics too if you change strings)
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
    """Returns the HTTPBasicAuth for Icecat."""
    return HTTPBasicAuth(ICECAT_USER, ICECAT_PASS)

def ensure_categories_csv():
    """Ensures that the categories.csv file exists."""
    if not os.path.exists(CATEGORIES_CSV):
        download_categories_list()

def download_categories_list():
    """Downloads and parses the CategoriesList.xml.gz to create categories.csv."""
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
    """Downloads the files.index.xml.gz file."""
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
    """Loads the category map from categories.csv."""
    cat_map = {}
    ensure_categories_csv()
    with open(CATEGORIES_CSV, mode='r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            cat_map[row['ID']] = row['Name']
    return cat_map

def get_target_category_ids(cat_map):
    """Gets the category IDs for the target categories."""
    target_ids = []
    print(f"Searching for categories matching: {TARGET_CATEGORIES}")
    for cat_id, name in cat_map.items():
        # Check explicit list
        for target in TARGET_CATEGORIES:
            if target.lower() == name.lower(): # Exact match preferred
                target_ids.append(cat_id)
            elif target.lower() in name.lower(): # Partial match
                target_ids.append(cat_id)
                
    unique_ids = list(set(target_ids))
    print(f"Found {len(unique_ids)} matching category IDs.")
    return unique_ids

def parse_xml_to_json(xml_content, cat_id, cat_name):
    """
    Parses raw Icecat XML bytes into a clean JSON dictionary.
    """
    try:
        root = ET.fromstring(xml_content)
        product = root.find("Product")
        if product is None: return None

        # 1. Basic Info
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

        # 2. Description
        desc_node = product.find(".//ProductDescription")
        if desc_node is not None:
            item["description"] = desc_node.get("LongDesc")

        # 3. Images (Consolidate HighPic and Gallery)
        image_list = []
        
        # A. Main High Res Image (Attribute on Product tag)
        main_pic = product.get("HighPic")
        if main_pic:
            image_list.append(main_pic)
            
        # B. Gallery Images
        for pic in product.findall(".//ProductGallery/ProductPicture"):
            url = pic.get("Pic")
            if url:
                image_list.append(url)
        
        # Clean up images (HTTPS + Unique)
        clean_images = []
        for img in image_list:
            if img:
                # FIX: Force HTTPS to avoid mixed content warnings in demo
                secure_url = img.replace("http://", "https://")
                if secure_url not in clean_images:
                    clean_images.append(secure_url)
        
        item["images"] = clean_images
        if clean_images:
            item["image_url"] = clean_images[0] # Convenience field for main image

        # 4. Specs / Features
        # Icecat structure: ProductFeature -> LocalFeature -> Feature -> Name
        for feature in product.findall(".//ProductFeature"):
            try:
                # The value (e.g., "Red", "100")
                value = feature.get("Presentation_Value")
                
                # The label (e.g., "Color", "Weight")
                # We need to dig into LocalFeature to find the English name
                name = None
                local_feat = feature.find(".//LocalFeature")
                if local_feat is not None:
                    feat_node = local_feat.find("Feature")
                    if feat_node is not None:
                        name = feat_node.get("Value")
                
                # Fallback: if no local name, use the ID (not ideal but better than nothing)
                if not name:
                    name = f"Feature_{feature.get('CategoryFeature_ID')}"

                if name and value:
                    item["specs"][name] = value
            except:
                continue

        return item

    except Exception as e:
        print(f"Error parsing XML: {e}")
        return None

def download_product_xml(product_path, cat_id, cat_map):
    """Downloads XML, converts to JSON, and saves."""
    product_url = f"https://data.icecat.biz/{product_path}"
    
    # Create folder structure: products/Mens_Shoes/
    category_name = cat_map.get(cat_id, "Unknown_Category")
    safe_cat_name = "".join(x for x in category_name if x.isalnum() or x in " -_").strip().replace(" ", "_")
    category_dir = os.path.join(OUTPUT_DIR, safe_cat_name)
    os.makedirs(category_dir, exist_ok=True)
    
    # Check if JSON already exists
    file_name_json = os.path.basename(product_path).replace('.xml', '.json')
    output_path_json = os.path.join(category_dir, file_name_json)

    if os.path.exists(output_path_json):
        return # Skip if already done

    # print(f"Processing {product_url}...") 
    try:
        response = requests.get(product_url, auth=get_auth(), timeout=10)
        if response.status_code == 200:
            # CONVERT XML TO JSON HERE
            product_data = parse_xml_to_json(response.content, cat_id, category_name)
            
            if product_data:
                with open(output_path_json, 'w', encoding='utf-8') as f:
                    json.dump(product_data, f, indent=2)
                print(f"Saved: {file_name_json}")
            else:
                print(f"Skipped (Empty Data): {product_url}")
        else:
            print(f"Failed to download {product_url} (Status {response.status_code})")
    except Exception as e:
        print(f"Error downloading {product_url}: {e}")

def download_products():
    """Main execution loop."""
    if not ICECAT_USER or not ICECAT_PASS:
        print("Error: ICECAT_USER/PASS not set in .env")
        return

    ensure_categories_csv()
    download_files_index()

    cat_map = load_category_map()
    target_ids = get_target_category_ids(cat_map)
    
    if not target_ids:
        print("No target categories found. Check your spelling in TARGET_CATEGORIES.")
        return

    os.makedirs(OUTPUT_DIR, exist_ok=True)

    print(f"Scanning index for {len(target_ids)} categories...")
    
    # Track counts to limit downloads
    cat_counts = {cid: 0 for cid in target_ids}
    
    try:
        with gzip.open(FILES_INDEX_XML, 'rb') as f:
            context = ET.iterparse(f, events=('end',))
            for _, elem in context:
                if elem.tag == 'file':
                    cid = elem.get('Catid')
                    if cid in target_ids:
                        # Check limit
                        if LIMIT_PER_CATEGORY > 0 and cat_counts[cid] >= LIMIT_PER_CATEGORY:
                            elem.clear()
                            continue
                            
                        product_path = elem.get('path')
                        if product_path:
                            download_product_xml(product_path, cid, cat_map)
                            cat_counts[cid] += 1
                            
                    elem.clear()
    except FileNotFoundError:
        print(f"Error: {FILES_INDEX_XML} missing.")

if __name__ == "__main__":
    download_products()