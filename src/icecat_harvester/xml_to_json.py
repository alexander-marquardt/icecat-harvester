import os
import json
import csv
import shutil
import hashlib
import xml.etree.ElementTree as ET
import argparse
import random
from tqdm import tqdm

# --- CONFIGURATION ---
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(os.path.dirname(SCRIPT_DIR))
DATA_DIR = os.path.join(PROJECT_ROOT, "data")
XML_SOURCE_DIR = os.path.join(DATA_DIR, "xml_source")
JSON_OUTPUT_DIR = os.path.join(DATA_DIR, "json_products")
FEATURES_CSV = os.path.join(DATA_DIR, "features.csv")
# CHANGED: Now looking for NDJSON
PRICES_NDJSON = os.path.join(DATA_DIR, "price_baselines.ndjson")

BATCH_SIZE = 1000 

# --- LOADERS ---
def load_feature_map():
    f_map = {}
    if os.path.exists(FEATURES_CSV):
        # print(f"Loading feature map from {FEATURES_CSV}...")
        with open(FEATURES_CSV, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                f_map[row['ID']] = row['Name']
    return f_map

def load_price_map():
    """
    Loads category base prices from NDJSON.
    Returns a dict: { "category name (lowercase)": float_price }
    """
    p_map = {}
    if os.path.exists(PRICES_NDJSON):
        print(f"Loading price baselines from {PRICES_NDJSON}...")
        try:
            with open(PRICES_NDJSON, 'r', encoding='utf-8') as f:
                for line in f:
                    if not line.strip(): continue
                    data = json.loads(line)
                    if "name" in data and "price" in data:
                        # Normalize key to lowercase for easier matching
                        p_map[data["name"].lower()] = float(data["price"])
        except Exception as e:
            print(f"⚠️ Error reading price map: {e}")
    else:
        print(f"⚠️ Warning: {PRICES_NDJSON} not found. Prices will be estimated heuristics.")
    return p_map

# --- PRICING LOGIC ---
def get_heuristic_fallback(cat_name):
    """Fallback if Category Name not in our file."""
    if not cat_name: return 50.0
    name = cat_name.lower()
    
    # Generic keywords if exact match failed
    if "server" in name: return 1500
    if "laptop" in name: return 800
    if "software" in name: return 100
    if "cable" in name: return 15
    return 45.0 

def estimate_price(prod_id, cat_name, brand_name, price_map):
    """
    1. Lookup Baseline from Map (using Name).
    2. If missing, use Heuristic.
    3. Apply Brand Multiplier.
    4. Apply Deterministic Randomness.
    """
    if not prod_id: return 0.0
    
    # 1. Determine Base Price
    base = 50.0
    found_match = False
    
    if cat_name:
        key = cat_name.lower()
        if key in price_map:
            base = price_map[key]
            found_match = True
        else:
            base = get_heuristic_fallback(key)

    # 2. Brand Multiplier (Sensible Tiers)
    multiplier = 1.0
    if brand_name:
        b = brand_name.lower()
        if b in ["apple", "samsung", "sony", "hp", "dell", "lenovo", "bose", "cisco"]:
            multiplier = 1.3 # Premium brands
        elif b in ["trust", "hama", "generic", "startech", "sweex"]:
            multiplier = 0.8 # Budget brands
    
    base = base * multiplier

    # 3. Deterministic Variance (+/- 30%)
    # Hash ID to get stable random float 0.0-1.0
    hash_val = int(hashlib.md5(prod_id.encode()).hexdigest(), 16)
    rand_factor = (hash_val % 1000) / 1000.0 
    
    # Formula: Base + (Variance * (Random - 0.5))
    # e.g. 100 +/- 30
    variance = base * 0.6 # 60% total range
    price = base + (variance * (rand_factor - 0.5))
    
    # Ensure positive price and nice rounding
    if price < 1.0: price = 1.0 + rand_factor
    
    return round(price, 2)

# --- PARSER ---
def parse_icecat_xml(xml_path, feature_map, price_map):
    try:
        tree = ET.parse(xml_path)
        root = tree.getroot()
        product = root.find(".//Product")
        if product is None: 
            if root.tag.endswith("Product"): product = root
            else: return None

        # --- 1. Map Local Groups ---
        group_map = {} 
        for cfg in product.findall(".//CategoryFeatureGroup"):
            cfg_id = cfg.get("ID")
            order_no = int(cfg.get("No") or 999)
            fg_node = cfg.find(".//FeatureGroup")
            if fg_node is not None:
                name_node = fg_node.find(".//Name")
                if name_node is not None:
                    g_name = name_node.get("Value")
                    if cfg_id and g_name:
                        group_map[cfg_id] = {"name": g_name, "order": order_no}

        # --- 2. Extract Features ---
        grouped_specs = {}
        specs_facets = [] 
        specs_names = []  

        for feature in product.findall(".//ProductFeature"):
            raw_value = feature.get("Presentation_Value")
            if not raw_value or raw_value in ["Y", "N", "Yes", "No"]: continue

            # Name Resolution
            feat_name = None
            feat_node = feature.find(".//Feature")
            if feat_node is not None:
                name_node = feat_node.find(".//Name")
                if name_node is not None:
                    feat_name = name_node.get("Value")
            
            if not feat_name:
                feat_id = feature.get("Local_ID")
                feat_name = feature_map.get(feat_id)
            
            if not feat_name:
                feat_id = feature.get("Local_ID")
                feat_name = f"Feature_{feat_id}"

            safe_name = feat_name.replace("|", "/")
            safe_val = raw_value.replace("|", "/")
            
            facet_str = f"{safe_name}|{safe_val}"
            if facet_str not in specs_facets:
                specs_facets.append(facet_str)
            
            if safe_name not in specs_names:
                specs_names.append(safe_name)

            display_str = f"{feat_name}: {raw_value}"
            group_id = feature.get("CategoryFeatureGroup_ID")
            group_info = group_map.get(group_id)

            if group_info:
                g_name = group_info['name']
                if g_name not in grouped_specs:
                    grouped_specs[g_name] = {"order": group_info['order'], "items": []}
                grouped_specs[g_name]["items"].append(display_str)
            else:
                if "General" not in grouped_specs:
                    grouped_specs["General"] = {"order": 9999, "items": []}
                grouped_specs["General"]["items"].append(display_str)

        # --- 3. Synthesize Description ---
        title = product.get("Title") or ""
        brand = ""
        supplier = product.find(".//Supplier")
        if supplier is not None: brand = supplier.get("Name")
        
        desc_parts = [title]
        desc_node = product.find(".//ProductDescription")
        if desc_node is not None:
            long_desc = desc_node.get("LongDesc")
            if long_desc and len(long_desc) > 20:
                desc_parts.append("\n\n" + long_desc)

        if grouped_specs:
            desc_parts.append("\n\nKey Specifications:")
            sorted_groups = sorted(grouped_specs.items(), key=lambda x: x[1]['order'])
            for g_name, g_data in sorted_groups:
                items_str = "; ".join(g_data['items'])
                if items_str:
                    desc_parts.append(f"- **{g_name}**: {items_str}")

        full_description = "\n".join(desc_parts)

        # --- 4. Final Output ---
        item = {
            "id": product.get("ID"),
            "title": title,
            "brand": brand,
            "description": full_description,
            "image_url": None,
            "price": None, 
            "currency": "EUR",
            "specs_facets": specs_facets,
            "specs_names": specs_names,
            "categories": []
        }
        
        # Categories & Price Logic
        cat_node = product.find(".//Category")
        cat_name_val = None
        
        if cat_node is not None:
             cat_name = cat_node.find(".//Name")
             if cat_name is not None:
                 cat_name_val = cat_name.get("Value")
                 item["categories"].append(cat_name_val)

        # CALL PRICING ENGINE (Using Name Lookup)
        item["price"] = estimate_price(item["id"], cat_name_val, brand, price_map)

        priorities = ["Pic500x500", "Pic", "Original", "HighPic"]
        for pic in product.findall(".//ProductPicture"):
            for attr in priorities:
                url = pic.get(attr)
                if url and "http" in url:
                    item["image_url"] = url
                    break
            if item["image_url"]: break

        return item

    except Exception:
        raise

def flush_batch(cat_json_dir, batch_data, batch_index):
    if not batch_data: return
    filename = f"batch_{batch_index:03d}.ndjson"
    filepath = os.path.join(cat_json_dir, filename)
    with open(filepath, "w", encoding="utf-8") as f:
        for item in batch_data:
            f.write(json.dumps(item, ensure_ascii=False) + "\n")

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--limit", type=int, default=0)
    parser.add_argument("--sample", action="store_true")
    parser.add_argument("--yes", action="store_true")
    args = parser.parse_args()

    # Clean Output
    if os.path.exists(JSON_OUTPUT_DIR):
        if not args.yes:
            confirm = input(f"⚠️  Overwrite {JSON_OUTPUT_DIR}? [y/N]: ").lower().strip()
            if confirm != 'y': return
        shutil.rmtree(JSON_OUTPUT_DIR)
    os.makedirs(JSON_OUTPUT_DIR, exist_ok=True)

    # Load Maps
    feature_map = load_feature_map()
    price_map = load_price_map() # Now loads NDJSON
    
    if not os.path.exists(XML_SOURCE_DIR): return

    categories = [d for d in os.listdir(XML_SOURCE_DIR) if os.path.isdir(os.path.join(XML_SOURCE_DIR, d))]
    stats = {"converted": 0, "skipped": 0, "errors": 0}

    with tqdm(categories, unit="cat") as pbar_cat:
        for cat in pbar_cat:
            pbar_cat.set_description(f"Processing {cat[:15]}")
            
            cat_dir = os.path.join(XML_SOURCE_DIR, cat)
            xml_files = [f for f in os.listdir(cat_dir) if f.endswith(".xml")]
            
            if not xml_files: continue
            
            if args.limit > 0:
                if args.sample and len(xml_files) > args.limit:
                    files_to_process = random.sample(xml_files, args.limit)
                else:
                    files_to_process = xml_files[:args.limit]
            else:
                files_to_process = xml_files

            cat_json_dir = os.path.join(JSON_OUTPUT_DIR, cat)
            os.makedirs(cat_json_dir, exist_ok=True)
            batch_data = []
            batch_index = 1

            for xml_file in files_to_process:
                xml_path = os.path.join(cat_dir, xml_file)
                try:
                    # Pass maps
                    item = parse_icecat_xml(xml_path, feature_map, price_map)
                    
                    if item and item.get("title"):
                        batch_data.append(item)
                        stats["converted"] += 1
                        if len(batch_data) >= BATCH_SIZE:
                            flush_batch(cat_json_dir, batch_data, batch_index)
                            batch_data = []
                            batch_index += 1
                    else:
                        stats["skipped"] += 1
                except Exception:
                    stats["errors"] += 1

            if batch_data:
                flush_batch(cat_json_dir, batch_data, batch_index)

    print(f"\n✅ Converted: {stats['converted']}")

if __name__ == "__main__":
    main()