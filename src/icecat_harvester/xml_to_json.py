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

# How many products to store in one .ndjson file before creating a new one
BATCH_SIZE = 1000 

def load_feature_map():
    f_map = {}
    if os.path.exists(FEATURES_CSV):
        print(f"Loading feature map from {FEATURES_CSV}...")
        with open(FEATURES_CSV, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                f_map[row['ID']] = row['Name']
    return f_map

def estimate_price(prod_id, category_name):
    """
    Generates a deterministic fake price based on category and ID.
    This ensures the same product always gets the same price (stable for demos),
    but the price range is realistic for the category.
    """
    if not prod_id: return 0.0
    
    # 1. Base Randomness (Deterministic)
    # Hash ID to get a float between 0.0 and 1.0
    hash_val = int(hashlib.md5(prod_id.encode()).hexdigest(), 16)
    rand_factor = (hash_val % 1000) / 1000.0 # 0.0 to 1.0
    
    # 2. Category Baselines (Approximate average price in EUR)
    # Add common categories here to make the data look smarter
    baselines = {
        "Laptops": 800,
        "Tablets": 400,
        "Smartphones": 500,
        "TVs": 600,
        "Monitors": 250,
        "Memory": 80,
        "Processors": 300,
        "Hard Drives": 100,
        "SSD": 120,
        "Motherboards": 150,
        "Video Cards": 400,
        "Cables": 15,
        "Keyboards": 40,
        "Mice": 30,
        "Headphones": 60,
        "Software": 100,
        "Servers": 1500,
        "Printers": 200,
        "Toner Cartridges": 80
    }
    
    # Fuzzy matching for category keys
    base = 50 # Default fallback
    if category_name:
        for key, val in baselines.items():
            if key.lower() in category_name.lower():
                base = val
                break
    
    # 3. Calculate Price with Variance
    # Price = Base +/- 40% variance
    variance = base * 0.4
    price = base + (variance * 2 * (rand_factor - 0.5))
    
    # Round to "nice" retail numbers (e.g. 19.95 is prettier than 19.9234)
    return round(price, 2)

def parse_icecat_xml(xml_path, feature_map):
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

            # Name Resolution (Name Tag > Map > ID)
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

            # Elastic Safe Strings
            safe_name = feat_name.replace("|", "/")
            safe_val = raw_value.replace("|", "/")
            
            # Deduplication
            facet_str = f"{safe_name}|{safe_val}"
            if facet_str not in specs_facets:
                specs_facets.append(facet_str)
            
            if safe_name not in specs_names:
                specs_names.append(safe_name)

            # Description Grouping
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
            "price": None, # Filled below
            "currency": "EUR",
            "specs_facets": specs_facets,
            "specs_names": specs_names,
            "categories": []
        }
        
        # Categories & Price
        cat_val = "Unknown"
        cat_node = product.find(".//Category")
        if cat_node is not None:
             cat_name = cat_node.find(".//Name")
             if cat_name is not None:
                 cat_val = cat_name.get("Value")
                 item["categories"].append(cat_val)

        # Generate Sensible Price
        item["price"] = estimate_price(item["id"], cat_val)

        # Images
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
        # Swallow parsing errors on individual files to keep the batch moving
        # (The main loop counts errors)
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
    parser.add_argument("--limit", type=int, default=0, help="Limit files per category")
    parser.add_argument("--sample", action="store_true", help="Randomly sample files")
    parser.add_argument("--yes", action="store_true", help="Skip confirmation prompt")
    args = parser.parse_args()

    # --- 1. Cleanup ---
    if os.path.exists(JSON_OUTPUT_DIR):
        if not args.yes:
            confirm = input(f"⚠️  Overwrite {JSON_OUTPUT_DIR}? [y/N]: ").lower().strip()
            if confirm != 'y': return
        shutil.rmtree(JSON_OUTPUT_DIR)
    
    os.makedirs(JSON_OUTPUT_DIR, exist_ok=True)
    print(f"Output Directory: {JSON_OUTPUT_DIR}")

    # --- 2. Resources ---
    feature_map = load_feature_map()
    if not feature_map:
        print("⚠️  Warning: Feature map empty. IDs will be used.")

    if not os.path.exists(XML_SOURCE_DIR):
        print("Error: XML Source missing.")
        return

    categories = [d for d in os.listdir(XML_SOURCE_DIR) if os.path.isdir(os.path.join(XML_SOURCE_DIR, d))]
    stats = {"converted": 0, "skipped": 0, "errors": 0}

    # --- 3. Processing ---
    with tqdm(categories, unit="cat") as pbar_cat:
        for cat in pbar_cat:
            pbar_cat.set_description(f"Processing {cat[:15]}")
            
            cat_dir = os.path.join(XML_SOURCE_DIR, cat)
            xml_files = [f for f in os.listdir(cat_dir) if f.endswith(".xml")]
            
            if not xml_files: continue
            
            # Sampling
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
                    item = parse_icecat_xml(xml_path, feature_map)
                    
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

            # Flush remainder
            if batch_data:
                flush_batch(cat_json_dir, batch_data, batch_index)

    print("\n--- Complete ---")
    print(f"✅ Converted: {stats['converted']}")
    print(f"❌ Skipped:   {stats['skipped']}")
    print(f"⚠️  Errors:    {stats['errors']}")

if __name__ == "__main__":
    main()