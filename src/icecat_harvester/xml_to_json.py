import os
import json
import csv
import shutil
import hashlib
import xml.etree.ElementTree as ET
import argparse
import random
import re
from datetime import datetime
from tqdm import tqdm

# --- CONFIGURATION ---
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
# Move up two levels: src/icecat_harvester/ -> src -> root
PROJECT_ROOT = os.path.abspath(os.path.join(SCRIPT_DIR, "..", ".."))

DATA_DIR = os.path.join(PROJECT_ROOT, "data")
XML_SOURCE_DIR = os.path.join(DATA_DIR, "xml_source")
JSON_OUTPUT_DIR = os.path.join(DATA_DIR, "products")
SAMPLE_DATA_DIR = os.path.join(DATA_DIR, "sample-data")
FEATURES_CSV = os.path.join(DATA_DIR, "features.csv")
PRICES_NDJSON = os.path.join(DATA_DIR, "price_baselines.ndjson")

BATCH_SIZE = 1000 

# --- LOADERS ---
def load_feature_map():
    f_map = {}
    if os.path.exists(FEATURES_CSV):
        with open(FEATURES_CSV, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                f_map[row['ID']] = row['Name']
    return f_map

def load_price_map():
    p_map = {}
    if os.path.exists(PRICES_NDJSON):
        try:
            with open(PRICES_NDJSON, 'r', encoding='utf-8') as f:
                for line in f:
                    if not line.strip(): continue
                    data = json.loads(line)
                    if "name" in data and "price" in data:
                        p_map[data["name"].lower()] = float(data["price"])
        except Exception:
            pass
    return p_map

# --- HELPERS ---
def clean_html_text(text):
    if not text:
        return ""
    text = re.sub(r'<[^>]+>', ' ', text)
    return " ".join(text.split())

def get_heuristic_fallback(cat_name):
    if not cat_name: return 50.0
    name = cat_name.lower()
    if "server" in name: return 1500
    if "laptop" in name: return 800
    if "software" in name: return 100
    if "cable" in name: return 15
    return 45.0 

def estimate_price(prod_id, cat_name, brand_name, price_map):
    if not prod_id: return 0.0
    base = 50.0
    if cat_name:
        key = cat_name.lower()
        base = price_map.get(key, get_heuristic_fallback(key))

    multiplier = 1.0
    if brand_name:
        b = brand_name.lower()
        if b in ["apple", "samsung", "sony", "hp", "dell", "lenovo", "bose", "cisco"]:
            multiplier = 1.3 
        elif b in ["trust", "hama", "generic", "startech", "sweex"]:
            multiplier = 0.8 
    
    base = base * multiplier
    hash_val = int(hashlib.md5(prod_id.encode()).hexdigest(), 16)
    rand_factor = (hash_val % 1000) / 1000.0 
    variance = base * 0.6 
    price = base + (variance * (rand_factor - 0.5))
    return round(max(price, 1.0), 2)

# --- PARSER ---
def parse_icecat_xml(xml_path, feature_map, price_map):
    try:
        tree = ET.parse(xml_path)
        root = tree.getroot()
        product = root.find(".//Product")
        if product is None: 
            if root.tag.endswith("Product"): product = root
            else: return None

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

        grouped_specs = {}
        attrs = {}  
        
        for feature in product.findall(".//ProductFeature"):
            raw_value = feature.get("Presentation_Value")
            if not raw_value or raw_value in ["Y", "N", "Yes", "No"]: continue

            feat_name = None
            feat_node = feature.find(".//Feature")
            if feat_node is not None:
                name_node = feat_node.find(".//Name")
                if name_node is not None:
                    feat_name = name_node.get("Value")
            
            if not feat_name:
                feat_id = feature.get("Local_ID")
                feat_name = feature_map.get(feat_id, f"Feature_{feat_id}")

            safe_name = feat_name.replace("|", "/").replace(".", "")
            attrs[safe_name] = raw_value.replace("|", "/")

            display_str = f"{feat_name}: {raw_value}"
            group_id = feature.get("CategoryFeatureGroup_ID")
            group_info = group_map.get(group_id, {"name": "General", "order": 9999})
            
            g_name = group_info['name']
            if g_name not in grouped_specs:
                grouped_specs[g_name] = {"order": group_info['order'], "items": []}
            grouped_specs[g_name]["items"].append(display_str)

        title = product.get("Title") or ""
        brand = ""
        supplier = product.find(".//Supplier")
        if supplier is not None: brand = supplier.get("Name")
        
        desc_parts = [title]
        desc_node = product.find(".//ProductDescription")
        if desc_node is not None:
            long_desc = desc_node.get("LongDesc")
            if long_desc and len(long_desc) > 20:
                desc_parts.append("\n\n" + clean_html_text(long_desc))

        if grouped_specs:
            desc_parts.append("\n\nKey Specifications:")
            for g_name, g_data in sorted(grouped_specs.items(), key=lambda x: x[1]['order']):
                items_str = "; ".join(g_data['items'])
                if items_str:
                    desc_parts.append(f"- **{g_name}**: {items_str}")

        item = {
            "id": product.get("ID"),
            "title": title,
            "brand": brand,
            "description": "\n".join(desc_parts),
            "image_url": None,
            "price": None, 
            "currency": "EUR",
            "categories": [],
            "attrs": attrs,                
            "attr_keys": sorted(list(attrs.keys()))
        }
        
        cat_node = product.find(".//Category")
        cat_name_val = None
        if cat_node is not None:
             cat_name = cat_node.find(".//Name")
             if cat_name is not None:
                 cat_name_val = cat_name.get("Value")
                 item["categories"].append(cat_name_val)

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
        return None

def flush_batch(cat_json_dir, batch_data, batch_index):
    if not batch_data: return
    filename = f"batch_{batch_index:03d}.ndjson"
    filepath = os.path.join(cat_json_dir, filename)
    with open(filepath, "w", encoding="utf-8") as f:
        for item in batch_data:
            f.write(json.dumps(item, ensure_ascii=False) + "\n")

def main():
    parser = argparse.ArgumentParser(description="Convert Icecat XML to NDJSON.")
    parser.add_argument("--generate-sample-data", type=int, default=0)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--max-input-files", type=int, default=0)
    parser.add_argument("--max-output-records", type=int, default=0)
    parser.add_argument("--yes", action="store_true")
    parser.add_argument("--output-subdir", type=str, default="")
    args = parser.parse_args()

    random.seed(args.seed)

    if args.generate_sample_data > 0:
        print(f"🚀 Seeding Sample Data ({args.generate_sample_data} per category, seed={args.seed})...")
        if os.path.exists(SAMPLE_DATA_DIR):
            shutil.rmtree(SAMPLE_DATA_DIR)
        os.makedirs(SAMPLE_DATA_DIR, exist_ok=True)
        
        feature_map = load_feature_map()
        price_map = load_price_map()
        categories = sorted([d for d in os.listdir(XML_SOURCE_DIR) if os.path.isdir(os.path.join(XML_SOURCE_DIR, d))])
        
        for cat in tqdm(categories, desc="Seeding"):
            cat_dir = os.path.join(XML_SOURCE_DIR, cat)
            xml_files = sorted([f for f in os.listdir(cat_dir) if f.endswith(".xml")])
            random.shuffle(xml_files)
            
            sample_count = 0
            sample_filepath = os.path.join(SAMPLE_DATA_DIR, f"{cat.replace(' ', '_')}.ndjson")
            
            with open(sample_filepath, "w", encoding="utf-8") as sf:
                for xml_file in xml_files:
                    if sample_count >= args.generate_sample_data: break
                    item = parse_icecat_xml(os.path.join(cat_dir, xml_file), feature_map, price_map)
                    if item and item.get("title") and item.get("image_url"):
                        sf.write(json.dumps(item, ensure_ascii=False) + "\n")
                        sample_count += 1
        print(f"✅ Samples created in {SAMPLE_DATA_DIR}")
        return

    # --- PRODUCTION EXTRACTION ---
    if args.output_subdir:
        json_output_dir = os.path.join(JSON_OUTPUT_DIR, args.output_subdir)
        if os.path.exists(json_output_dir) and not args.yes:
            confirm = input(f"⚠️  Overwrite {json_output_dir}? [y/N]: ").lower().strip()
            if confirm != 'y': return
            shutil.rmtree(json_output_dir)
    else:
        json_output_dir = os.path.join(JSON_OUTPUT_DIR, datetime.now().strftime("%Y%m%d-%H%M%S"))

    os.makedirs(json_output_dir, exist_ok=True)
    feature_map = load_feature_map()
    price_map = load_price_map()

    categories = sorted([d for d in os.listdir(XML_SOURCE_DIR) if os.path.isdir(os.path.join(XML_SOURCE_DIR, d))])
    stats = {"converted": 0, "skipped": 0, "errors": 0}
    total_records_written = 0

    for cat in tqdm(categories, unit="cat"):
        if args.max_output_records and total_records_written >= args.max_output_records: break
        
        cat_dir = os.path.join(XML_SOURCE_DIR, cat)
        xml_files = sorted([f for f in os.listdir(cat_dir) if f.endswith(".xml")])
        random.shuffle(xml_files)
        files_to_process = xml_files[:args.max_input_files] if args.max_input_files > 0 else xml_files

        cat_json_dir = os.path.join(json_output_dir, cat)
        os.makedirs(cat_json_dir, exist_ok=True)
        batch_data, batch_index = [], 1

        for xml_file in files_to_process:
            if args.max_output_records and total_records_written >= args.max_output_records: break
            item = parse_icecat_xml(os.path.join(cat_dir, xml_file), feature_map, price_map)
            if item and item.get("title") and item.get("image_url"):
                batch_data.append(item)
                stats["converted"] += 1
                total_records_written += 1
                if len(batch_data) >= BATCH_SIZE:
                    flush_batch(cat_json_dir, batch_data, batch_index)
                    batch_data, batch_index = [], batch_index + 1
            else:
                stats["skipped"] += 1

        if batch_data: flush_batch(cat_json_dir, batch_data, batch_index)

    print(f"\n✅ Converted: {stats['converted']} | ⏭️  Skipped: {stats['skipped']}")

if __name__ == "__main__":
    main()