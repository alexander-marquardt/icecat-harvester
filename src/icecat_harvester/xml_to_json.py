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
# Moves up 2 levels: src/icecat_harvester -> src -> root
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
        except Exception: pass
    return p_map

# --- HELPERS ---
def clean_html_text(text):
    if not text: return ""
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
    multiplier = 1.3 if brand_name and brand_name.lower() in ["apple", "samsung", "sony", "hp", "dell", "lenovo"] else 1.0
    base *= multiplier
    hash_val = int(hashlib.md5(prod_id.encode()).hexdigest(), 16)
    variance = base * 0.6 
    price = base + (variance * (((hash_val % 1000) / 1000.0) - 0.5))
    return round(max(price, 1.0), 2)

# --- PARSER ---
def parse_icecat_xml(xml_path, feature_map, price_map):
    try:
        tree = ET.parse(xml_path)
        root = tree.getroot()
        product = root.find(".//Product")
        if product is None: return None

        attrs = {}
        for feature in product.findall(".//ProductFeature"):
            raw_value = feature.get("Presentation_Value")
            if not raw_value or raw_value in ["Y", "N", "Yes", "No"]: continue
            
            feat_node = feature.find(".//Feature/Name")
            feat_name = feat_node.get("Value") if feat_node is not None else feature_map.get(feature.get("Local_ID"), f"Feature_{feature.get('Local_ID')}")
            if feat_name:
                attrs[feat_name.replace(".", "")] = raw_value

        title = product.get("Title") or ""
        brand = (product.find(".//Supplier").get("Name") if product.find(".//Supplier") is not None else "")
        
        item = {
            "id": product.get("ID"),
            "title": title,
            "brand": brand,
            "description": title, 
            "image_url": None,
            "price": 0.0, 
            "currency": "EUR",
            "categories": [],
            "attrs": attrs,                
            "attr_keys": sorted(list(attrs.keys()))
        }
        
        cat_node = product.find(".//Category/Name")
        if cat_node is not None:
            cat_val = cat_node.get("Value")
            item["categories"].append(cat_val)
            item["price"] = estimate_price(item["id"], cat_val, brand, price_map)
        else:
            item["price"] = estimate_price(item["id"], None, brand, price_map)

        for pic in product.findall(".//ProductPicture"):
            url = pic.get("Pic500x500") or pic.get("Pic")
            if url: 
                item["image_url"] = url
                break

        return item
    except Exception: return None

def flush_batch(cat_json_dir, batch_data, batch_idx):
    if not batch_data: return
    filepath = os.path.join(cat_json_dir, f"batch_{batch_idx:03d}.ndjson")
    with open(filepath, "w", encoding="utf-8") as f:
        for item in batch_data:
            f.write(json.dumps(item, ensure_ascii=False) + "\n")

# --- MAIN ---
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--generate-sample-data", type=int, default=0)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--max-input-files", type=int, default=0)
    parser.add_argument("--max-output-records", type=int, default=0)
    parser.add_argument("--output-subdir", type=str, default="")
    parser.add_argument("--yes", action="store_true")
    args = parser.parse_args()

    random.seed(args.seed)
    feature_map = load_feature_map()
    price_map = load_price_map()

    # Determine Output Directory
    is_sampling = args.generate_sample_data > 0
    if is_sampling:
        out_root = SAMPLE_DATA_DIR
        if os.path.exists(SAMPLE_DATA_DIR): shutil.rmtree(SAMPLE_DATA_DIR)
        os.makedirs(SAMPLE_DATA_DIR, exist_ok=True)
        print(f"🚀 SEEDING SAMPLES (N={args.generate_sample_data}, seed={args.seed})")
    else:
        timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
        out_root = os.path.join(JSON_OUTPUT_DIR, args.output_subdir or timestamp)
        if os.path.exists(out_root) and not args.yes:
            if input(f"⚠️ Overwrite {out_root}? [y/N]: ").lower() != 'y': return
            shutil.rmtree(out_root)
        os.makedirs(out_root, exist_ok=True)
        print(f"📦 PRODUCTION RUN -> {out_root}")

    # Initial fast scan for categories
    categories = sorted([d for d in os.listdir(XML_SOURCE_DIR) if os.path.isdir(os.path.join(XML_SOURCE_DIR, d))])
    total_written = 0
    stats = {"converted": 0, "skipped": 0}

    

    with tqdm(categories, unit="cat", desc="Extracted: 0") as pbar:
        for cat in pbar:
            if args.max_output_records and total_written >= args.max_output_records: break
            
            cat_src = os.path.join(XML_SOURCE_DIR, cat)
            cat_out = os.path.join(out_root, cat) if not is_sampling else out_root
            os.makedirs(cat_out, exist_ok=True)
            
            batch_data, batch_idx, cat_count = [], 1, 0
            
            # Use os.scandir for instant file access
            with os.scandir(cat_src) as entries:
                # If sampling, we need to collect and shuffle, otherwise we stream
                if is_sampling or args.max_input_files > 0:
                    files = []
                    # Optimization: only scan enough to satisfy max_input if provided
                    # but if sampling we might want more to pick 'randomly'
                    scan_limit = args.max_input_files * 3 if args.max_input_files else 1000
                    for entry in entries:
                        if entry.name.endswith(".xml"):
                            files.append(entry.name)
                            if is_sampling and len(files) >= scan_limit: break
                    
                    random.shuffle(files)
                    target_files = files[:args.generate_sample_data] if is_sampling else files[:args.max_input_files]
                else:
                    # Pure streaming for production
                    target_files = entries

                for entry in target_files:
                    if args.max_output_records and total_written >= args.max_output_records: break
                    
                    # Handle both strings (from sampling list) and DirEntry objects (from generator)
                    f_path = entry.path if hasattr(entry, 'path') else os.path.join(cat_src, entry)
                    if not f_path.endswith(".xml"): continue

                    item = parse_icecat_xml(f_path, feature_map, price_map)
                    if item and item.get("image_url"):
                        if is_sampling:
                            # Write to single category file for samples
                            sample_file = os.path.join(out_root, f"{cat}.ndjson")
                            with open(sample_file, "a", encoding="utf-8") as sf:
                                sf.write(json.dumps(item, ensure_ascii=False) + "\n")
                        else:
                            batch_data.append(item)
                        
                        cat_count += 1
                        total_written += 1
                        stats["converted"] += 1
                        
                        if not is_sampling and len(batch_data) >= BATCH_SIZE:
                            flush_batch(cat_out, batch_data, batch_idx)
                            batch_data, batch_idx = [], batch_idx + 1
                    else:
                        stats["skipped"] += 1
                    
                    # Update progress bar every record so it doesn't look stuck
                    pbar.set_description(f"Extracted: {stats['converted']}")

            if batch_data and not is_sampling:
                flush_batch(cat_out, batch_data, batch_idx)

    print(f"\n✅ SUCCESS: {stats['converted']} products saved.")

if __name__ == "__main__":
    main()