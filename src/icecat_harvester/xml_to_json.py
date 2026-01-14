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
            for row in reader: f_map[row['ID']] = row['Name']
    return f_map

def load_price_map():
    p_map = {}
    if os.path.exists(PRICES_NDJSON):
        try:
            with open(PRICES_NDJSON, 'r', encoding='utf-8') as f:
                for line in f:
                    if not line.strip(): continue
                    data = json.loads(line)
                    if "name" in data and "price" in data: p_map[data["name"].lower()] = float(data["price"])
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
    
    # Brand Premium/Discount Logic
    multiplier = 1.0
    if brand_name:
        b = brand_name.lower()
        if b in ["apple", "samsung", "sony", "hp", "dell", "lenovo", "bose", "cisco"]:
            multiplier = 1.3 
        elif b in ["trust", "hama", "generic", "startech", "sweex"]:
            multiplier = 0.8 
    
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
        if product is None: 
            if root.tag.endswith("Product"): product = root
            else: return None

        # Map Groups for Description synthesis
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

            feat_node = feature.find(".//Feature/Name")
            feat_name = feat_node.get("Value") if feat_node is not None else feature_map.get(feature.get("Local_ID"), f"Feature_{feature.get('Local_ID')}")
            
            safe_name = feat_name.replace("|", "/").replace(".", "")
            attrs[safe_name] = raw_value.replace("|", "/")

            group_id = feature.get("CategoryFeatureGroup_ID")
            group_info = group_map.get(group_id, {"name": "General", "order": 9999})
            g_name = group_info['name']
            if g_name not in grouped_specs:
                grouped_specs[g_name] = {"order": group_info['order'], "items": []}
            grouped_specs[g_name]["items"].append(f"{feat_name}: {raw_value}")

        title = product.get("Title") or ""
        brand = (product.find(".//Supplier").get("Name") if product.find(".//Supplier") is not None else "")
        
        # Synthesize Markdown Description
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
                if items_str: desc_parts.append(f"- **{g_name}**: {items_str}")

        item = {
            "id": product.get("ID"),
            "title": title,
            "brand": brand,
            "description": "\n".join(desc_parts),
            "image_url": None,
            "price": 0.0, 
            "currency": "USD",
            "categories": [],
            "attrs": attrs,                
            "attr_keys": sorted(list(attrs.keys()))
        }
        
        cat_node = product.find(".//Category/Name")
        cat_val = cat_node.get("Value") if cat_node is not None else None
        if cat_val: item["categories"].append(cat_val)
        item["price"] = estimate_price(item["id"], cat_val, brand, price_map)

        # High-Quality Image Filtering
        priorities = ["Pic500x500", "Pic", "Original", "HighPic"]
        for pic in product.findall(".//ProductPicture"):
            for attr in priorities:
                url = pic.get(attr)
                if url and "http" in url:
                    item["image_url"] = url
                    break
            if item["image_url"]: break

        return item
    except Exception: return None

def flush_batch(cat_json_dir, batch_data, batch_idx):
    if not batch_data: return
    filepath = os.path.join(cat_json_dir, f"batch_{batch_idx:03d}.ndjson")
    with open(filepath, "w", encoding="utf-8") as f:
        for item in batch_data: f.write(json.dumps(item, ensure_ascii=False) + "\n")

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

    is_sampling = args.generate_sample_data > 0
    out_root = SAMPLE_DATA_DIR if is_sampling else os.path.join(JSON_OUTPUT_DIR, args.output_subdir or datetime.now().strftime("%Y%m%d-%H%M%S"))

    if os.path.exists(out_root) and not args.yes:
        if input(f"⚠️ Overwrite {out_root}? [y/N]: ").lower() != 'y': return
        shutil.rmtree(out_root)
    os.makedirs(out_root, exist_ok=True)

    categories = sorted([d for d in os.listdir(XML_SOURCE_DIR) if os.path.isdir(os.path.join(XML_SOURCE_DIR, d))])
    
    # PHASE 1: Document Count for Accurate Progress Bar
    print("🔍 Pre-calculating total job size...")
    total_docs = 0
    cat_files = {}
    for cat in categories:
        names = [f for f in os.listdir(os.path.join(XML_SOURCE_DIR, cat)) if f.endswith(".xml")]
        limit = args.generate_sample_data if is_sampling else args.max_input_files
        count = min(len(names), limit) if limit > 0 else len(names)
        cat_files[cat] = names
        total_docs += count
        if args.max_output_records and total_docs >= args.max_output_records:
            total_docs = args.max_output_records
            break

    # PHASE 2: Process with Throttled Feedback
    stats = {"converted": 0, "skipped": 0}
    total_processed = 0

    

    with tqdm(total=total_docs, unit="doc", desc="Total Progress") as pbar:
        for cat in categories:
            if args.max_output_records and total_processed >= args.max_output_records: break
            
            names = cat_files.get(cat, [])
            if not names: continue
            
            # Always shuffle for Samples; shuffle for Production only if a limit is applied
            if is_sampling or args.max_input_files > 0:
                random.shuffle(names)
                limit = args.generate_sample_data if is_sampling else args.max_input_files
                names = names[:limit]

            cat_out = os.path.join(out_root, cat) if not is_sampling else out_root
            os.makedirs(cat_out, exist_ok=True)
            batch_data, batch_idx = [], 1

            for xml_file in names:
                if args.max_output_records and total_processed >= args.max_output_records: break
                
                item = parse_icecat_xml(os.path.join(XML_SOURCE_DIR, cat, xml_file), feature_map, price_map)
                
                # QUALITY GUARD: Only proceed if item has a valid title and image
                if item and item.get("image_url") and item.get("title"):
                    if is_sampling:
                        # Write directly to the category's sample file
                        with open(os.path.join(out_root, f"{cat}.ndjson"), "a") as sf:
                            sf.write(json.dumps(item, ensure_ascii=False) + "\n")
                    else:
                        batch_data.append(item)
                    
                    total_processed += 1
                    stats["converted"] += 1
                    
                    if not is_sampling and len(batch_data) >= BATCH_SIZE:
                        flush_batch(cat_out, batch_data, batch_idx)
                        batch_data, batch_idx = [], batch_idx + 1
                else:
                    stats["skipped"] += 1
                    # We still increment processed count for the bar so it doesn't lag
                    total_processed += 1
                
                # THROTTLED REFRESH: Update every 100 docs to keep CPU focused on parsing
                if total_processed % 100 == 0:
                    pbar.n = total_processed
                    pbar.set_postfix(cat=cat[:10], skip=stats["skipped"])
                    pbar.refresh()

            if batch_data and not is_sampling: flush_batch(cat_out, batch_data, batch_idx)
            pbar.n = total_processed
            pbar.refresh()

    print(f"\n✅ Done! Total Converted: {stats['converted']} | Skipped (Low Quality): {stats['skipped']}")

if __name__ == "__main__":
    main()