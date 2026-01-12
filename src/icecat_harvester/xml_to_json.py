import os
import json
import csv
import shutil
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
            
            # Find Group Name (nested)
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

            # --- KEY FIX: Name Resolution Strategy ---
            feat_name = None
            
            # Strategy A: Check internal <Name> tag (Prioritize this for Feature_0 fix)
            feat_node = feature.find(".//Feature")
            if feat_node is not None:
                name_node = feat_node.find(".//Name")
                if name_node is not None:
                    feat_name = name_node.get("Value")
            
            # Strategy B: If internal name missing, use ID Lookup
            if not feat_name:
                feat_id = feature.get("Local_ID")
                feat_name = feature_map.get(feat_id)
            
            # Strategy C: Fallback
            if not feat_name:
                feat_id = feature.get("Local_ID")
                feat_name = f"Feature_{feat_id}"

            # --- Elastic Safe Strings ---
            safe_name = feat_name.replace("|", "/")
            safe_val = raw_value.replace("|", "/")
            
            # Deduplication Check
            facet_str = f"{safe_name}|{safe_val}"
            if facet_str not in specs_facets:
                specs_facets.append(facet_str)
            
            if safe_name not in specs_names:
                specs_names.append(safe_name)

            # --- Description Grouping ---
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
                # Only add group if it has items
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
            "specs_facets": specs_facets,
            "specs_names": specs_names,
            "categories": []
        }
        
        cat_node = product.find(".//Category")
        if cat_node is not None:
             cat_name = cat_node.find(".//Name")
             if cat_name is not None:
                 item["categories"].append(cat_name.get("Value"))

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
    parser = argparse.ArgumentParser()
    parser.add_argument("--limit", type=int, default=0)
    parser.add_argument("--sample", action="store_true")
    parser.add_argument("--yes", action="store_true")
    args = parser.parse_args()

    if os.path.exists(JSON_OUTPUT_DIR):
        if not args.yes:
            confirm = input(f"⚠️  Overwrite {JSON_OUTPUT_DIR}? [y/N]: ").lower().strip()
            if confirm != 'y': return
        shutil.rmtree(JSON_OUTPUT_DIR)
    
    os.makedirs(JSON_OUTPUT_DIR, exist_ok=True)
    print(f"Output Directory: {JSON_OUTPUT_DIR}")

    feature_map = load_feature_map()
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

            if batch_data:
                flush_batch(cat_json_dir, batch_data, batch_index)

    print(f"\n✅ Converted: {stats['converted']}")

if __name__ == "__main__":
    main()