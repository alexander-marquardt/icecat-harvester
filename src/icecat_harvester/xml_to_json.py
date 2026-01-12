import os
import json
import xml.etree.ElementTree as ET
from tqdm import tqdm

# --- PATH CONFIGURATION ---
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(SCRIPT_DIR)
DATA_DIR = os.path.join(PROJECT_ROOT, "data")
INPUT_XML_DIR = os.path.join(DATA_DIR, "xml_source")
OUTPUT_JSON_DIR = os.path.join(DATA_DIR, "products")

# --- PARSING LOGIC ---
def parse_xml_to_json(xml_content, cat_name):
    try:
        root = ET.fromstring(xml_content)
        product = root.find("Product")
        if product is None: return None

        item = {
            "id": product.get("ID"),
            "title": product.get("Name") or product.get("Title"),
            "brand": product.find("Supplier").get("Name") if product.find("Supplier") is not None else "Unknown",
            "category": cat_name,
            "images": [],
            "specs": {}
        }

        # Description
        desc = product.find(".//ProductDescription")
        item["description"] = desc.get("LongDesc") if desc is not None else ""

        # Images
        image_list = []
        if product.get("HighPic"): image_list.append(product.get("HighPic"))
        for pic in product.findall(".//ProductGallery/ProductPicture"):
            if pic.get("Pic"): image_list.append(pic.get("Pic"))
        
        # Deduplicate and Secure Images
        clean_images = []
        for img in image_list:
            secure = img.replace("http://", "https://")
            if secure not in clean_images: clean_images.append(secure)
        item["images"] = clean_images
        item["image_url"] = clean_images[0] if clean_images else None

        # Specs
        for feature in product.findall(".//ProductFeature"):
            try:
                val = feature.get("Presentation_Value")
                local = feature.find(".//LocalFeature")
                name = local.find("Feature").get("Value") if local and local.find("Feature") is not None else None
                if not name: name = f"Feature_{feature.get('CategoryFeature_ID')}"
                if name and val: item["specs"][name] = val
            except: continue

        return item
    except:
        return None

def main():
    if not os.path.exists(INPUT_XML_DIR):
        print(f"Error: No XML source found at {INPUT_XML_DIR}")
        print("Run 'uv run src/download_xml.py' first.")
        return

    # Reset output directory
    print(f"Generating JSON files in {OUTPUT_JSON_DIR}...")
    os.makedirs(OUTPUT_JSON_DIR, exist_ok=True)
    
    # Optional: Clear old files to ensure clean state
    # for f in os.listdir(OUTPUT_JSON_DIR):
    #     if f.endswith(".ndjson"):
    #         os.remove(os.path.join(OUTPUT_JSON_DIR, f))

    # 1. Gather all files
    all_files = []
    print("Scanning XML directory...")
    for root, dirs, files in os.walk(INPUT_XML_DIR):
        for file in files:
            if file.endswith(".xml"):
                cat_name = os.path.basename(root)
                all_files.append((os.path.join(root, file), cat_name))

    print(f"Found {len(all_files)} XML files. Starting conversion...")

    # 2. Parse with Progress Bar
    for path, cat_name in tqdm(all_files, unit="file"):
        try:
            with open(path, 'rb') as f:
                content = f.read()
            
            data = parse_xml_to_json(content, cat_name)
            if data:
                # Append to Category.ndjson
                safe_name = cat_name.replace(" ", "_").replace("/", "-").replace("&", "and")
                out_file = os.path.join(OUTPUT_JSON_DIR, f"{safe_name}.ndjson")
                with open(out_file, 'a', encoding='utf-8') as f:
                    f.write(json.dumps(data) + "\n")
        except:
            pass

    print("\nDone!")

if __name__ == "__main__":
    main()