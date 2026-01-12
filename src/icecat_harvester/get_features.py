import requests
import gzip
import xml.etree.ElementTree as ET
import csv
import os
from requests.auth import HTTPBasicAuth
from dotenv import load_dotenv

# --- CONFIG ---
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(os.path.dirname(SCRIPT_DIR))
DATA_DIR = os.path.join(PROJECT_ROOT, "data")
LOCAL_GZ_PATH = os.path.join(DATA_DIR, "FeaturesList.xml.gz")
FEATURES_CSV = os.path.join(DATA_DIR, "features.csv")
FEATURES_URL = "https://data.icecat.biz/export/freexml/refs/FeaturesList.xml.gz"

load_dotenv(os.path.join(PROJECT_ROOT, ".env"))
ICECAT_USER = os.getenv('ICECAT_USER')
ICECAT_PASS = os.getenv('ICECAT_PASS')

def download_if_missing():
    if os.path.exists(LOCAL_GZ_PATH) and os.path.getsize(LOCAL_GZ_PATH) > 0:
        print(f"Using existing file: {LOCAL_GZ_PATH}")
        return

    if not ICECAT_USER:
        print("Error: Credentials missing.")
        return

    print(f"Downloading Features List to {LOCAL_GZ_PATH}...")
    os.makedirs(DATA_DIR, exist_ok=True)

    session = requests.Session()
    session.auth = HTTPBasicAuth(ICECAT_USER, ICECAT_PASS)
    
    with session.get(FEATURES_URL, stream=True) as r:
        r.raise_for_status()
        with open(LOCAL_GZ_PATH, 'wb') as f:
            for chunk in r.iter_content(chunk_size=8192):
                f.write(chunk)
    print("Download complete.")

def parse_features():
    print("Parsing XML (Strict Mode: Names Only)...")
    features = {}
    
    try:
        with gzip.open(LOCAL_GZ_PATH, 'rb') as f:
            context = ET.iterparse(f, events=('end',))
            
            for event, elem in context:
                if elem.tag.endswith('Feature'):
                    fid = elem.get('ID')
                    name_val = None
                    
                    # STRICT SEARCH: Only look for "Name" tags, ignore "Description"
                    # We look for a Name tag with langid="1" anywhere inside this Feature
                    # Note: We check specifically for the tag ending in 'Name'
                    
                    for child in elem.iter():
                        if child.tag.endswith("Name") and child.get("langid") == "1":
                            # Prefer 'Value' attribute (standard for labels)
                            if child.get("Value"):
                                name_val = child.get("Value")
                            # Fallback to text
                            elif child.text:
                                name_val = child.text
                            
                            # Once we find the English Name, stop looking in this Feature
                            if name_val: break
                    
                    if fid and name_val:
                        features[fid] = name_val
                    
                    elem.clear()

    except Exception as e:
        print(f"Error during parsing: {e}")
        return

    print(f"Found {len(features)} feature labels.")
    
    if len(features) > 0:
        print(f"Saving to {FEATURES_CSV}...")
        with open(FEATURES_CSV, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow(['ID', 'Name'])
            for fid, name in features.items():
                writer.writerow([fid, name])
        print("Done.")
        
        # Verify the first few for the user
        print("\n--- Preview of first 5 labels ---")
        preview_count = 0
        for fid, name in features.items():
            print(f"ID {fid}: {name}")
            preview_count += 1
            if preview_count >= 5: break

    else:
        print("⚠️ Found 0 features.")

if __name__ == "__main__":
    download_if_missing()
    parse_features()