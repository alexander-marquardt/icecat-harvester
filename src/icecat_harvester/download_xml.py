import requests
import gzip
import os
import csv
from concurrent.futures import ThreadPoolExecutor, as_completed
from requests.auth import HTTPBasicAuth
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from dotenv import load_dotenv
from tqdm import tqdm
import shutil

# --- PATH CONFIGURATION ---
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(os.path.dirname(SCRIPT_DIR))

DATA_DIR = os.path.join(PROJECT_ROOT, "data")
XML_SAVE_DIR = os.path.join(DATA_DIR, "xml_source")
TARGETS_FILE = os.path.join(PROJECT_ROOT, "targets.txt")
CATEGORIES_CSV = os.path.join(DATA_DIR, "categories.csv")
FILES_INDEX_GZ = os.path.join(DATA_DIR, "files.index.xml.gz")
FILES_INDEX_RAW = os.path.join(DATA_DIR, "files.index.xml")

load_dotenv(os.path.join(PROJECT_ROOT, ".env"))

ICECAT_USER = os.getenv('ICECAT_USER')
ICECAT_PASS = os.getenv('ICECAT_PASS')
FILES_INDEX_URL = "https://data.icecat.biz/export/freexml/EN/files.index.xml.gz"

# --- NETWORK CONFIGURATION ---
MAX_WORKERS = 16  # Increased workers since most requests will be fast 404s
TIMEOUT = 5       # Lower timeout to fail faster on bad links

def create_session():
    s = requests.Session()
    s.auth = HTTPBasicAuth(ICECAT_USER, ICECAT_PASS)
    retries = Retry(total=2, backoff_factor=0.5, status_forcelist=[500, 502, 503])
    s.mount('https://', HTTPAdapter(max_retries=retries))
    return s

def load_targets():
    if not os.path.exists(TARGETS_FILE): return []
    targets = []
    with open(TARGETS_FILE, "r", encoding="utf-8") as f:
        for line in f:
            if line.strip() and not line.strip().startswith("#"):
                targets.append(line.strip())
    return targets

def ensure_index_ready():
    os.makedirs(DATA_DIR, exist_ok=True)
    if not os.path.exists(FILES_INDEX_RAW) and not os.path.exists(FILES_INDEX_GZ):
        print(f"Downloading Index...")
        r = requests.get(FILES_INDEX_URL, auth=HTTPBasicAuth(ICECAT_USER, ICECAT_PASS), stream=True)
        total_size = int(r.headers.get('content-length', 0))
        with open(FILES_INDEX_GZ, 'wb') as f, tqdm(total=total_size, unit='B', unit_scale=True, desc="Downloading") as bar:
            for chunk in r.iter_content(chunk_size=8192):
                f.write(chunk)
                bar.update(len(chunk))

    if os.path.exists(FILES_INDEX_GZ) and not os.path.exists(FILES_INDEX_RAW):
        print("Unzipping index...")
        with gzip.open(FILES_INDEX_GZ, 'rb') as f_in:
            with open(FILES_INDEX_RAW, 'wb') as f_out:
                shutil.copyfileobj(f_in, f_out)
    return FILES_INDEX_RAW

def load_category_map():
    cat_map = {}
    if os.path.exists(CATEGORIES_CSV):
        with open(CATEGORIES_CSV, mode='r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                cat_map[row['ID']] = row['Name']
    return cat_map

def get_target_category_ids(cat_map, target_names):
    target_ids = []
    target_names_lower = [t.lower() for t in target_names]
    for cat_id, name in cat_map.items():
        for target in target_names_lower:
            if target in name.lower():
                target_ids.append(cat_id)
                break
    return list(set(target_ids))

def fast_extract_attribute(line, attr):
    key = f'{attr}="'
    start = line.find(key)
    if start == -1: return None
    start += len(key)
    end = line.find('"', start)
    if end == -1: return None
    return line[start:end]

def get_local_path(product_path, cat_name):
    filename = os.path.basename(product_path)
    safe_cat_name = cat_name.replace(" ", "_").replace("/", "-").replace("&", "and")
    return os.path.join(XML_SAVE_DIR, safe_cat_name, filename)

def download_file(session, url, local_path):
    """
    Returns: 
    1 = Downloaded
    0 = Failed (Network)
    -1 = Restricted (404/Access Denied)
    """
    if os.path.exists(local_path): return 1 # Already have it
    
    try:
        os.makedirs(os.path.dirname(local_path), exist_ok=True)
        resp = session.get(url, timeout=TIMEOUT)
        
        if resp.status_code == 200:
            with open(local_path, 'wb') as f:
                f.write(resp.content)
            return 1
        elif resp.status_code == 404:
            # Icecat returns 404 for restricted items in the free index
            return -1
        elif "restricted" in resp.text.lower():
            return -1
            
    except Exception:
        pass 
    return 0

def main():
    if not ICECAT_USER:
        print("Error: Credentials missing.")
        return

    targets = load_targets()
    if not targets: return

    index_file = ensure_index_ready()
    cat_map = load_category_map()
    target_ids = set(get_target_category_ids(cat_map, targets))
    
    if not target_ids:
        print("No matching categories found.")
        return

    # --- PHASE 1: SCAN ---
    print(f"\n--- Phase 1: Auditing Index ({len(target_ids)} categories) ---")
    missing_files = [] 
    
    file_size = os.path.getsize(index_file)
    with open(index_file, 'r', encoding='utf-8', errors='ignore') as f:
        with tqdm(total=file_size, unit='B', unit_scale=True, desc="Scanning Index") as pbar:
            accumulated_bytes = 0
            for line in f:
                accumulated_bytes += len(line)
                if accumulated_bytes > 5 * 1024 * 1024: 
                    pbar.update(accumulated_bytes)
                    accumulated_bytes = 0

                if "<file " not in line: continue
                
                cat_id = fast_extract_attribute(line, "Catid")
                if cat_id and cat_id in target_ids:
                    path = fast_extract_attribute(line, "path")
                    if path:
                        cat_name = cat_map.get(cat_id, "Unknown")
                        local_path = get_local_path(path, cat_name)
                        
                        if not os.path.exists(local_path):
                            full_url = f"https://data.icecat.biz/{path}"
                            missing_files.append((full_url, local_path))
            
            pbar.update(accumulated_bytes)

    if not missing_files:
        print("All files up to date!")
        return

    # --- PHASE 2: DOWNLOAD ---
    print(f"\n--- Phase 2: Attempting {len(missing_files)} remaining files ---")
    print(f"Note: High 'Restricted' count is normal for Open Icecat.\n")
    
    session = create_session()
    
    total_dl = 0
    total_restricted = 0
    
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = [executor.submit(download_file, session, url, path) for url, path in missing_files]
        
        # We use a custom bar format to show Restricted counts clearly
        with tqdm(total=len(futures), unit="file", bar_format="{l_bar}{bar}| {n_fmt}/{total_fmt} [{elapsed}, {rate_fmt} {postfix}]") as pbar:
            for future in as_completed(futures):
                result = future.result()
                if result == 1:
                    total_dl += 1
                elif result == -1:
                    total_restricted += 1
                
                # Update the postfix to show the stats live
                pbar.set_postfix(new=total_dl, restricted=total_restricted)
                pbar.update(1)

    print(f"\nSync Complete.")
    print(f"Downloaded: {total_dl}")
    print(f"Restricted (Skipped): {total_restricted}")

if __name__ == "__main__":
    main()