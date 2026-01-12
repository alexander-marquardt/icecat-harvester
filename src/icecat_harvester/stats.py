import os
import csv
import gzip
from tqdm import tqdm

# --- PATH CONFIGURATION ---
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(os.path.dirname(SCRIPT_DIR))

DATA_DIR = os.path.join(PROJECT_ROOT, "data")
XML_SOURCE_DIR = os.path.join(DATA_DIR, "xml_source")
TARGETS_FILE = os.path.join(PROJECT_ROOT, "targets.txt")
CATEGORIES_CSV = os.path.join(DATA_DIR, "categories.csv")

# CHANGED: Output to Markdown instead of CSV
OUTPUT_COUNTS_MD = os.path.join(DATA_DIR, "category_counts.md")

FILES_INDEX_XML = os.path.join(DATA_DIR, "files.index.xml")
FILES_INDEX_GZ = os.path.join(DATA_DIR, "files.index.xml.gz")

def load_targets():
    if not os.path.exists(TARGETS_FILE): return []
    targets = []
    with open(TARGETS_FILE, "r", encoding="utf-8") as f:
        for line in f:
            if line.strip() and not line.strip().startswith("#"):
                targets.append(line.strip())
    return targets

def load_category_map():
    cat_map = {} # ID -> Name
    if os.path.exists(CATEGORIES_CSV):
        with open(CATEGORIES_CSV, mode='r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                cat_map[row['ID']] = row['Name']
    return cat_map

def get_target_ids(cat_map, targets):
    target_ids = set()
    target_lower = set(t.lower() for t in targets)
    
    for cat_id, name in cat_map.items():
        if name.lower() in target_lower:
            target_ids.add(cat_id)
            
    for t in targets:
        if t.isdigit():
            target_ids.add(t)
            
    return target_ids

def main():
    print(f"--- Icecat Harvester Statistics ---")
    
    cat_map = load_category_map()
    targets = load_targets()
    target_ids = get_target_ids(cat_map, targets)
    
    # 1. Scan Index for ALL Counts
    index_file = FILES_INDEX_XML if os.path.exists(FILES_INDEX_XML) else FILES_INDEX_GZ
    if not os.path.exists(index_file):
        print(f"Error: Index file missing at {index_file}")
        return

    print(f"Scanning index ({os.path.basename(index_file)}) for global stats...")
    
    global_counts = {}
    opener = gzip.open if index_file.endswith('.gz') else open
    
    try:
        with opener(index_file, 'rb') as f:
            f_size = os.path.getsize(index_file)
            with tqdm(total=f_size, unit='B', unit_scale=True, desc="Scanning Index") as pbar:
                for line in f:
                    pbar.update(len(line))
                    if b'<file ' not in line: continue
                    
                    line_str = line.decode('utf-8', errors='ignore')
                    start = line_str.find('Catid="')
                    if start != -1:
                        end = line_str.find('"', start + 7)
                        cid = line_str[start+7:end]
                        global_counts[cid] = global_counts.get(cid, 0) + 1
                            
    except Exception as e:
        print(f"Error reading index: {e}")

    # 2. Count Local Files
    local_counts = {}
    if os.path.exists(XML_SOURCE_DIR):
        for cid, name in cat_map.items():
            safe_name = name.replace(" ", "_").replace("/", "-").replace("&", "and")
            folder_path = os.path.join(XML_SOURCE_DIR, safe_name)
            if os.path.exists(folder_path):
                local_counts[cid] = len([n for n in os.listdir(folder_path) if n.endswith('.xml')])

    # 3. Output to Markdown
    print(f"\nSaving full breakdown to {OUTPUT_COUNTS_MD}...")
    sorted_stats = sorted(global_counts.items(), key=lambda x: x[1], reverse=True)
    
    with open(OUTPUT_COUNTS_MD, 'w', encoding='utf-8') as f:
        # Markdown Header
        f.write("# Icecat Category Statistics\n\n")
        f.write(f"**Total Categories:** {len(sorted_stats)}\n\n")
        f.write("| Category ID | Name | Total Available (Index) | Downloaded (Local) |\n")
        f.write("|---|---|---|---|\n")
        
        # Markdown Rows
        for cid, count in sorted_stats:
            name = cat_map.get(cid, f"Unknown ID {cid}")
            local = local_counts.get(cid, 0)
            
            # Simple bolding for targets to make them pop in the file
            if cid in target_ids:
                name = f"**{name}**"
            
            f.write(f"| {cid} | {name} | {count} | {local} |\n")

    # 4. Print UI Report
    
    # A) Your Targets
    print(f"\n{'--- YOUR TARGETS ---':<50}")
    print(f"{'CATEGORY':<30} | {'LOCAL':<10} | {'TOTAL':<10} | {'COVERAGE'}")
    print("-" * 70)
    
    for cid in target_ids:
        name = cat_map.get(cid, "Unknown")
        total = global_counts.get(cid, 0)
        local = local_counts.get(cid, 0)
        pct = (local / total * 100) if total > 0 else 0
        print(f"{name[:30]:<30} | {local:<10} | {total:<10} | {pct:.1f}%")

    # B) Top 20 Largest Categories (General)
    print(f"\n{'--- TOP 20 LARGEST CATEGORIES (Global) ---':<50}")
    print(f"{'CATEGORY':<30} | {'TOTAL FILES':<10} | {'IN TARGETS?'}")
    print("-" * 70)
    
    for i, (cid, count) in enumerate(sorted_stats[:20]):
        name = cat_map.get(cid, f"Unknown ID {cid}")
        is_target = "YES" if cid in target_ids else "-"
        print(f"{name[:30]:<30} | {count:<10} | {is_target}")

    print("-" * 70)
    print(f"Full stats saved to: {OUTPUT_COUNTS_MD}")

if __name__ == "__main__":
    main()