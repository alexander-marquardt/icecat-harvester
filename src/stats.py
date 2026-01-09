import gzip
import xml.etree.ElementTree as ET
from collections import Counter
import csv
import os

# --- PATH CONFIGURATION ---
# Calculate paths relative to this script file
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(SCRIPT_DIR)
DATA_DIR = os.path.join(PROJECT_ROOT, "data")

# Files
INDEX_FILE = os.path.join(DATA_DIR, "files.index.xml.gz")
MAP_FILE = os.path.join(DATA_DIR, "categories.csv")
OUTPUT_FILE = os.path.join(DATA_DIR, "category_counts.csv")

def load_category_map():
    cat_map = {}
    if not os.path.exists(MAP_FILE):
        print(f"Warning: {MAP_FILE} not found. Names will be unknown.")
        print("Run 'uv run src/get_category_names.py' first.")
        return cat_map
        
    with open(MAP_FILE, mode='r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            cat_map[row['ID']] = row['Name']
    return cat_map

def scan_categories():
    # 1. Load the names
    cat_map = load_category_map()
    
    print(f"Scanning {INDEX_FILE} for product counts...")
    counts = Counter()
    
    try:
        with gzip.open(INDEX_FILE, 'rb') as f:
            context = ET.iterparse(f, events=('end',))
            for event, elem in context:
                if elem.tag == 'file':
                    cat_id = elem.get('Catid')
                    if cat_id:
                        counts[cat_id] += 1
                    elem.clear()
    except FileNotFoundError:
        print(f"Error: {INDEX_FILE} missing.")
        print("Run 'uv run src/downloader.py' at least once to download the index.")
        return

    # 2. Write ALL counts to CSV
    print(f"Writing complete list to {OUTPUT_FILE}...")
    try:
        with open(OUTPUT_FILE, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(['ID', 'Count', 'Name']) # Header
            
            # most_common() with no arguments returns ALL items, sorted by count descending
            for cat_id, count in counts.most_common():
                name = cat_map.get(cat_id, "Unknown Category")
                writer.writerow([cat_id, count, name])
                
        print("Success! File saved.")
        
    except Exception as e:
        print(f"Error writing to CSV: {e}")

    # 3. Print Top 20 to Console for quick check
    print("\n--- TOP 20 CATEGORIES BY VOLUME ---")
    print(f"{'ID':<10} | {'Count':<10} | {'Name'}")
    print("-" * 50)
    
    for cat_id, count in counts.most_common(20):
        name = cat_map.get(cat_id, "Unknown Category")
        print(f"{cat_id:<10} | {count:<10} | {name}")

if __name__ == "__main__":
    scan_categories()