import gzip
import xml.etree.ElementTree as ET
from collections import Counter
import csv
import os

# Files
INDEX_FILE = "files.index.xml.gz"
MAP_FILE = "categories.csv"

def load_category_map():
    cat_map = {}
    if not os.path.exists(MAP_FILE):
        print(f"Warning: {MAP_FILE} not found. Names will be unknown.")
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
        print(f"Error: {INDEX_FILE} missing. Run the harvester first to download the index.")
        return

    print("\n--- TOP 30 CATEGORIES BY VOLUME ---")
    print(f"{'ID':<10} | {'Count':<10} | {'Name'}")
    print("-" * 50)
    
    for cat_id, count in counts.most_common(30):
        # Look up name in our CSV map
        name = cat_map.get(cat_id, "Unknown Category")
        print(f"{cat_id:<10} | {count:<10} | {name}")

if __name__ == "__main__":
    scan_categories()