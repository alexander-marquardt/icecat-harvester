import csv
import os

# --- CONFIG ---
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(os.path.dirname(SCRIPT_DIR))
DATA_DIR = os.path.join(PROJECT_ROOT, "data")
CATEGORIES_CSV = os.path.join(DATA_DIR, "categories.csv")
OUTPUT_PRICES_CSV = os.path.join(DATA_DIR, "price_baselines.csv")

# Heuristics: (Keyword, Estimated_USD)
# Order matters: more specific matches should be higher
RULES = [
    ("Server", 1500),
    ("Workstation", 1200),
    ("Laptop", 800),
    ("Notebook", 800),
    ("Smartphone", 600),
    ("Tablet", 400),
    ("Television", 600),
    ("TV", 500),
    ("Monitor", 250),
    ("Camera", 400),
    ("Printer", 200),
    ("Washer", 400),
    ("Fridge", 500),
    ("Processor", 250),
    ("Motherboard", 150),
    ("Graphics Card", 400),
    ("Memory", 80),
    ("RAM", 80),
    ("HDD", 100),
    ("SSD", 120),
    ("Switch", 150), # Network switch
    ("Router", 80),
    ("Headphones", 70),
    ("Speaker", 100),
    ("Keyboard", 40),
    ("Mouse", 30),
    ("Cable", 15),
    ("Adapter", 20),
    ("Case", 25),
    ("Bag", 30),
    ("Battery", 40),
    ("Cartridge", 60),
    ("Toner", 90),
    ("Paper", 10),
    ("Software", 120),
    ("License", 100),
    ("Warranty", 50)
]

def guess_price(cat_name):
    name_lower = cat_name.lower()
    for keyword, price in RULES:
        if keyword.lower() in name_lower:
            return price
    return 50 # Safe Default for "Unknown"

def main():
    if not os.path.exists(CATEGORIES_CSV):
        print(f"Error: {CATEGORIES_CSV} not found.")
        print("Run 'uv run -m icecat_harvester.get_category_names' first!")
        return

    print("Generating price baselines based on category names...")
    
    entries = []
    matched = 0
    total = 0
    
    with open(CATEGORIES_CSV, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            cid = row['ID']
            name = row['Name']
            price = guess_price(name)
            
            # Track if we used a rule or the default
            is_match = price != 50
            if is_match: matched += 1
            total += 1
            
            entries.append([cid, name, price])

    # Save to new CSV
    with open(OUTPUT_PRICES_CSV, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(['Category_ID', 'Category_Name', 'Baseline_Price'])
        writer.writerows(entries)

    print(f"Done. Processed {total} categories.")
    print(f"Matched {matched} ({matched/total:.1%}) using keywords.")
    print(f"Saved to {OUTPUT_PRICES_CSV}")
    print("Tip: You can now verify/edit this file manually in Excel.")

if __name__ == "__main__":
    main()