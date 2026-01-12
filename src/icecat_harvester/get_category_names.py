import requests
import gzip
import xml.etree.ElementTree as ET
from requests.auth import HTTPBasicAuth
import csv
import os
from dotenv import load_dotenv

# Calculate paths relative to this script file
# (This allows running from root OR from inside src/)
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(SCRIPT_DIR)
DATA_DIR = os.path.join(PROJECT_ROOT, "data")
OUTPUT_FILE = os.path.join(DATA_DIR, "categories.csv")

# Load .env from project root
load_dotenv(os.path.join(PROJECT_ROOT, ".env"))

# --- CONFIGURATION ---
ICECAT_USER = os.getenv('ICECAT_USER')
ICECAT_PASS = os.getenv('ICECAT_PASS')
CATS_URL = "https://data.icecat.biz/export/freexml/refs/CategoriesList.xml.gz"

def get_auth():
    return HTTPBasicAuth(ICECAT_USER, ICECAT_PASS)

def fetch_and_save_categories():
    # Ensure data directory exists
    os.makedirs(DATA_DIR, exist_ok=True)
    
    print(f"Downloading Categories List to {OUTPUT_FILE}...")
    
    try:
        response = requests.get(CATS_URL, auth=get_auth(), stream=True)
        if response.status_code != 200:
            print(f"Error: Failed to download (Status {response.status_code})")
            return

        with gzip.open(response.raw, 'rb') as f, open(OUTPUT_FILE, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(['ID', 'Name']) # Header
            
            context = ET.iterparse(f, events=('start', 'end'))
            count = 0
            
            for event, elem in context:
                if event == 'end':
                    # CRITICAL FIX: Explicitly check tag name is EXACTLY 'Category'
                    # (This ignores 'VirtualCategory', 'CloudCategory', etc.)
                    if elem.tag == 'Category' or elem.tag.endswith('}Category'):
                        cat_id = elem.get('ID')
                        name = "Unknown"
                        
                        # Find English Name
                        for child in elem:
                            if child.tag.endswith('Name'):
                                if child.get('ID') == '1': # English
                                    name = child.get('Value')
                                    break
                                if name == "Unknown":
                                    name = child.get('Value')

                        # Fallback for older XML attributes
                        if name == "Unknown":
                            name = elem.get('Name')

                        if cat_id and name and name != "Unknown":
                            writer.writerow([cat_id, name])
                            count += 1
                        
                        elem.clear()
                    
                    # Explicitly clear VirtualCategory to free memory
                    elif 'VirtualCategory' in elem.tag:
                        elem.clear()
            
            print(f"Success! Saved {count} REAL categories to {OUTPUT_FILE}")

    except Exception as e:
        print(f"Error processing XML: {e}")

if __name__ == "__main__":
    fetch_and_save_categories()