import requests
import gzip
import xml.etree.ElementTree as ET
from requests.auth import HTTPBasicAuth
import os
import csv 
from dotenv import load_dotenv

# Run with: uv run --with requests --with dotenv ./get_category_names.py    

# Load variables from .env file
load_dotenv()

# --- CONFIGURATION ---
# Now we read from the environment, with a fallback for safety
ICECAT_USER = os.getenv('ICECAT_USER')
ICECAT_PASS = os.getenv('ICECAT_PASS')

CATS_URL = "https://data.icecat.biz/export/freexml/refs/CategoriesList.xml.gz"
OUTPUT_FILE = "categories.csv"

def get_auth():
    return HTTPBasicAuth(ICECAT_USER, ICECAT_PASS)

def fetch_and_save_categories():
    print(f"Downloading Categories List to {OUTPUT_FILE}...")
    
    try:
        response = requests.get(CATS_URL, auth=get_auth(), stream=True)
        if response.status_code != 200:
            print(f"Error: Failed to download (Status {response.status_code})")
            return

        with gzip.open(response.raw, 'rb') as f, open(OUTPUT_FILE, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(['ID', 'Name']) # Header
            
            context = ET.iterparse(f, events=('end',))
            count = 0
            
            for event, elem in context:
                if elem.tag.endswith('Category'):
                    cat_id = elem.get('ID')
                    name = "Unknown"
                    
                    # Find English Name (ID='1')
                    for child in elem:
                        if child.tag.endswith('Name'):
                            if child.get('ID') == '1':
                                name = child.get('Value')
                                break
                            if name == "Unknown":
                                name = child.get('Value')

                    if cat_id and name and name != "Unknown":
                        writer.writerow([cat_id, name])
                        count += 1
                    
                    elem.clear()
            
            print(f"Success! Saved {count} categories to {OUTPUT_FILE}")

    except Exception as e:
        print(f"Error processing XML: {e}")

if __name__ == "__main__":
    fetch_and_save_categories()