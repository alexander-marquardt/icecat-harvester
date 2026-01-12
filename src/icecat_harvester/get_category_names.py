import requests
import gzip
import xml.etree.ElementTree as ET
import csv
import os
from requests.auth import HTTPBasicAuth
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from dotenv import load_dotenv

# --- PATH CONFIGURATION ---
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(os.path.dirname(SCRIPT_DIR))

DATA_DIR = os.path.join(PROJECT_ROOT, "data")
CATEGORIES_CSV = os.path.join(DATA_DIR, "categories.csv")

# UPDATED URL: Now inside /refs/ subdirectory
REFS_URL = "https://data.icecat.biz/export/freexml/refs/CategoriesList.xml.gz"

load_dotenv(os.path.join(PROJECT_ROOT, ".env"))
ICECAT_USER = os.getenv('ICECAT_USER')
ICECAT_PASS = os.getenv('ICECAT_PASS')

def create_session():
    """Creates a session that handles 429/50x errors automatically."""
    s = requests.Session()
    s.auth = HTTPBasicAuth(ICECAT_USER, ICECAT_PASS)
    
    retries = Retry(
        total=5, 
        backoff_factor=1, 
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"]
    )
    s.mount('https://', HTTPAdapter(max_retries=retries))
    return s

def main():
    if not ICECAT_USER:
        print("Error: Credentials missing in .env")
        return

    print(f"Downloading Category References from {REFS_URL}...")
    os.makedirs(DATA_DIR, exist_ok=True)

    session = create_session()

    try:
        response = session.get(REFS_URL, stream=True)
        response.raise_for_status()

        print("Processing stream...")
        with gzip.open(response.raw, 'rb') as f:
            context = ET.iterparse(f, events=('end',))
            
            categories = {}
            
            for _, elem in context:
                if elem.tag == 'Category':
                    cat_id = elem.get('ID')
                    # Look for English Name (langid='1')
                    name_elem = elem.find("./Name[@langid='1']")
                    
                    if name_elem is not None and cat_id:
                        # FIX: Get the 'Value' attribute, not the text
                        cat_name = name_elem.get('Value')
                        if cat_name:
                            categories[cat_id] = cat_name
                            
                    elem.clear()

        print(f"Found {len(categories)} categories. Saving to CSV...")
        with open(CATEGORIES_CSV, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow(['ID', 'Name'])
            for cid, name in categories.items():
                writer.writerow([cid, name])

        print(f"Done. Saved to {CATEGORIES_CSV}")

    except Exception as e:
        print(f"\nError: {e}")

if __name__ == "__main__":
    main()