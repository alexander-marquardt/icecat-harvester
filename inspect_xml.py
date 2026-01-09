import requests
import gzip
import xml.etree.ElementTree as ET
from requests.auth import HTTPBasicAuth
import os
from dotenv import load_dotenv

load_dotenv()

ICECAT_USER = os.getenv('ICECAT_USER')
ICECAT_PASS = os.getenv('ICECAT_PASS')
CATS_URL = "https://data.icecat.biz/export/freexml/refs/CategoriesList.xml.gz"

def get_auth():
    return HTTPBasicAuth(ICECAT_USER, ICECAT_PASS)

def inspect_445():
    print(f"Streaming XML to inspect ID 445...")
    
    response = requests.get(CATS_URL, auth=get_auth(), stream=True)
    
    # We read line by line (decoded) to find the raw text context
    # This is "brute force" text search, avoiding the XML parser logic that might be failing us
    with gzip.open(response.raw, 'rt', encoding='utf-8') as f:
        for i, line in enumerate(f):
            # Look for the exact ID string
            if 'ID="445"' in line:
                print(f"\n--- FOUND MATCH ON LINE {i} ---")
                print(line.strip())
                # Print the next few lines to see context (Name, etc.)
                for _ in range(5):
                    print(next(f).strip())
                print("-----------------------------")

if __name__ == "__main__":
    inspect_445()