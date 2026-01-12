import os
import shutil

# --- CONFIGURATION ---
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(os.path.dirname(SCRIPT_DIR))
XML_SOURCE_DIR = os.path.join(PROJECT_ROOT, "data", "xml_source")
TARGETS_FILE = os.path.join(PROJECT_ROOT, "targets.txt")

def get_safe_foldername(cat_name):
    """Must match the logic in download_xml.py exactly."""
    return cat_name.strip().replace(" ", "_").replace("/", "-").replace("&", "and")

def main():
    if not os.path.exists(TARGETS_FILE):
        print("Error: targets.txt not found.")
        return

    # 1. Load valid folder names from targets.txt
    valid_folders = set()
    with open(TARGETS_FILE, 'r') as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith("#"):
                valid_folders.add(get_safe_foldername(line))

    print(f"--- Allowed Folders ({len(valid_folders)}) ---")
    # print(sorted(list(valid_folders))) 
    
    # 2. Check existing folders
    if not os.path.exists(XML_SOURCE_DIR):
        print(f"No data directory found at {XML_SOURCE_DIR}")
        return

    existing_folders = [f for f in os.listdir(XML_SOURCE_DIR) if os.path.isdir(os.path.join(XML_SOURCE_DIR, f))]
    
    to_delete = []
    for folder in existing_folders:
        if folder not in valid_folders:
            to_delete.append(folder)

    # 3. Report
    if not to_delete:
        print("\n✅ Clean! No extra folders found.")
        return

    print(f"\n🚫 Found {len(to_delete)} folders to delete (Not in targets.txt):")
    for folder in to_delete:
        print(f"   - {folder}")

    # 4. Confirmation
    confirm = input("\nType 'yes' to permanently delete these folders: ").strip().lower()
    
    if confirm == 'yes':
        for folder in to_delete:
            full_path = os.path.join(XML_SOURCE_DIR, folder)
            shutil.rmtree(full_path)
            print(f"Deleted: {folder}")
        print("\nCleanup Complete.")
    else:
        print("\nAction cancelled. No files were deleted.")

if __name__ == "__main__":
    main()