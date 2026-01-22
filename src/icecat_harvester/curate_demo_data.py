import os
import json
import csv
import argparse
import random
from tqdm import tqdm
from collections import defaultdict


def load_target_categories(target_file):
    """Reads the names of categories from targets.txt."""
    if not os.path.exists(target_file):
        print(f"❌ Error: Target file not found at {target_file}")
        return []
    with open(target_file, 'r', encoding='utf-8') as f:
        return [line.strip() for line in f if line.strip()]


def load_category_mapping(mapping_file, target_names):
    """Maps names in targets.txt to numeric IDs using categories.csv."""
    mapping = {}
    if not os.path.exists(mapping_file):
        print(f"❌ Error: Category mapping file not found at {mapping_file}")
        return mapping

    targets_lower = [t.lower() for t in target_names]
    with open(mapping_file, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            if row.get('Name') and row['Name'].lower() in targets_lower:
                mapping[row['ID']] = row['Name']
    return mapping


def get_matching_keyword(item, keywords):
    """Returns the first keyword that matches the item content, or None."""
    content = f"{item.get('title', '')} {item.get('brand', '')} {item.get('description', '')}".lower()
    for kw in keywords:
        if kw.lower() in content:
            return kw.lower()
    return None


def main():
    parser = argparse.ArgumentParser(description="Curate a balanced sample for the ecommerce demo.")
    parser.add_argument("--input-path", type=str, required=True, help="Path to the dataset folder.")
    parser.add_argument("--limit", type=int, default=15, help="Total items to pull per category.")
    parser.add_argument("--keywords", type=str, default="iphone,samsung,nokia,macbook", help="Keywords to balance.")
    parser.add_argument("--output", type=str, default="data/demo_catalog.ndjson", help="Output file path.")

    args = parser.parse_args()
    keywords = [k.strip().lower() for k in args.keywords.split(",")] if args.keywords else []

    # --- Path Resolution (2 levels up from src/icecat_harvester) ---
    script_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.abspath(os.path.join(script_dir, "..", ".."))

    input_dir = os.path.abspath(args.input_path)
    targets_path = os.path.join(project_root, "targets.txt")
    mapping_path = os.path.join(project_root, "data", "categories.csv")
    output_path = os.path.join(project_root, args.output)

    target_names = load_target_categories(targets_path)
    cat_map = load_category_mapping(mapping_path, target_names)

    if not cat_map:
        print("Stopping: Category map could not be built.")
        return

    # Map target names (normalized) to actual folder names on disk
    existing_folders = {d.lower().replace('_', ' ').replace('-', ' '): d
                        for d in os.listdir(input_dir) if os.path.isdir(os.path.join(input_dir, d))}

    print(f"✅ Targets loaded. Searching for balanced keywords: {', '.join(keywords)}")
    final_items = []

    for cat_id, cat_name in tqdm(cat_map.items(), desc="Curating"):
        # Folder matching logic
        folder_name = None
        if os.path.exists(os.path.join(input_dir, cat_id)):
            folder_name = cat_id
        elif cat_name.lower().replace('_', ' ').replace('-', ' ') in existing_folders:
            folder_name = existing_folders[cat_name.lower().replace('_', ' ').replace('-', ' ')]

        if not folder_name:
            continue

        cat_folder = os.path.join(input_dir, folder_name)

        # Bucket items by which keyword they match
        keyword_buckets = defaultdict(list)
        generic_items = []

        batch_files = [f for f in os.listdir(cat_folder) if f.endswith(".ndjson")]
        for batch in batch_files:
            with open(os.path.join(cat_folder, batch), 'r', encoding='utf-8') as f:
                for line in f:
                    try:
                        item = json.loads(line)
                        item['category_label'] = cat_name

                        matched_kw = get_matching_keyword(item, keywords)
                        if matched_kw:
                            keyword_buckets[matched_kw].append(item)
                        else:
                            generic_items.append(item)
                    except:
                        continue

        if not generic_items and not keyword_buckets:
            continue

        # --- Balanced Sampling Logic ---
        category_sample = []
        per_kw_limit = args.limit // len(keywords) if keywords else args.limit

        # 1. Take a fair share from each keyword bucket
        for kw in keywords:
            items = keyword_buckets[kw]
            random.shuffle(items)
            category_sample.extend(items[:per_kw_limit])

        # 2. If we haven't reached the limit, fill from the remaining keyword pool
        if len(category_sample) < args.limit:
            remaining_kw_pool = []
            for kw in keywords:
                # Add what wasn't already picked
                remaining_kw_pool.extend(keyword_buckets[kw][per_kw_limit:])
            random.shuffle(remaining_kw_pool)

            needed = args.limit - len(category_sample)
            category_sample.extend(remaining_kw_pool[:needed])

        # 3. If still below limit, fill from generic items
        if len(category_sample) < args.limit:
            random.shuffle(generic_items)
            needed = args.limit - len(category_sample)
            category_sample.extend(generic_items[:needed])

        final_items.extend(category_sample[:args.limit])

    if final_items:
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        with open(output_path, 'w', encoding='utf-8') as f_out:
            for item in final_items:
                f_out.write(json.dumps(item, ensure_ascii=False) + "\n")
        print(f"\n🚀 Balanced demo catalog generated with {len(final_items)} items.")
        print(f"📍 Location: {output_path}")
    else:
        print("\n⚠️ Export empty. Check folder names and target categories.")


if __name__ == "__main__":
    main()