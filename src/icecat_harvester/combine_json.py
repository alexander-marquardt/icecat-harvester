import os
import shutil
import argparse
from tqdm import tqdm

def main():
    parser = argparse.ArgumentParser(
        description="Combine batch NDJSON files from a subdirectory in 'data/products' into single-file-per-category NDJSONs."
    )
    parser.add_argument(
        "input_subdir",
        type=str,
        help="The name of the subdirectory in 'data/products' to process.",
    )
    parser.add_argument(
        "--output-dir",
        type=str,
        default="data/products_combined",
        help="The directory to write the combined NDJSON files to. Defaults to 'data/products_combined'.",
    )
    parser.add_argument(
        "--yes",
        action="store_true",
        help="Automatically confirm overwriting the output directory.",
    )
    args = parser.parse_args()

    # --- Path Setup ---
    project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    input_dir = os.path.join(project_root, "data", "products", args.input_subdir)
    output_dir = os.path.join(project_root, args.output_dir)

    if not os.path.isdir(input_dir):
        print(f"ERROR: Input directory not found: {input_dir}")
        return

    if os.path.exists(output_dir):
        if not args.yes:
            confirm = input(f"⚠️  Output directory '{output_dir}' exists. Overwrite? [y/N]: ").lower().strip()
            if confirm != 'y':
                print("Aborted.")
                return
        shutil.rmtree(output_dir)
    
    os.makedirs(output_dir, exist_ok=True)
    print(f"Output will be written to: {output_dir}")

    # --- Processing ---
    categories = [d for d in os.listdir(input_dir) if os.path.isdir(os.path.join(input_dir, d))]
    if not categories:
        print(f"No category subdirectories found in {input_dir}.")
        return

    print(f"Found {len(categories)} categories to combine.")

    with tqdm(categories, unit="category") as pbar:
        for category_name in pbar:
            pbar.set_description(f"Processing {category_name}")
            
            category_input_dir = os.path.join(input_dir, category_name)
            batch_files = sorted([f for f in os.listdir(category_input_dir) if f.startswith("batch_") and f.endswith(".ndjson")])

            if not batch_files:
                continue

            output_filename = f"{category_name}.ndjson"
            output_filepath = os.path.join(output_dir, output_filename)

            with open(output_filepath, "wb") as f_out:
                for batch_file in batch_files:
                    batch_filepath = os.path.join(category_input_dir, batch_file)
                    with open(batch_filepath, "rb") as f_in:
                        shutil.copyfileobj(f_in, f_out)

    print("\n✅ Combination complete.")
    print(f"Combined NDJSON files are available in: {output_dir}")

if __name__ == "__main__":
    main()
