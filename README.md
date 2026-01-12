# Icecat Product Data Harvester

A robust Python toolset to harvest, parse, and organize product data from the [Open Icecat](https://icecat.biz) XML interface.

This project is designed as an **ETL (Extract, Transform, Load) pipeline**:
1.  **Extract:** Downloads raw XML files for specific categories from the Icecat repository.
2.  **Transform:** Parses local XML files into clean, flat NDJSON files ready for search engines.

## Features

* **Clean Separation:** Downloads and parsing are distinct steps. You can modify your JSON schema and re-parse millions of products without re-downloading them.
* **Resumable Downloads:** The downloader checks the file index and skips files you already have.
* **Standard Python Layout:** Built as a proper Python package (`src-layout`) for better manageability.
* **Fast Execution:** Uses `uv` for zero-setup environment management and `tqdm` for progress tracking.

## Project Structure

```text
icecat-harvester/
├── data/                  # All data lives here (ignored by git)
│   ├── xml_source/        # Raw XML files (The "Truth")
│   └── products/          # Processed NDJSON files (The "Output")
├── src/
│   └── icecat_harvester/  # Python Package
│       ├── download_xml.py
│       ├── xml_to_json.py
│       └── ...
├── pyproject.toml         # Dependencies
├── targets.txt            # List of categories to download
└── .env                   # Credentials
```

## Prerequisites

* **Icecat Account:** You need a free [Open Icecat](https://icecat.biz/en/menu/register/user) account.
* **uv:** This project uses [uv](https://github.com/astral-sh/uv) for fast Python management.
    * *Mac/Linux:* `curl -LsSf https://astral.sh/uv/install.sh | sh`
    * *Windows:* `powershell -c "irm https://astral.sh/uv/install.ps1 | iex"`

## Installation

1.  **Clone the repository:**
    ```bash
    git clone [https://github.com/YOUR_USERNAME/icecat-harvester.git](https://github.com/YOUR_USERNAME/icecat-harvester.git)
    cd icecat-harvester
    ```

2.  **Sync Dependencies:**
    This creates the virtual environment and installs all required libraries (requests, tqdm, etc.).
    ```bash
    uv sync
    ```

3.  **Configure Credentials:**
    Create a `.env` file in the root directory:
    ```bash
    cp .env.example .env
    ```
    Add your Icecat username and password inside `.env`.

## Usage

All commands are run using `uv run -m` to execute the modules within the package context.

### 1. Configure Targets
Edit the `targets.txt` file in the root directory. Add the categories you want to harvest, one per line:
```text
Laptops
Smartphones
Washing Machines
```

### 2. Update Category Map
Download the latest mapping of Category IDs to Names. This ensures the downloader can find the categories you listed in `targets.txt`.
```bash
uv run -m icecat_harvester.get_category_names
```

### 3. Download Data (Extract)
This script downloads the **Raw XML** files into `data/xml_source/`.
* It checks the Icecat index.
* It skips files you already have.
* It organizes files into folders by category.

```bash
uv run -m icecat_harvester.download_xml
```

### 4. Process Data (Transform)
This script reads your local XML files and converts them into clean **NDJSON** (Newline Delimited JSON) files in `data/products/`.
* Run this whenever you change your JSON schema or parsing logic.
* No internet connection required.

```bash
uv run -m icecat_harvester.xml_to_json
```

### 5. Check Statistics (Optional)
See how many products exist in the index for your categories versus how many you have downloaded.
```bash
uv run -m icecat_harvester.stats
```

## Output Format

The output JSON files (`data/products/Category.ndjson`) use a flat structure optimized for search engines:

```json
{"id": "12345", "title": "Product Name", "brand": "Brand", "category": "Laptops", "images": ["url1", "url2"], "specs": {"Screen Size": "15 inch", "RAM": "16GB"}}
{"id": "67890", ...}
```

## License

* **Code:** [MIT License](LICENSE)
* **Data:** The product data downloaded by these scripts is subject to the [Open Icecat Content License (OPL)](https://icecat.biz/en/menu/support/terms-conditions).

> **Disclaimer:** This tool is not affiliated with Icecat NV. Users are responsible for ensuring their use of the data complies with Icecat's terms.