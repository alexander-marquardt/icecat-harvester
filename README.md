# Icecat Product Data Harvester

A Python toolset to harvest, parse, and organize product data from the [Open Icecat](https://icecat.biz) XML interface.

This project allows you to download product catalogs by category, automatically converting the raw, complex Icecat XML structure into clean, flat NDJSON (Newline Delimited JSON) files. It is designed to be robust, resumable, and easy to use for populating search engines (like Elasticsearch, Solr, or Typesense) or analytics tools.

## Features

* **Category-Based Downloading:** Target specific categories (e.g., "Smartphones", "Shoes") rather than downloading the entire massive dataset.
* **XML to JSON Conversion:** Flattens nested XML attributes into a usable JSON schema (`id`, `title`, `brand`, `images`, `specs`).
* **Dual-Save Mode:** Saves both the clean JSON output *and* the original raw XML files for inspection/debugging.
* **Smart Resume:** Automatically detects existing downloads and skips them to save bandwidth and time.
* **Strict Category Validation:** Filters out "Virtual Categories" (marketing filters) to ensure you only get physical product data.

## Prerequisites

1.  **Icecat Account:** You need a free [Open Icecat](https://icecat.biz/en/menu/register/user) account.
2.  **uv:** This project uses [uv](https://github.com/astral-sh/uv) for fast, zero-setup Python execution.
    * *Mac/Linux:* `curl -LsSf https://astral.sh/uv/install.sh | sh`
    * *Windows:* `powershell -c "irm https://astral.sh/uv/install.ps1 | iex"`

## Setup

1.  **Clone the repository:**
    ```bash
    git clone https://github.com/alexander-marquardt//icecat-harvester.git
    cd icecat-harvester
    ```

2.  **Configure Credentials:**
    Create a `.env` file in the root directory:
    ```bash
    cp .env.example .env
    ```
    Open `.env` and add your Icecat username and password:
    ```ini
    ICECAT_USER=your_username
    ICECAT_PASS=your_password
    ```

## Usage

All scripts can be run directly using `uv`, which handles dependencies (like `requests`) automatically.

### 1. Update Category Map
First, download the latest mapping of Category IDs to Names. This ensures you are targeting the correct categories and prevents "Ghost Category" errors.

```bash
uv run --with requests --with python-dotenv src/get_category_names.py
```

### 2. Check Statistics (Optional)
See how many products you have found per category in the index.

```
uv run --with requests src/stats.py
```

### 3. Configure Your Targets
Open targetx.txt and modify the list to match the products you want:

Python

```
Laptops
Laptop Spare Parts
PCs/Workstations
Tablets
Mobile Phone Cases
Mobile Phones
Notebooks
Smartphones
Smartwatches
TVs
TV Mounts & Stands
```

### 4. Run the Downloader
Start the harvest. The script will download the index, find matching products, and save them to the products/ directory.

```
uv run --with requests --with python-dotenv src/downloader.py
```

### Output Structure:

```
products/
├── Smartphones.ndjson      # Clean JSON (One product per line)
├── Laptops.ndjson
```



### Data License & Attribution
This repository contains code to access data provided by Icecat.

Data Source: [Open Icecat](https://icecat.biz/)

License: The product data downloaded by these scripts is subject to the [Open Icecat Content License (OPL)](https://www.google.com/search?q=https://icecat.biz/en/menu/support/terms-conditions).

Usage: You are free to use, distribute, and modify Open Icecat content, provided you attribute Icecat as the source.

Disclaimer: This tool is not affiliated with or endorsed by Icecat NV. It is an independent open-source utility for processing XML feeds. Users are responsible for ensuring their use of the data complies with Icecat's terms of service.

License
[MIT License](https://www.google.com/search?q=LICENSE) (for the code in this repository).