# Keepa Deals API
Jules collaboration on Keepa Deals.

## Overview
This codebase includes scripts (`Keepa_Deals.py`, `stable_products.py`, etc.) to fetch and process Keepa API data, outputting `Keepa_Deals_Export.csv`.

## Environment
- Virtualenv: `/home/timscripts/keepa_venv/`
- Python: 3.10.17
- Dependencies: See `requirements.txt`
- Config: `config.json` (API key)

## Rules
- Maintain chunk markers (e.g., `# Chunk X starts/ends`) in Python files for modular updates.
- Preserve all field-order comments (e.g., `# Percent Down 90`, `# Author starts/ends`) in Python files. These are critical for tracking yet-to-be-solved fields and ensuring their correct order in the CSV file (216 columns). Do not remove or modify these comments.
- No auto-updates to dependencies or Python for stability.
- Output: `Keepa_Deals_Export.csv` (216 columns, e.g., Title, ASIN, Used Offer Count - Current).

## Setup
### Python Version
- Standard: Python 3.10.17 (used in `/home/timscripts/keepa_venv/` and Jules’ environment).
- Note: Python 3.10.17 ensures consistency and compatibility with the Keepa API.

### Setup Instructions
1. Clone: `git clone https://github.com/timemery/keepa-deals`
2. Create venv: `/usr/local/bin/python3.10 -m venv /home/timscripts/keepa_venv`
3. Activate: `source /home/timscripts/keepa_venv/bin/activate`
4. Install: `pip install -r requirements.txt`
5. Run: `python3 Keepa_Deals.py --no-cache`
6. Outputs: `Keepa_Deals_Export.csv`, `debug_log.txt`

### Dependencies
See `requirements.txt` for exact versions. Key packages:
- certifi==2025.4.26
- charset-normalizer==3.4.2
- idna==3.10
- numpy==2.2.6
- pandas==2.2.3
- python-dateutil==2.9.0.post0
- pytz==2025.2
- requests==2.32.3
- retrying==1.3.4
- six==1.16.0
- tzdata==2025.2
- urllib3==2.4.0

## Development Setup
- **Editor**: Sublime Text for editing.
- **Version Control**: GitHub Desktop for commits.
- **Environment**: Python 3.10.17 in `/home/timscripts/keepa_venv/`. Project files: `/home/timscripts/keepa_api/keepa-deals/`.
- **Execution**: `source /home/timscripts/keepa_venv/bin/activate; pip install -r requirements.txt; python3 Keepa_Deals.py --no-cache`

## Project Structure
- `Keepa_Deals.py`: Main script for fetching deals and writing CSV.
- `stable_products.py`: Defines product conditions (e.g., used_good, used_like_new).
- `stable_deals.py`: Handles deal logic (e.g., Percent Down 90).
- `stable_calculations.py`: Isolates calculated fields (e.g., Percent Down 90).
- `field_mappings.py`: Maps CSV headers to functions (FUNCTION_LIST).
- `headers.json`: Defines 216 CSV column headers or mappings.
- `config.json`: Contains the Keepa API key.
- `requirements.txt`: Lists dependencies (requests, retrying, pandas, pytz).
- `README.md`: Project documentation.
- `Keepa_Deals_Export.csv`: Output file for deal data.
- `debug_log.txt`: Debug logs (e.g., stats.current).
- `API_Dev_Log_v4.txt`: Development log.

## Backup Process
- Backups in `/home/timscripts/keepa_api/Bak_May/` with descriptive names (e.g., `keepa_deals_may12_2025.py`).

## Code Sharing
- Prefer inline code blocks for snippets, logs, or fixes.