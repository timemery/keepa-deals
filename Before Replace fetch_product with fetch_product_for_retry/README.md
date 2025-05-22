## Keepa API Deals Project

Processes Keepa deals with Keepa_Deals.py to generate Keepa_Deals_Export.csv.
Environment

Virtualenv: /home/timscripts/keepa_venv/
Python: 3.11
Dependencies: See requirements.txt
Config: config.json (Keepa API key)

## Rules

Maintain chunk markers (# Chunk X starts/ends) in Keepa_Deals.py for modular updates.
No auto-updates to dependencies or Python for stability.
Output: Keepa_Deals_Export.csv with 216 columns (e.g., Title, ASIN, Used Offer Count - Current).

## Setup

source keepa_venv/bin/activate
pip install -r requirements.txt
python3 Keepa_Deals.py
- External files: `headers.json` (protected CSV columns), `config.json` (API key), `deal_filters.json` (deal parameters).
- Output: `Keepa_Deals_Export.csv`

## Development Setup

Tools and processes for developing and maintaining the project:
Editor: Sublime Text for editing Keepa_Deals.py, stable.py, function_map.py, headers.json, and other files. Preferred over nano or VS Code for its lightweight interface and syntax support.
Version Control: GitHub Desktop for committing and pushing changes to the Git repository, instead of terminal commands like git commit or git push.
Environment: Python 3.11 in /home/timscripts/keepa_venv/. Project files in /home/timscripts/keepa_api/keepa-deals/. Activate with source keepa_venv/bin/activate.
Execution: Run python Keepa_Deals.py 2>&1 | tee output.txt to generate Keepa_Deals_Export.csv with 216 columns, including confirmed fields (e.g., Title, Sales Rank - Current) and untested fields (e.g., Used, like new - 30 days avg.).

## Project Structure

Keepa_Deals.py: Main script containing core logic (fetching deals, products, writing CSV) and untested functions (e.g., new_3rd_party_fbm). Imports headers.json, stable.py, and function_map.py. Never imported by other modules.
stable.py: Confirmed functions (e.g., get_title, sales_rank_current) for stable fields. No imports.
function_map.py: Defines FUNCTION_MAP for 192 confirmed header-to-function mappings, importing functions from stable.py.
headers.json: Defines 216 headers for CSV output, loaded by Keepa_Deals.py.
Logs: debug_log.txt for debugging, including unmapped headers and function outputs.

## Development Workflow

## Develop:

Add untested functions to Keepa_Deals.py (e.g., used_like_new).
Map them in the untested_functions dictionary in main().

## Test:

Run python Keepa_Deals.py.
Check debug_log.txt for function outputs and unmapped headers.
Verify Keepa_Deals_Export.csv has 216 columns with expected data.

## Confirm:

Move confirmed functions to stable.py.
Update FUNCTION_MAP in function_map.py.
Remove the function and its mapping from Keepa_Deals.py.

## Backup:

Before updates, back up files (e.g., cp Keepa_Deals.py Bak_May/keepa_deals_may12_2025.py).
Commit changes via GitHub Desktop.

## Code Sharing Preferences

Prefer inline code shares (e.g., code blocks in communication or documentation) for script snippets, logs, or fixes. Avoid artifact shares (e.g., external files, GitHub Gists) unless explicitly requested, as inline shares streamline review and integration.

## Backup Process

Backups are saved locally with random, descriptive names (e.g., keepa_deals_may12_2025.py, stable_may12_2025.py).
Working files (Keepa_Deals.py, stable.py, function_map.py, headers.json) are not renamed or versioned to maintain consistency.
Before updating files, manually copy them to a backup folder (e.g., /home/timscripts/keepa_api/keepa-deals/Bak_May/) with a unique name.

