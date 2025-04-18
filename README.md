# Keepa API Deals Project
Processes Keepa deals with `Process500_Deals_v6.py`.

## Environment
- Virtualenv: `/home/timscripts/keepa_venv/`
- Python: 3.11
- Dependencies: See `requirements.txt`
- Config: `config.json` (Keepa API key)

## Rules
- Maintain chunk markers (`# Chunk X starts/ends`) for modular updates.
- No auto-updates to dependencies or Python for stability.
- Output: `keepa_full_deals_v6.csv`

## Setup
- source keepa_venv/bin/activate
- pip install -r requirements.txt
- python3 Process500_Deals_v6.py

## Development Setup
Tools used for developing and maintaining the project:

**Editor:** Sublime Text for editing `Process500_Deals_v6.py`, `field_mapping.json`, `stable_fields.py`, and other files. Preferred over nano or VS Code for its lightweight interface and syntax support.

**Version Control:** GitHub Desktop for committing and pushing changes to the Git repository, instead of terminal commands like git commit or git push.

**Environment:** Python 3.11 in `/home/timscripts/keepa_venv/`. Project files in `/home/timscripts/keepa_api/keepa-deals/`. Activate with source `keepa_venv/bin/activate`.

**Execution:** Run python `Process500_Deals_v6.py 2>&1 | tee output.txt` to generate `keepa_full_deals_v6.csv` with 192 columns (e.g., Percent Down 365, ASIN, Type).

April 18, 2025: Fixed safe_get error, added Package Volume (cm³) and Item Volume (cm³) to CSV (e.g., 234.94, 293.658 for 053123035X). FBA fees use package dimensions.