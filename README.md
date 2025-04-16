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
```bash
source keepa_venv/bin/activate
pip install -r requirements.txt
python3 Process500_Deals_v6.py