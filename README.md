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

### Code Sharing Preferences
- Prefer **inline code shares** (e.g., code blocks directly in communication or documentation) for script snippets, logs, or fixes. Avoid artifact shares (e.g., external files, GitHub Gists) unless explicitly requested, as inline shares streamline review and integration.

### Backup Process
- Backups are saved locally with random, descriptive names (e.g., `headers_backup_april25.json`, `field_mapping_test_mar20.json`).
- Working files (`headers.json`, `field_mapping.json`, `Process500_Deals_v6.py`) are not renamed or versioned to maintain consistency.
- Before updating files, manually copy them to a backup folder with a unique name.