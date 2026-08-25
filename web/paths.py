"""Shared filesystem paths for the web layer — the same layout src/main.py uses.

Centralised here so app.py, jobs.py and uploads.py can't quietly disagree
about where input/, output/ or the venv live.
"""
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
SRC_DIR = PROJECT_ROOT / "src"
MAIN_PY = SRC_DIR / "main.py"
VENV_PYTHON = PROJECT_ROOT / "venv" / "bin" / "python"

INPUT_DIR = PROJECT_ROOT / "input"
ESSAYS_DIR = INPUT_DIR / "essays"

OUTPUT_DIR = PROJECT_ROOT / "output"
CACHE_FILE = OUTPUT_DIR / "grading_cache.json"

CONFIG_DIR = PROJECT_ROOT / "config"
CAMPAIGN_FILE = CONFIG_DIR / "campaign.txt"

LOGS_DIR = PROJECT_ROOT / "logs"
