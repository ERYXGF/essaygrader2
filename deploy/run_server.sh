#!/bin/bash
# Launched by launchd (see essaygrader2.plist template) to keep the web app
# running. Always uses the project's own venv — never the system python3
# (which on this Mac is 3.9; the venv is 3.12, and web/app.py relies on it).
set -euo pipefail

PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$PROJECT_DIR"

exec venv/bin/uvicorn web.app:app --host 127.0.0.1 --port "${PORT:-8002}"
