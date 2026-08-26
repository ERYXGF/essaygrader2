"""Atomic-swap helpers for the two upload endpoints.

Both replace what's on disk rather than merge into it — the confirmed
workflow is "drag in the whole current essays folder" / "drag in the latest
export", not an incremental add. Both write to a temp location first so a
failed or partial upload can never leave input/ half-populated.
"""
from __future__ import annotations

import re
import shutil
from datetime import date
from pathlib import Path
from typing import Iterable, List

from fastapi import UploadFile

from paths import INPUT_DIR, ESSAYS_DIR

# Same glob/date pattern recruitment_list.py's find_export() uses, so a
# replacement can never leave behind a file that logic would treat as newer.
EXPORT_GLOB = "[Rr]ecruitment_[Ee]xport_*.csv"
EXPORT_NAME_RE = re.compile(r"^[Rr]ecruitment_[Ee]xport_.*\.csv$")
EXPORT_DATE_RE = re.compile(r"(\d{4})[-_]?(\d{2})[-_]?(\d{2})")
FALLBACK_NAME = "recruitment_list.csv"


async def replace_essays(files: Iterable[UploadFile]) -> dict:
    """Atomically replaces input/essays/ with the uploaded files.

    Flattens each upload to its basename — browsers report a directory
    upload's files with a leading folder in `filename`, and stripping it also
    closes a path-traversal vector (`../../etc`).
    """
    INPUT_DIR.mkdir(parents=True, exist_ok=True)
    tmp_dir = INPUT_DIR / ".essays_upload_tmp"
    old_dir = INPUT_DIR / ".essays_upload_old"
    for stale in (tmp_dir, old_dir):
        if stale.exists():
            shutil.rmtree(stale)
    tmp_dir.mkdir()

    saved: List[str] = []
    sources: dict[str, List[str]] = {}
    try:
        for upload in files:
            original = upload.filename or ""
            name = Path(original).name
            if not name or name.startswith("."):
                continue
            (tmp_dir / name).write_bytes(await upload.read())
            saved.append(name)
            sources.setdefault(name, []).append(original)

        if not saved:
            raise ValueError("No files in the upload")

        if ESSAYS_DIR.exists():
            ESSAYS_DIR.rename(old_dir)
        tmp_dir.rename(ESSAYS_DIR)
        if old_dir.exists():
            shutil.rmtree(old_dir)
    except Exception:
        if tmp_dir.exists():
            shutil.rmtree(tmp_dir)
        raise

    # Two uploads can flatten to the same basename (e.g. same filename in
    # different subfolders) — later ones silently overwrite earlier ones on
    # disk. Surface that instead of letting the count quietly shrink.
    collisions = {name: paths for name, paths in sources.items() if len(paths) > 1}

    result = {
        "replaced": True,
        "count": len(set(saved)),
        "filenames": sorted(set(saved)),
    }
    if collisions:
        result["collisions"] = collisions
    return result


async def replace_recruitment_csv(file: UploadFile) -> dict:
    """Atomically replaces the recruitment export find_export() will pick up.

    Removes every existing dated export, and the hand-named fallback, first —
    otherwise a stale file could still be "newest by filename date" after
    this upload, or (for the fallback) get used at all once no dated export
    is left.
    """
    INPUT_DIR.mkdir(parents=True, exist_ok=True)
    for stale in list(INPUT_DIR.glob(EXPORT_GLOB)) + [INPUT_DIR / FALLBACK_NAME]:
        if stale.exists():
            stale.unlink()

    original_name = Path(file.filename or "").name
    if EXPORT_NAME_RE.match(original_name) and EXPORT_DATE_RE.search(original_name):
        filename = original_name
    else:
        filename = f"Recruitment_Export_{date.today().isoformat()}.csv"

    dest = INPUT_DIR / filename
    tmp = dest.with_name(dest.name + ".tmp")
    tmp.write_bytes(await file.read())
    tmp.replace(dest)

    return {"replaced": True, "filename": filename}
