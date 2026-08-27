"""One-off: renames existing `input/essays/` files to carry a job number, and
migrates the grading cache in lockstep so nothing is regraded as a result.

Context: essay identity moved from (candidate_number, role) to
(candidate_number, role, job_number) so a second written assignment for the
same role no longer collides with the first (see pdf_loader.py). Files
already on disk predate that convention — this script backfills them from
the recruitment CSV (the source of truth for job numbers), once, so the
whole corpus is on the new naming convention.

Renaming a file changes what cache key it resolves to (`_cache_key` appends
`|job_number` once it's non-empty). Without migrating the cache in lockstep,
every renamed essay would look new on the next run and get regraded at full
API cost — so this script moves each affected cache entry to its new key in
the same pass, rather than leaving that to happen naturally on the next run.

Idempotent: a file that already carries a job number is skipped, so it's
safe to re-run later (e.g. after a future upload adds more old-style files).
Does not guess: a file with no matching CSV row, or a genuine tie between
several equally-latest CSV rows, is left unrenamed and reported.

Usage:
    venv/bin/python scripts/backfill_job_numbers.py [--essays-dir DIR]
        [--cache-path PATH] [--campaign FY26]
"""

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from grading_cache import _cache_key, load_cache, save_cache  # noqa: E402
from pdf_loader import _parse_filename  # noqa: E402
from recruitment_list import find_export, load_report, rows_for  # noqa: E402


def _new_filename(path: Path, staff: str, role: str, job_number: str) -> str:
    return f"{staff}_{role}_assignment_{job_number}{path.suffix}"


def resolve(files, applications, campaign):
    """Returns (renames, no_match, ties).

    renames: list of (Path, new_filename, job_number)
    no_match: list of filenames with no matching CSV row
    ties: list of (filename, [(job_number, submitted_at), ...])
    """
    renames = []
    no_match = []
    ties = []

    for path in files:
        candidate_number, role, job_number = _parse_filename(path.name)
        if job_number:
            continue  # already backfilled — idempotent

        matches = [
            a for a in rows_for(applications, candidate_number, campaign)
            if a.role == role
        ]
        if not matches:
            no_match.append(path.name)
            continue

        latest = max(m.submitted_at for m in matches)
        latest_matches = [m for m in matches if m.submitted_at == latest]
        if len(latest_matches) > 1:
            ties.append(
                (path.name, [(m.job_number, m.submitted_at) for m in matches])
            )
            continue

        chosen_job_number = latest_matches[0].job_number
        new_name = _new_filename(path, candidate_number, role, chosen_job_number)
        renames.append((path, new_name, candidate_number, role, chosen_job_number))

    return renames, no_match, ties


def migrate_cache_entry(cache, campaign, staff, role, job_number, new_source_file):
    old_key = _cache_key(campaign, staff, role)
    new_key = _cache_key(campaign, staff, role, job_number)
    candidates = cache["candidates"]
    if old_key not in candidates:
        return False
    entry = candidates.pop(old_key)
    entry["source_file"] = new_source_file
    if isinstance(entry.get("result"), dict):
        entry["result"]["source_file"] = new_source_file
        entry["result"]["job_number"] = job_number
    candidates[new_key] = entry
    return True


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--essays-dir", default="input/essays")
    parser.add_argument("--cache-path", default="output/grading_cache.json")
    parser.add_argument("--campaign", default="FY26")
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Report what would happen without renaming or touching the cache.",
    )
    args = parser.parse_args()

    essays_dir = Path(args.essays_dir)
    files = sorted(p for p in essays_dir.iterdir() if not p.name.startswith("."))

    export = find_export("input")
    applications, skipped = load_report(export)
    print(f"Recruitment CSV: {export} ({len(applications)} rows, {skipped} skipped)")
    print(f"Essays folder: {essays_dir} ({len(files)} files)")

    renames, no_match, ties = resolve(files, applications, args.campaign)

    # Safety check: no two renames may collide, and no target may already
    # exist on disk (both are not expected to trigger).
    targets = [new_name for _, new_name, *_ in renames]
    dupes = {n for n in targets if targets.count(n) > 1}
    if dupes:
        print(f"ABORT: rename targets collide: {sorted(dupes)}")
        return 1
    existing = [n for n in targets if (essays_dir / n).exists()]
    if existing:
        print(f"ABORT: rename target(s) already exist on disk: {existing}")
        return 1

    print(f"\nWill rename: {len(renames)}")
    print(f"No matching CSV row (left unrenamed): {len(no_match)}")
    for name in no_match:
        print(f"  - {name}")
    print(f"Ties (left unrenamed): {len(ties)}")
    for name, candidates_info in ties:
        print(f"  - {name}: {candidates_info}")

    if args.dry_run:
        print("\n--dry-run: no files renamed, cache untouched.")
        return 0

    if not renames:
        print("\nNothing to rename.")
        return 0

    cache = load_cache(args.cache_path)
    migrated = 0
    for path, new_name, staff, role, job_number in renames:
        new_path = path.parent / new_name
        path.rename(new_path)
        if migrate_cache_entry(cache, args.campaign, staff, role, job_number, new_name):
            migrated += 1

    save_cache(args.cache_path, cache)

    print(f"\nRenamed {len(renames)} file(s).")
    print(f"Migrated {migrated} cache entr{'y' if migrated == 1 else 'ies'}.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
