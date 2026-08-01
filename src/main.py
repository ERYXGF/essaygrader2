"""Main pipeline controller.

Flow:  PDFs in input/essays/  →  Claude grading  →  plagiarism screen  →  Excel report

Run `python src/main.py` to process everything, or scope a rubric-driven
regrade to particular roles with `--roles TRI`. New and edited submissions are
always graded regardless of the scope — see grading_cache.classify().

`--dry-run` reports what would be graded, and why, without spending anything.
Worth doing before any rubric *or extractor* change: reuse is keyed on a hash
of the extracted text, so changing how a PDF is read invalidates grades exactly
as a rubric edit does.
"""

import argparse
from pathlib import Path
from typing import Optional, Set

from pdf_loader import load_essays
from essay_grader import grade_essays, DEFAULT_MODEL, _load_grading_prompt
from grading_cache import (
    load_cache,
    save_cache,
    fingerprint,
    rubric_version,
    record_grade,
    classify,
    partition,
    merge_and_update,
    stale_keys,
    stale_extractions,
    GRADE_REASONS,
    REASON_CHANGED,
    REASON_LABELS,
)
from plagiarism_checker import check_plagiarism, apply_plagiarism_overrides
from report_writer import write_report


def _report_dry_run(
    essays: list, cache: dict, current_prompt_hash: str, version: str, roles
) -> None:
    """Prints what a real run would grade, and why, without grading anything.

    Reads the same classify() the real run uses, so the report cannot drift
    from what would actually happen.
    """
    from collections import Counter

    classified = classify(essays, cache, current_prompt_hash, roles)
    counts = Counter(reason for _, reason in classified)
    would_grade = sum(counts[r] for r in GRADE_REASONS)

    print()
    print("🔎 Dry run — nothing will be graded, no API calls made")
    print(f"   Rubric: {version or '(unversioned)'}")
    if roles is not None:
        print(f"   Regrade scoped to: {', '.join(sorted(roles))}")
    print()
    print(f"   Would grade {would_grade}:")
    for reason in GRADE_REASONS:
        if counts[reason]:
            print(f"      {counts[reason]:4d}  {REASON_LABELS[reason]}")
    print(f"   Would reuse {len(classified) - would_grade}:")
    for reason, count in counts.items():
        if reason not in GRADE_REASONS and count:
            print(f"      {count:4d}  {REASON_LABELS[reason]}")

    # Resubmissions bypass --roles, so name them rather than just counting.
    changed = [e for e, r in classified if r == REASON_CHANGED]
    if changed:
        print()
        print(
            f"   ⚠ {len(changed)} candidate(s) submitted a different file since "
            f"their last grade. This ignores --roles:"
        )
        for essay in changed:
            print(f"      {essay['candidate_number']}|{essay['role']}")

    _warn_stale_extractions(essays, cache)

    print()
    print(f"   → {would_grade} API call(s) if run for real. Nothing was spent.")


def _warn_stale_extractions(essays: list, cache: dict) -> None:
    """Warns when extraction drifted but EXTRACTOR_VERSION was not bumped.

    Grades are keyed on the file, so this costs nothing — which is the point.
    But it means the stored grades were produced from different text than we
    read today, and staying silent about that would trade a cost surprise for
    a correctness one.
    """
    drifted = stale_extractions(essays, cache)
    if not drifted:
        return
    print()
    print(
        f"   ⚠ {len(drifted)} file(s) now extract differently than when they "
        f"were graded, but EXTRACTOR_VERSION was not bumped, so their grades "
        f"will NOT refresh. Bump it in text_extractors.py if the new "
        f"extraction is better:"
    )
    for essay in drifted[:10]:
        print(f"      {essay['candidate_number']}|{essay['role']}")
    if len(drifted) > 10:
        print(f"      ... and {len(drifted) - 10} more")


def run_pipeline(roles: Optional[Set[str]] = None, dry_run: bool = False) -> None:
    # ============================================================
    # PATHS  (project root = parent of src/)
    # ============================================================
    base_dir = Path(__file__).resolve().parent.parent

    essays_dir = base_dir / "input" / "essays"

    output_dir = base_dir / "output"
    output_dir.mkdir(parents=True, exist_ok=True)

    report_file = output_dir / "ai_essay_grading_report.xlsx"
    cache_file = output_dir / "grading_cache.json"

    print("🚀 Pipeline starting...")

    # ============================================================
    # STEP 1 — LOAD ESSAYS FROM PDFs
    # ============================================================
    print(f"📄 Loading essays from {essays_dir}...")
    essays = load_essays(str(essays_dir))
    print(f"   ✓ Loaded {len(essays)} essay(s)")

    # ============================================================
    # STEP 2 — CLAUDE GRADING (incremental — cached grades reused)
    # ============================================================
    # Only new or changed essays are graded. If the rubric changed, the whole
    # cache is stale and everything is regraded (handled inside partition()).
    cache = load_cache(str(cache_file))
    # Fingerprint covers the rubric AND the model — a grade is only reusable
    # when both are unchanged.
    prompt_text = _load_grading_prompt()
    current_prompt_hash = fingerprint(prompt_text, DEFAULT_MODEL)
    version = rubric_version(prompt_text)

    # Return before grade_essays — the only place spend occurs. Essays are
    # still loaded above, because extraction is what produces the hashes the
    # report is about.
    if dry_run:
        _report_dry_run(essays, cache, current_prompt_hash, version, roles)
        return

    to_grade, reused = partition(essays, cache, current_prompt_hash, roles)
    print(
        f"🗃️  Reusing {len(reused)} cached grade(s); "
        f"grading {len(to_grade)} new/changed essay(s)"
    )
    _warn_stale_extractions(essays, cache)
    if roles is not None:
        # A scoped run must say plainly what it left behind, or a reviewer has
        # no way to know the report mixes rubrics.
        left = len(stale_keys(cache, current_prompt_hash)) - len(to_grade)
        print(
            f"   ⚠ Regrade scoped to {', '.join(sorted(roles))} — "
            f"{max(left, 0)} grade(s) from an older rubric left untouched"
        )

    if to_grade:
        print("📤 Sending essays to Claude...")

        def _persist(essay: dict, result: dict) -> None:
            """Save each grade the moment it lands.

            A full cold run takes hours; without this, one rate-limit error
            near the end would throw away every grade before it.
            """
            record_grade(cache, essay, result, current_prompt_hash, version)
            save_cache(str(cache_file), cache)

        new_results = grade_essays(to_grade, on_result=_persist)
    else:
        print("   ✓ Nothing to grade — all essays served from cache")
        new_results = []

    graded = list(zip(to_grade, new_results))
    results, plagiarism_essays = merge_and_update(
        cache, essays, graded, current_prompt_hash, version
    )
    save_cache(str(cache_file), cache)

    if not results:
        raise ValueError("No results available (no essays graded or cached)")

    # ============================================================
    # STEP 3 — PLAGIARISM SCREEN
    # ============================================================
    # Runs across the full corpus (folder + cached essays), not just the new
    # ones. Cheap lexical screen over every pair; only flagged pairs go to
    # Claude. High-risk matches downgrade 'Priority Interview' to 'Maybe'.
    print("🔍 Screening essay pairs for plagiarism...")
    similarity_pairs = check_plagiarism(plagiarism_essays)
    apply_plagiarism_overrides(results, similarity_pairs)
    print(f"   ✓ {len(similarity_pairs)} pair(s) flagged for review")

    # ============================================================
    # STEP 4 — REPORT GENERATION
    # ============================================================
    print("📝 Writing Excel report...")
    write_report(
        results=results,
        output_path=str(report_file),
        similarity_pairs=similarity_pairs,
    )

    # ============================================================
    # DONE
    # ============================================================
    print("✅ Pipeline complete!")
    print(f"📄 Report saved at: {report_file}")

def _parse_args(argv=None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--roles",
        default=None,
        help=(
            "Comma-separated roles (e.g. TRI or TRI,TFO) to regrade when the "
            "rubric has changed. New and edited submissions are always graded "
            "whatever their role. Omit to regrade every stale grade."
        ),
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help=(
            "Report what would be graded and why, then exit without calling "
            "the API. Use before any rubric or extractor change to see what it "
            "will cost."
        ),
    )
    return parser.parse_args(argv)


if __name__ == "__main__":
    args = _parse_args()
    selected = (
        {r.strip().upper() for r in args.roles.split(",") if r.strip()}
        if args.roles
        else None
    )
    run_pipeline(roles=selected, dry_run=args.dry_run)
