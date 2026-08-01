"""Main pipeline controller.

Flow:  PDFs in input/essays/  →  Claude grading  →  plagiarism screen  →  Excel report

Run `python src/main.py` to process everything, or scope a rubric-driven
regrade to particular roles with `--roles TRI`. New and edited submissions are
always graded regardless of the scope — see grading_cache.partition().
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
    partition,
    merge_and_update,
    stale_keys,
)
from plagiarism_checker import check_plagiarism, apply_plagiarism_overrides
from report_writer import write_report


def run_pipeline(roles: Optional[Set[str]] = None) -> None:
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
    to_grade, reused = partition(essays, cache, current_prompt_hash, roles)
    print(
        f"🗃️  Reusing {len(reused)} cached grade(s); "
        f"grading {len(to_grade)} new/changed essay(s)"
    )
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
    return parser.parse_args(argv)


if __name__ == "__main__":
    args = _parse_args()
    selected = (
        {r.strip().upper() for r in args.roles.split(",") if r.strip()}
        if args.roles
        else None
    )
    run_pipeline(roles=selected)
