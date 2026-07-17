"""Main pipeline controller.

Flow:  PDFs in input/essays/  →  Claude grading  →  plagiarism screen  →  Excel report
"""

from pathlib import Path

from pdf_loader import load_essays
from essay_grader import grade_essays
from plagiarism_checker import check_plagiarism, apply_plagiarism_overrides
from report_writer import write_report


def run_pipeline() -> None:
    # ============================================================
    # PATHS  (project root = parent of src/)
    # ============================================================
    base_dir = Path(__file__).resolve().parent.parent

    essays_dir = base_dir / "input" / "essays"

    output_dir = base_dir / "output"
    output_dir.mkdir(parents=True, exist_ok=True)

    report_file = output_dir / "ai_essay_grading_report.xlsx"

    print("🚀 Pipeline starting...")

    # ============================================================
    # STEP 1 — LOAD ESSAYS FROM PDFs
    # ============================================================
    print(f"📄 Loading essays from {essays_dir}...")
    essays = load_essays(str(essays_dir))
    print(f"   ✓ Loaded {len(essays)} essay(s)")

    # ============================================================
    # STEP 2 — CLAUDE GRADING
    # ============================================================
    print("📤 Sending essays to Claude...")
    results = grade_essays(essays)

    if not results:
        raise ValueError("No results returned from Claude grading step")

    # ============================================================
    # STEP 3 — PLAGIARISM SCREEN
    # ============================================================
    # Cheap lexical screen over every pair; only flagged pairs go to Claude.
    # High-risk matches downgrade 'Priority Interview' to 'Maybe'.
    print("🔍 Screening essay pairs for plagiarism...")
    similarity_pairs = check_plagiarism(essays)
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

if __name__ == "__main__":
    run_pipeline()
