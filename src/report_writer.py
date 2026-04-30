"""Writes the Excel grading report.
 
The report has two sheets:
- Summary  : one row per candidate, headline grades, colour-coded classification,
             plus three feedback columns (Human Override / Override Reason /
             Reviewed) ready for the eventual RAG phase.
- Detailed : strengths, weaknesses, rationale, AI indicators, Q1 summary.
"""

from pathlib import Path
from typing import List, Dict

from openpyxl import Workbook
from openpyxl.styles import Font, PatternFill, Alignment


def write_report(results: List[Dict], output_path: str) -> None:
    """Writes the grading results to an .xlsx file at output_path."""
    if not results:
        raise ValueError("No results provided to write report")

    output_file = Path(output_path)
    output_file.parent.mkdir(parents=True, exist_ok=True)

    wb = Workbook()

    # =========================
    # STYLES
    # =========================
    header_font = Font(bold=True)
    center = Alignment(horizontal="center", vertical="center")

    green = PatternFill(start_color="C6EFCE", end_color="C6EFCE", fill_type="solid")
    yellow = PatternFill(start_color="FFEB9C", end_color="FFEB9C", fill_type="solid")
    red = PatternFill(start_color="FFC7CE", end_color="FFC7CE", fill_type="solid")

    # ============================================================
    # SHEET 1 — SUMMARY
    # ============================================================
    # The last three columns are intentionally left empty on first write.
    # The reviewer fills them in; a future feedback_capture script will
    # ingest completed rows into the RAG database.
    ws1 = wb.active
    ws1.title = "Summary"

    summary_headers = [
        "Candidate Number",
        "Role",
        "Classification",
        "Passion",
        "Humility",
        "Potential",
        "Writing Quality",
        "AI Risk",
        "Human Override",
        "Override Reason",
        "Reviewed",
    ]

    for col, h in enumerate(summary_headers, 1):
        cell = ws1.cell(row=1, column=col, value=h)
        cell.font = header_font
        cell.alignment = center

    for row_idx, r in enumerate(results, start=2):
        cca = r.get("cross_cutting_assessment", {}) or {}
        writing = r.get("writing_quality", {}) or {}

        values = [
            r.get("candidate_number", "Unknown"),
            r.get("Role", "Unknown"),
            r.get("classification", "Unknown"),
            cca.get("passion", ""),
            cca.get("humility", ""),
            cca.get("potential", ""),
            writing.get("rating", ""),
            r.get("ai_usage_probability", ""),
            "",  # Human Override (blank — reviewer fills in)
            "",  # Override Reason
            "",  # Reviewed
        ]

        for col, val in enumerate(values, 1):
            cell = ws1.cell(row=row_idx, column=col, value=val)
            cell.alignment = center

        # Classification colour band
        class_cell = ws1.cell(row=row_idx, column=3)
        classification = r.get("classification", "")

        if classification == "Priority Interview":
            class_cell.fill = green
        elif classification == "Maybe":
            class_cell.fill = yellow
        elif "Do Not Interview" in classification:
            class_cell.fill = red

    _autosize(ws1, max_width=40)

    # ============================================================
    # SHEET 2 — DETAILED
    # ============================================================
    ws2 = wb.create_sheet(title="Detailed")

    detail_headers = [
        "Candidate Number",
        "Source File",
        "Strengths",
        "Weaknesses",
        "Rationale",
        "AI Indicators",
        "Q1 Summary",
    ]

    for col, h in enumerate(detail_headers, 1):
        cell = ws2.cell(row=1, column=col, value=h)
        cell.font = header_font

    for row_idx, r in enumerate(results, start=2):
        strengths = r.get("strengths", []) or []
        weaknesses = r.get("weaknesses", []) or []
        q_assessments = r.get("question_assessments", []) or []

        values = [
            r.get("candidate_number", "Unknown"),
            r.get("source_file", ""),
            ", ".join(strengths),
            ", ".join(weaknesses),
            r.get("rationale", ""),
            r.get("ai_usage_indicators", ""),
            q_assessments[0].get("summary", "") if q_assessments else "",
        ]

        for col, val in enumerate(values, 1):
            ws2.cell(row=row_idx, column=col, value=val)

    _autosize(ws2, max_width=60)

    # =========================
    # SAVE
    # =========================
    wb.save(output_file)


def _autosize(ws, max_width: int = 40) -> None:
    """Approximate column auto-sizing based on content length."""
    for col in ws.columns:
        max_len = max(
            (len(str(c.value)) for c in col if c.value is not None),
            default=0,
        )
        ws.column_dimensions[col[0].column_letter].width = min(max_len + 2, max_width)
