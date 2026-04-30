"""Scans the essays folder, parses each filename, and extracts essay text.
 
Expected filename convention:
    {candidate_number}_{role}_assesment.pdf
 
where:
    - candidate_number is one or more digits
    - role is one of LTC, TFO, TRI
 
Anything that doesn't match raises an error — fail fast, don't guess.
"""
 
import re
from pathlib import Path
from typing import List, Dict, Tuple
 
import pdfplumber
 
 
# ============================================================
# CONVENTIONS
# ============================================================
# The expected filename suffix. Centralised here so a future rename
# (e.g. to "assessment" with the correct spelling) is a one-line change.
FILENAME_SUFFIX = "_assignment.pdf"
 
# Roles accepted in filenames. Must match what the grading prompt expects.
VALID_ROLES = {"LTC", "TFO", "TRI"}
 
# Strict filename pattern: digits, underscore, role, suffix.
# Anchored with ^ and $ so partial matches are rejected.
FILENAME_PATTERN = re.compile(
    r"^(?P<number>\d+)_(?P<role>[A-Z]+)" + re.escape(FILENAME_SUFFIX) + r"$"
)
 
# Heuristic for "this PDF is probably scanned/image-based".
# A PDF over this size that yields under this many characters is suspicious.
SCANNED_PDF_SIZE_THRESHOLD = 50_000  # bytes
SCANNED_PDF_TEXT_THRESHOLD = 100     # characters
 
 
# ============================================================
# PUBLIC API
# ============================================================
def load_essays(folder_path: str) -> List[Dict]:
    """Loads every PDF in the given folder, returns a list of essay records.
 
    Each record is a dict with:
        - candidate_number: str (e.g. "12345")
        - role: str (one of "LTC", "TFO", "TRI")
        - essay_text: str (extracted from the PDF)
        - source_file: str (original filename, useful for error messages)
 
    Raises
    ------
    FileNotFoundError : if the folder doesn't exist
    ValueError        : if the folder contains no PDFs, a filename is malformed,
                        a role is unrecognised, a candidate number is duplicated,
                        or a PDF appears to be scanned/empty.
    """
    folder = Path(folder_path)
 
    if not folder.exists():
        raise FileNotFoundError(f"Essays folder not found: {folder}")
    if not folder.is_dir():
        raise NotADirectoryError(f"Essays path is not a directory: {folder}")
 
    pdf_paths = sorted(folder.glob("*.pdf"))
 
    if not pdf_paths:
        raise ValueError(f"No PDF files found in {folder}")
 
    seen_numbers: Dict[str, str] = {}  # candidate_number -> filename (for dup detection)
    essays: List[Dict] = []
 
    for pdf_path in pdf_paths:
        candidate_number, role = _parse_filename(pdf_path.name)
 
        # Duplicate-number check. The user confirmed numbers are unique;
        # this guard catches accidental copies (e.g. "12345_LTC_assesment (1).pdf"
        # which wouldn't even match the regex, but also a legitimate-looking
        # "12345_LTC_assesment.pdf" alongside "12345_TFO_assesment.pdf").
        if candidate_number in seen_numbers:
            raise ValueError(
                f"Duplicate candidate number {candidate_number}: "
                f"appears in both '{seen_numbers[candidate_number]}' "
                f"and '{pdf_path.name}'."
            )
        seen_numbers[candidate_number] = pdf_path.name
 
        essay_text = _extract_pdf_text(pdf_path)
 
        essays.append({
            "candidate_number": candidate_number,
            "role": role,
            "essay_text": essay_text,
            "source_file": pdf_path.name,
        })
 
    return essays
 
 
# ============================================================
# INTERNAL HELPERS
# ============================================================
def _parse_filename(filename: str) -> Tuple[str, str]:
    """Extracts (candidate_number, role) from a filename or raises ValueError."""
    match = FILENAME_PATTERN.match(filename)
    if not match:
        raise ValueError(
            f"Filename '{filename}' does not match the expected pattern "
            f"'{{number}}_{{role}}{FILENAME_SUFFIX}'. "
            f"Example: '12345_LTC{FILENAME_SUFFIX}'"
        )
 
    candidate_number = match.group("number")
    role = match.group("role")
 
    if role not in VALID_ROLES:
        raise ValueError(
            f"Filename '{filename}' uses unrecognised role '{role}'. "
            f"Valid roles: {sorted(VALID_ROLES)}"
        )
 
    return candidate_number, role
 
 
def _extract_pdf_text(pdf_path: Path) -> str:
    """Extracts all text from a PDF. Raises if the result is suspiciously empty."""
    try:
        with pdfplumber.open(pdf_path) as pdf:
            pages = [page.extract_text() or "" for page in pdf.pages]
    except Exception as exc:
        raise ValueError(
            f"Failed to read PDF '{pdf_path.name}': {exc}"
        ) from exc
 
    text = "\n\n".join(pages).strip()
 
    # Sanity check: a 50KB+ PDF that yields almost no text is almost
    # certainly scanned/image-based. Refuse rather than send nonsense
    # to Claude.
    file_size = pdf_path.stat().st_size
    if file_size > SCANNED_PDF_SIZE_THRESHOLD and len(text) < SCANNED_PDF_TEXT_THRESHOLD:
        raise ValueError(
            f"PDF '{pdf_path.name}' yielded only {len(text)} chars of text "
            f"despite being {file_size:,} bytes. It is probably scanned or "
            f"image-based; OCR is not currently supported."
        )
 
    if not text:
        raise ValueError(f"PDF '{pdf_path.name}' contains no extractable text.")
 
    return text
 