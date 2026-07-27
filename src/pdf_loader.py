"""Scans the essays folder, parses each filename, and extracts essay text.
 
Expected filename convention:
    {candidate_number}_{role}_assesment.pdf
 
where:
    - candidate_number is one or more digits
    - role is one of LTC, TFO, TRI
 
Filenames that don't match raise an error — fail fast, don't guess.

Files we cannot read as PDFs are different: a submission saved in the
wrong format (a .docx, or a Pages/Word file renamed to .pdf) is a data
problem, not a system failure. Those are flagged wrong_format so the
report shows 'wrong format' against the candidate instead of the run
dying or the submission vanishing without trace.
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

# The only file type we can extract text from. Anything else is reported
# as a wrong-format row rather than silently dropped from the report.
PDF_EXTENSION = ".pdf"
WRONG_FORMAT_FLAG = "wrong format"

# Lenient pattern for labelling a wrong-format file: pulls the leading
# number and role out of a filename we otherwise can't process, so the
# reviewer knows which candidate needs to resubmit.
LOOSE_NAME_PATTERN = re.compile(r"^(?P<number>\d+)_(?P<role>[A-Za-z]+)")
 
# Strict filename pattern: digits, underscore, role, suffix.
# Anchored with ^ and $ so partial matches are rejected.
FILENAME_PATTERN = re.compile(
    r"^(?P<number>\d+)_(?P<role>[A-Z]+)" + re.escape(FILENAME_SUFFIX) + r"$"
)
 
# Heuristic for "this PDF is probably scanned/image-based".
# A PDF over this size that yields under this many characters is suspicious.
SCANNED_PDF_SIZE_THRESHOLD = 50_000  # bytes
SCANNED_PDF_TEXT_THRESHOLD = 100     # characters
 
 
class UnreadableSubmission(Exception):
    """Raised when a file cannot be turned into essay text.

    Carries a human-readable reason for the report; callers convert this
    into a wrong-format row rather than letting it abort the run.
    """


# ============================================================
# PUBLIC API
# ============================================================
def load_essays(folder_path: str) -> List[Dict]:
    """Loads every submission in the folder, returns a list of essay records.
 
    Each record is a dict with:
        - candidate_number: str (e.g. "12345")
        - role: str (one of "LTC", "TFO", "TRI")
        - essay_text: str (extracted from the PDF; "" when wrong_format)
        - source_file: str (original filename, useful for error messages)
        - wrong_format: bool (True for non-PDF files, which are never graded)

    Non-PDF files are not an error: they are returned flagged wrong_format
    so the report can show 'wrong format' against that candidate instead of
    the submission vanishing without trace.
 
    Raises
    ------
    FileNotFoundError : if the folder doesn't exist
    ValueError        : if the folder is empty, a PDF filename is malformed,
                        a role is unrecognised, or a candidate number + role
                        combination is duplicated. Files that cannot be read
                        as PDFs (wrong format, scanned, empty) are flagged
                        wrong_format instead of raising.
    """
    folder = Path(folder_path)
 
    if not folder.exists():
        raise FileNotFoundError(f"Essays folder not found: {folder}")
    if not folder.is_dir():
        raise NotADirectoryError(f"Essays path is not a directory: {folder}")
 
    # Every visible file, not just PDFs — unreadable submissions must still
    # appear in the report so the reviewer can chase a resubmission.
    paths = sorted(
        p for p in folder.iterdir() if p.is_file() and not p.name.startswith(".")
    )

    if not paths:
        raise ValueError(f"No files found in {folder}")

    seen: Dict[Tuple[str, str], str] = {}  # (candidate_number, role) -> filename
    essays: List[Dict] = []

    for path in paths:
        if path.suffix.lower() != PDF_EXTENSION:
            essays.append(_wrong_format_record(path))
            continue

        candidate_number, role = _parse_filename(path.name)

        # Duplicate check. A candidate may legitimately apply for more than
        # one role (e.g. "11111_LTC_..." alongside "11111_TRI_..."), so only
        # the same number AND role counts as a duplicate.
        key = (candidate_number, role)
        if key in seen:
            raise ValueError(
                f"Duplicate submission for candidate {candidate_number}, "
                f"role {role}: appears in both '{seen[key]}' "
                f"and '{path.name}'."
            )
        seen[key] = path.name

        # A .pdf extension is not proof of a PDF: candidates have submitted
        # Pages/Word files simply renamed. Extraction failure is reported,
        # not fatal.
        try:
            essay_text = _extract_pdf_text(path)
        except UnreadableSubmission as exc:
            essays.append(_wrong_format_record(path, str(exc)))
            continue

        essays.append({
            "candidate_number": candidate_number,
            "role": role,
            "essay_text": essay_text,
            "source_file": path.name,
            "wrong_format": False,
        })

    return essays
 
 
# ============================================================
# INTERNAL HELPERS
# ============================================================
def _wrong_format_record(path: Path, reason: str = "") -> Dict:
    """Record for a submission we cannot read as a PDF.

    Never graded and never screened for plagiarism (its essay_text is empty);
    it exists so the report shows 'wrong format' against the candidate, with
    `reason` explaining what was wrong with the file.
    """
    reason = reason or f"'{path.name}' is not a PDF file."
    match = LOOSE_NAME_PATTERN.match(path.name)
    candidate_number = match.group("number") if match else "unknown"
    role = match.group("role").upper() if match else "unknown"
    if role not in VALID_ROLES:
        role = "unknown"

    print(f"   ! Ignoring '{path.name}': {WRONG_FORMAT_FLAG} - {reason}")

    return {
        "candidate_number": candidate_number,
        "role": role,
        "essay_text": "",
        "source_file": path.name,
        "wrong_format": True,
        "format_reason": reason,
    }


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
        raise UnreadableSubmission(
            f"'{pdf_path.name}' could not be opened as a PDF - it is most "
            f"likely another format (Word, Pages) renamed to .pdf ({exc})."
        ) from exc
 
    text = "\n\n".join(pages).strip()
 
    # Sanity check: a 50KB+ PDF that yields almost no text is almost
    # certainly scanned/image-based. Refuse rather than send nonsense
    # to Claude.
    file_size = pdf_path.stat().st_size
    if file_size > SCANNED_PDF_SIZE_THRESHOLD and len(text) < SCANNED_PDF_TEXT_THRESHOLD:
        raise UnreadableSubmission(
            f"'{pdf_path.name}' yielded only {len(text)} characters of text "
            f"despite being {file_size:,} bytes. It is probably scanned or "
            f"image-based; OCR is not currently supported."
        )
 
    if not text:
        raise UnreadableSubmission(
            f"'{pdf_path.name}' contains no extractable text."
        )
 
    return text
 