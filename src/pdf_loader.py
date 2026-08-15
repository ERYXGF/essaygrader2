"""Scans the essays folder, parses each filename, and extracts essay text.

Expected filename convention:
    {candidate_number}_{role}_assignment.{extension}

where:
    - candidate_number is one or more digits
    - role is one of LTC, TFO, TRI

Filenames that don't match raise an error — fail fast, don't guess.

The *extension* is not trusted: the real format is sniffed from the file's
contents (candidates have submitted Pages documents named ".pdf"). Word and
Pages submissions are read and graded like any other, but flagged
wrong_format so the report can warn that interviewers cannot open the file
and the candidate must resubmit as a PDF. Only files we cannot read at all
go ungraded.
"""

import hashlib
import re
from pathlib import Path
from typing import List, Dict, Tuple

from text_extractors import (
    FORMAT_NAMES,
    FORMAT_PDF,
    FORMAT_UNKNOWN,
    UnreadableSubmission,
    extract_text,
)


# ============================================================
# CONVENTIONS
# ============================================================
# The expected filename suffix, used in error messages. The pattern below
# accepts any extension so a wrongly-saved submission is still identified.
FILENAME_SUFFIX = "_assignment.pdf"

# Roles accepted in filenames. Must match what the grading prompt expects.
# "TFO TRI" sits the same written paper as TRI and is assessed by the same
# criteria; it is a distinct role for reporting, not an alias for TRI.
VALID_ROLES = {"LTC", "TFO", "TRI", "TFO TRI"}

# Spellings that mean the same role. Submissions arrive named
# "12345_TFO TRI_assignment.pdf", but the underscored form is an easy thing for
# someone to type, and a filename typo should not abort a whole run.
ROLE_ALIASES = {"TFO_TRI": "TFO TRI"}

# Report labels for the File Format column.
WRONG_FORMAT_FLAG = "wrong format"
FORMAT_OK_LABEL = "OK"

# Strict filename pattern: digits, underscore, role, "_assignment", any
# extension. Anchored with ^ and $ so partial matches are rejected.
#
# The role group allows spaces and underscores so multi-word roles like
# "TFO TRI" parse. Greedy matching still resolves correctly — it backtracks off
# the trailing separator to satisfy "_assignment." — and VALID_ROLES remains the
# real gatekeeper, so a mistyped role still gets a clear error rather than being
# quietly accepted.
FILENAME_PATTERN = re.compile(
    r"^(?P<number>\d+)_(?P<role>[A-Z_ ]+)_assignment\.(?P<extension>[A-Za-z0-9]+)$"
)


# ============================================================
# PUBLIC API
# ============================================================
def load_essays(folder_path: str) -> List[Dict]:
    """Loads every submission in the folder, returns a list of essay records.

    Each record is a dict with:
        - candidate_number: str (e.g. "12345")
        - role: str (one of "LTC", "TFO", "TRI")
        - essay_text: str (extracted text; "" only when unreadable)
        - file_sha256: str (hash of the raw file bytes — the submission's
          identity for caching; "" if the file could not be read)
        - source_file: str (original filename, useful for error messages)
        - file_format: str (one of the text_extractors FORMAT_* values)
        - wrong_format: bool (True whenever the file is not a PDF)
        - format_label: str ("OK", "wrong format (Pages)", ...)
        - format_reason: str (why it could not be read; "" when it could)

    A Word or Pages submission still yields essay_text and is graded
    normally — only its container is wrong. Files that cannot be read at all
    come back with empty essay_text for the caller to flag.

    Raises
    ------
    FileNotFoundError : if the folder doesn't exist
    ValueError        : if the folder is empty, a filename is malformed, a
                        role is unrecognised, or a candidate number + role
                        combination is duplicated. Unreadable files are
                        flagged, not raised.
    """
    folder = Path(folder_path)

    if not folder.exists():
        raise FileNotFoundError(f"Essays folder not found: {folder}")
    if not folder.is_dir():
        raise NotADirectoryError(f"Essays path is not a directory: {folder}")

    # Every visible file, not just PDFs — a submission saved in the wrong
    # format must still reach the report.
    paths = sorted(
        p for p in folder.iterdir() if p.is_file() and not p.name.startswith(".")
    )

    if not paths:
        raise ValueError(f"No files found in {folder}")

    seen: Dict[Tuple[str, str], str] = {}  # (candidate_number, role) -> filename
    essays: List[Dict] = []

    for path in paths:
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

        try:
            essay_text, file_format = extract_text(path)
        except UnreadableSubmission as exc:
            essays.append(
                _record(path, candidate_number, role, "", FORMAT_UNKNOWN, str(exc))
            )
            continue

        essays.append(
            _record(path, candidate_number, role, essay_text, file_format)
        )

    return essays


# ============================================================
# INTERNAL HELPERS
# ============================================================
def _record(
    path: Path,
    candidate_number: str,
    role: str,
    essay_text: str,
    file_format: str,
    reason: str = "",
) -> Dict:
    """Builds one essay record and announces any format problem."""
    wrong_format = file_format != FORMAT_PDF

    if not wrong_format:
        label = FORMAT_OK_LABEL
    elif essay_text:
        label = f"{WRONG_FORMAT_FLAG} ({FORMAT_NAMES[file_format]})"
    else:
        label = f"{WRONG_FORMAT_FLAG} - unreadable"

    if wrong_format and essay_text:
        print(
            f"   ! '{path.name}' is a {FORMAT_NAMES[file_format]} document, "
            f"not a PDF - grading it anyway, flagged for resubmission"
        )
    elif wrong_format:
        print(f"   ! Cannot read '{path.name}': {reason}")

    return {
        "candidate_number": candidate_number,
        "role": role,
        "essay_text": essay_text,
        "file_sha256": _file_hash(path),
        "source_file": path.name,
        "file_format": file_format,
        "wrong_format": wrong_format,
        "format_label": label,
        "format_reason": reason,
    }


def _file_hash(path: Path) -> str:
    """sha256 of the submission's raw bytes.

    This is the submission's identity. Hashing the *file* rather than the text
    we read out of it is what lets the grading cache tell "the candidate
    resubmitted" apart from "we changed how PDFs are read" — see
    grading_cache.classify(). Returns '' if the file cannot be read, which
    falls the caller back to comparing extracted text.
    """
    digest = hashlib.sha256()
    try:
        with open(path, "rb") as fh:
            for chunk in iter(lambda: fh.read(65536), b""):
                digest.update(chunk)
    except OSError:
        return ""
    return digest.hexdigest()


def _parse_filename(filename: str) -> Tuple[str, str]:
    """Extracts (candidate_number, role) from a filename or raises ValueError.

    The role is normalised through ROLE_ALIASES, so 'TFO_TRI' and 'TFO TRI'
    both resolve to the single canonical spelling. Everything downstream — the
    cache key, the grading prompt, the report — then sees one role, not two
    that happen to mean the same thing.
    """
    match = FILENAME_PATTERN.match(filename)
    if not match:
        raise ValueError(
            f"Filename '{filename}' does not match the expected pattern "
            f"'{{number}}_{{role}}{FILENAME_SUFFIX}'. "
            f"Example: '12345_LTC{FILENAME_SUFFIX}'"
        )

    candidate_number = match.group("number")
    role = match.group("role").strip()
    role = ROLE_ALIASES.get(role, role)

    if role not in VALID_ROLES:
        raise ValueError(
            f"Filename '{filename}' uses unrecognised role '{role}'. "
            f"Valid roles: {sorted(VALID_ROLES)}"
        )

    return candidate_number, role
