"""Reads the Instructor Recruitment Master List export.

This is the second input to the pipeline, alongside the written assignments.
It is a CSV export of a Microsoft List, backed up nightly, and it holds one row
per application **across all campaigns** — which is what makes the re-application
embargo computable at all.

Why this file matters more than the PDFs
----------------------------------------
The PDFs cannot tell us when a candidate applied. Their filesystem mtime is
reset by copying, and their internal PDF `CreationDate` records when the
*document was authored*, not when it was submitted — a candidate reusing an old
document would look like an older applicant than they are. The List's `Created`
column is the real submission timestamp, so it is the only trustworthy basis for
a date-sensitive rule.

Column matching
---------------
The exported headers carry decorative emoji and inconsistent spacing (e.g.
'🔴 INTERVIEW DECISION 🔴', '📧 INTERVIEW EMAIL SENT  📧'). Matching them
literally would break the moment someone edits a column label in SharePoint, so
headers are normalised — emoji and punctuation stripped, whitespace collapsed,
lowercased — and looked up by that normalised form.

Only `Created` and `Staff Number` are required. Everything else is optional and
absent values become empty strings, so a List that gains or loses a workflow
column keeps loading.
"""

import csv
import datetime as dt
import re
from pathlib import Path
from typing import Dict, List, NamedTuple, Optional

BASE_DIR = Path(__file__).resolve().parent
DEFAULT_LIST_FILE = BASE_DIR.parent / "input" / "recruitment_list.csv"

# Dates arrive UK-style (dd/mm/yyyy), optionally with a time. '01/04/2026' is
# 1 April, never 4 January — parsing it the American way would silently shift
# submissions by up to eleven months and corrupt every embargo decision, so the
# formats are pinned explicitly rather than left to a guessing parser.
_DATE_FORMATS = ("%d/%m/%Y %H:%M:%S", "%d/%m/%Y %H:%M", "%d/%m/%Y")


class Application(NamedTuple):
    """One row of the List: a single application by one candidate."""

    staff_number: str
    submitted_at: dt.date
    role: str
    outcome: str  # OUTCOME column: YES / NO / PENDING / '' — see note below.
    successful: str  # 'APPLICATION SUCCESSFUL' — the written-task sift result.


def _normalise_header(header: str) -> str:
    """Reduces an exported column label to a stable lookup key.

    Strips the decorative emoji and any non-alphanumeric noise, collapses
    whitespace and lowercases, so '🟣 APPLICATION SUCCESSFUL 🟣' and
    'Application Successful' resolve to the same key.
    """
    cleaned = re.sub(r"[^0-9a-zA-Z]+", " ", header or "")
    return " ".join(cleaned.split()).lower()


# Normalised header -> the field we want it for.
_CREATED = "created"
_STAFF_NUMBER = "staff number"
_ROLE = "position applied for"
_OUTCOME = "outcome"
_SUCCESSFUL = "application successful"

REQUIRED_COLUMNS = (_CREATED, _STAFF_NUMBER)


def parse_date(value: str) -> Optional[dt.date]:
    """Parses a List timestamp to a date, or None if it is absent/unreadable.

    Returns a date rather than a datetime: the embargo is measured in months,
    so the time of day is noise, and dropping it keeps comparisons obvious.
    """
    value = (value or "").strip()
    if not value:
        return None
    for fmt in _DATE_FORMATS:
        try:
            return dt.datetime.strptime(value, fmt).date()
        except ValueError:
            continue
    return None


def load_applications(path: Optional[Path] = None) -> List[Application]:
    """Reads every application from the List export, oldest first.

    Rows with no staff number or no readable submission date are skipped: both
    are required to place an application in time and attribute it to a person,
    and a row missing either cannot participate in an embargo decision. The
    count of skipped rows is available via `load_report()` for callers that
    need to surface it.

    Raises FileNotFoundError if the export is absent, and ValueError if it is
    present but missing a required column — a silently empty result would read
    as "nobody has applied before", which is the one wrong answer that clears
    every candidate.
    """
    applications, _ = load_report(path)
    return applications


def load_report(path: Optional[Path] = None):
    """`load_applications`, plus a count of rows that could not be used.

    Returns (applications, skipped). Separated so the normal call site stays
    tidy while the pipeline can still report how much of the export it ignored.
    """
    list_file = Path(path) if path else DEFAULT_LIST_FILE

    # utf-8-sig: the export carries a BOM, and leaving it attached would turn
    # the first header into '﻿Created' and break the lookup.
    with open(list_file, newline="", encoding="utf-8-sig") as handle:
        reader = csv.reader(handle)
        try:
            headers = next(reader)
        except StopIteration:
            raise ValueError(f"'{list_file}' is empty — no header row.")

        index = {_normalise_header(h): i for i, h in enumerate(headers)}
        missing = [c for c in REQUIRED_COLUMNS if c not in index]
        if missing:
            raise ValueError(
                f"'{list_file}' is missing required column(s): "
                f"{', '.join(missing)}. Found: {', '.join(headers)}"
            )

        def field(row: List[str], key: str) -> str:
            position = index.get(key)
            if position is None or position >= len(row):
                return ""
            return (row[position] or "").strip()

        applications: List[Application] = []
        skipped = 0
        for row in reader:
            if not any((cell or "").strip() for cell in row):
                continue  # trailing blank line
            staff_number = field(row, _STAFF_NUMBER)
            submitted_at = parse_date(field(row, _CREATED))
            if not staff_number or submitted_at is None:
                skipped += 1
                continue
            applications.append(
                Application(
                    staff_number=staff_number,
                    submitted_at=submitted_at,
                    role=field(row, _ROLE),
                    outcome=field(row, _OUTCOME).upper(),
                    successful=field(row, _SUCCESSFUL).upper(),
                )
            )

    # Oldest first, so "the most recent prior application" is simply the last
    # match when scanning, and ordering is deterministic for tests.
    applications.sort(key=lambda a: (a.submitted_at, a.staff_number))
    return applications, skipped


def by_staff_number(applications: List[Application]) -> Dict[str, List[Application]]:
    """Groups applications by candidate, preserving the oldest-first order."""
    grouped: Dict[str, List[Application]] = {}
    for application in applications:
        grouped.setdefault(application.staff_number, []).append(application)
    return grouped
