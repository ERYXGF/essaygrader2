"""Reads the Instructor Recruitment Master List export.

This is the second input to the pipeline, alongside the written assignments.
It is a CSV export of a Microsoft List, backed up nightly, and it holds one row
per application **across all campaigns** — which is what makes the re-application
embargo computable at all.

It is read for one thing: **the submission date**. `role`, `outcome` and
`successful` are carried alongside it for reporting, and nothing else in the
export is interpreted. The file is opened read-only and never written to — every
output of this pipeline goes to the Excel workbook.

Why this file matters more than the PDFs
----------------------------------------
The PDFs cannot tell us when a candidate applied. Their filesystem mtime is
reset by copying, and their internal PDF `CreationDate` records when the
*document was authored*, not when it was submitted — a candidate reusing an old
document would look like an older applicant than they are. The List's `Created`
column is the real submission timestamp, so it is the only trustworthy basis for
a date-sensitive rule.

Two export shapes, one loader
-----------------------------
The List can be exported two ways and both are in use, so both are read:

  - The **friendly** export, whose headers are the display names, decorative
    emoji and all ('🔴 INTERVIEW DECISION 🔴'), with UK dates ('01/04/2026 11:56').
  - The **OData** export straight from SharePoint, whose headers are internal
    names with escapes ('Staff_x0020_Number'), whose dates are ISO 8601 UTC
    ('2026-06-04T13:26:07Z'), and whose choice fields arrive as JSON:
    '{"@odata.type":"...","Id":2,"Value":"LTC"}'.

Headers are therefore decoded (`_x0020_` → space) and then normalised — emoji
and punctuation stripped, whitespace collapsed, lowercased — so both shapes
resolve to the same key. Values are unwrapped from the JSON envelope where
present. Matching on the normalised name rather than on column position also
means the loader survives columns being added, removed or reordered, which the
two exports already disagree about.

Only `Created` and `Staff Number` are required. Everything else is optional and
absent values become empty strings, so a List that gains or loses a workflow
column keeps loading.
"""

import csv
import datetime as dt
import json
import re
from pathlib import Path
from typing import Dict, List, NamedTuple, Optional

from campaign import fy_for_date

BASE_DIR = Path(__file__).resolve().parent
DEFAULT_LIST_DIR = BASE_DIR.parent / "input"

# The nightly backup is named for the day it was taken:
# 'Recruitment_Export_2026-08-06.csv'. Matched case-insensitively, since the
# export's capitalisation is not ours to rely on.
EXPORT_GLOB = "[Rr]ecruitment_[Ee]xport_*.csv"
EXPORT_DATE = re.compile(r"(\d{4})[-_]?(\d{2})[-_]?(\d{2})")

# Accepted as a fallback for anyone who renamed their export by hand.
FALLBACK_NAME = "recruitment_list.csv"


def find_export(directory: Optional[Path] = None) -> Path:
    """The most recent recruitment export in `directory`.

    Newest is decided by **the date in the filename**, not the file's mtime.
    That is the same trap this whole feature exists to avoid: copying a file
    resets its mtime, so on a machine the exports were copied to, every one of
    them looks like it arrived on the day of the copy. The name carries the real
    date. An export whose name has no readable date sorts last rather than being
    ignored, so it is still usable when it is all there is.

    Raises FileNotFoundError naming the directory and the pattern — a missing
    export must be diagnosable, never indistinguishable from "nobody applied".
    """
    folder = Path(directory) if directory else DEFAULT_LIST_DIR

    def sort_key(path: Path):
        match = EXPORT_DATE.search(path.name)
        return (1, path.name) if match else (0, path.name)

    exports = sorted(folder.glob(EXPORT_GLOB), key=sort_key, reverse=True)
    if exports:
        return exports[0]

    fallback = folder / FALLBACK_NAME
    if fallback.exists():
        return fallback

    raise FileNotFoundError(
        f"No recruitment export in '{folder}'. Expected a file named like "
        f"'Recruitment_Export_2026-08-06.csv' (or '{FALLBACK_NAME}'), or pass "
        f"--recruitment-list with an explicit path."
    )

# The OData export uses ISO 8601 in UTC; the friendly export uses UK dates.
# '01/04/2026' is 1 April, never 4 January — parsing it the American way would
# shift a submission by nine months, which on its own is enough to clear a
# six-month embargo. The formats are therefore pinned explicitly rather than
# left to a guessing parser, and the day-first ones are tried first so an
# ambiguous value can never be read month-first by accident.
_DATE_FORMATS = (
    "%Y-%m-%dT%H:%M:%SZ",
    "%Y-%m-%dT%H:%M:%S",
    "%Y-%m-%d",
    "%d/%m/%Y %H:%M:%S",
    "%d/%m/%Y %H:%M",
    "%d/%m/%Y",
)

# SharePoint escapes characters it cannot use in an internal name as _xHHHH_,
# with astral characters running to eight hex digits (_x0001f4e7_ is 📧).
_SP_ESCAPE = re.compile(r"_x([0-9a-fA-F]{4,8})_")


class Application(NamedTuple):
    """One row of the List: a single application by one candidate."""

    staff_number: str
    submitted_at: dt.date
    role: str
    outcome: str  # final decision where recorded: YES/NO/PENDING/APPROVED/...
    successful: str  # 'APPLICATION SUCCESSFUL' — the written-task sift result.


def _decode_sharepoint(name: str) -> str:
    """Turns 'Staff_x0020_Number' back into 'Staff Number'."""
    def replace(match):
        try:
            return chr(int(match.group(1), 16))
        except (ValueError, OverflowError):
            return " "
    return _SP_ESCAPE.sub(replace, name or "")


def _normalise_header(header: str) -> str:
    """Reduces an exported column label to a stable lookup key.

    Decodes SharePoint's escapes, then strips decorative emoji and any other
    non-alphanumeric noise, collapses whitespace and lowercases — so
    '🟣 APPLICATION SUCCESSFUL 🟣', 'APPLICATIONSUCCESSFUL' and
    'Application Successful' all resolve to the same key.
    """
    cleaned = re.sub(r"[^0-9a-zA-Z]+", " ", _decode_sharepoint(header))
    return " ".join(cleaned.split()).lower()


def unwrap(value: str) -> str:
    """Extracts the useful text from a SharePoint choice field.

    The OData export renders a choice as
    '{"@odata.type":"...","Id":2,"Value":"LTC"}'. Anything that is not such an
    envelope is returned unchanged, so the friendly export passes straight
    through.
    """
    value = (value or "").strip()
    if not value.startswith("{"):
        return value
    try:
        decoded = json.loads(value)
    except (ValueError, TypeError):
        return value
    if isinstance(decoded, dict) and "Value" in decoded:
        return str(decoded["Value"] or "").strip()
    return value


# Lookup keys: the normalised header with spaces removed, which is what makes
# 'APPLICATION SUCCESSFUL' and 'APPLICATIONSUCCESSFUL' the same column.
_CREATED = "created"
_STAFF_NUMBER = "staffnumber"
_ROLE = "positionappliedfor"
_SUCCESSFUL = "applicationsuccessful"

# Readable names for the error message, since the lookup keys are squashed.
_COLUMN_LABELS = {_CREATED: "Created", _STAFF_NUMBER: "Staff Number"}

# The final decision lives under a different label in each export, and under
# more than one in the OData one. Tried in order of finality: an approval
# outcome beats an interview decision, which beats the generic column.
_OUTCOME_COLUMNS = ("finalapproval", "decision", "outcome")

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
    list_file = Path(path) if path else find_export()

    # utf-8-sig: the export carries a BOM, and leaving it attached would turn
    # the first header into '﻿Created' and break the lookup.
    with open(list_file, newline="", encoding="utf-8-sig") as handle:
        reader = csv.reader(handle)
        try:
            headers = next(reader)
        except StopIteration:
            raise ValueError(f"'{list_file}' is empty — no header row.")

        # Keys are the normalised header with spaces removed, so the friendly
        # export's 'APPLICATION SUCCESSFUL' and the OData export's
        # 'APPLICATIONSUCCESSFUL' land on the same key. First occurrence wins:
        # 'Position applied for' must beat its trailing 'Position applied
        # for#Id' companion, which normalises to a longer, distinct key anyway.
        index: Dict[str, int] = {}
        for position, header in enumerate(headers):
            index.setdefault(_normalise_header(header).replace(" ", ""), position)

        def locate(key: str) -> Optional[int]:
            """Column position for a key: exact match, else a unique prefix.

            SharePoint truncates internal names to 32 characters, sometimes
            mid-escape ('Have_x0020_you_x0020_applied_x00'), so an exact match
            is not always possible. A prefix is accepted only when exactly one
            header starts with it — an ambiguous prefix (`created` against both
            'Created' and 'Created By') must never silently pick one.
            """
            if key in index:
                return index[key]
            matches = [h for h in index if h.startswith(key)]
            # The OData export pairs each choice column with companions
            # ('...#Id', '...#Claims'), which share its prefix and would
            # otherwise make every prefix ambiguous. Drop them first.
            real = [h for h in matches if not h.endswith(("id", "claims", "odatatype"))]
            candidates = real or matches
            if not candidates:
                return None
            # Shortest wins: the plain column is always a prefix of its
            # decorated siblings. Still refuse a genuine tie between two
            # different columns of equal length.
            candidates.sort(key=len)
            if len(candidates) > 1 and len(candidates[0]) == len(candidates[1]):
                return None
            return index[candidates[0]]

        positions = {key: locate(key) for key in
                     (_CREATED, _STAFF_NUMBER, _ROLE, _SUCCESSFUL)}
        missing = [c for c in REQUIRED_COLUMNS if positions.get(c) is None]
        if missing:
            raise ValueError(
                f"'{list_file}' is missing required column(s): "
                f"{', '.join(_COLUMN_LABELS.get(c, c) for c in missing)}. "
                f"Found: {', '.join(headers[:20])}"
            )
        outcome_position = next(
            (p for p in (locate(c) for c in _OUTCOME_COLUMNS) if p is not None), None
        )

        def field(row: List[str], position: Optional[int]) -> str:
            if position is None or position >= len(row):
                return ""
            return unwrap(row[position])

        applications: List[Application] = []
        skipped = 0
        for row in reader:
            if not any((cell or "").strip() for cell in row):
                continue  # trailing blank line
            staff_number = field(row, positions[_STAFF_NUMBER])
            submitted_at = parse_date(field(row, positions[_CREATED]))
            if not staff_number or submitted_at is None:
                skipped += 1
                continue
            applications.append(
                Application(
                    staff_number=staff_number,
                    submitted_at=submitted_at,
                    role=field(row, positions[_ROLE]),
                    outcome=field(row, outcome_position).upper(),
                    successful=field(row, positions[_SUCCESSFUL]).upper(),
                )
            )

    # Oldest first, so "the most recent prior application" is simply the last
    # match when scanning, and ordering is deterministic for tests.
    applications.sort(key=lambda a: (a.submitted_at, a.staff_number))
    return applications, skipped


def submitted_dates(applications: List[Application]) -> Dict[tuple, dt.date]:
    """Maps (staff number, campaign, role) -> the date that application arrived.

    The campaign comes from the submission date itself via `fy_for_date`, so the
    export needs no campaign column of its own.

    **Where a candidate applied more than once for the same role in the same
    campaign, the latest date wins.** This is real, not hypothetical: 860775 has
    two FY26 LTC rows, 4 June and 15 July, but only one graded essay. The essay
    on file is the most recent submission, so the most recent date is the one
    that describes it.
    """
    dates: Dict[tuple, dt.date] = {}
    for application in applications:
        key = (
            application.staff_number,
            fy_for_date(application.submitted_at),
            application.role,
        )
        existing = dates.get(key)
        if existing is None or application.submitted_at > existing:
            dates[key] = application.submitted_at
    return dates


def submitted_for(
    dates: Dict[tuple, dt.date], staff_number: str, campaign: str, role: str
) -> Optional[dt.date]:
    """The submission date for one graded entry, or None if it is not known.

    Falls back to ignoring the role, which covers a candidate whose List entry
    records the post differently from the filename that was graded. Returns None
    rather than any nearby date when nothing matches — a Submitted cell must
    never be an inference.
    """
    exact = dates.get((staff_number, campaign, role))
    if exact is not None:
        return exact
    same_campaign = [
        date for (staff, camp, _), date in dates.items()
        if staff == staff_number and camp == campaign
    ]
    return max(same_campaign) if same_campaign else None


def by_staff_number(applications: List[Application]) -> Dict[str, List[Application]]:
    """Groups applications by candidate, preserving the oldest-first order."""
    grouped: Dict[str, List[Application]] = {}
    for application in applications:
        grouped.setdefault(application.staff_number, []).append(application)
    return grouped
