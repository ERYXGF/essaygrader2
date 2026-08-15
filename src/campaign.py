"""Which recruitment campaign (financial year) a run belongs to.

Recruitment runs in financial years, 1 October to 30 September, and each year
starts from scratch: the report covers that year's written tasks only.

The campaign is **declared, not detected**. Two things rule out inferring it:

  - Submission files carry no usable date. Copying PDFs into the folder resets
    their mtime, so every file looks like it arrived the day it was collected.
  - The calendar is not authoritative either. Applications for the next FY can
    arrive during September, while the current campaign is still live, so any
    date-derived rule would silently misfile them.

So the campaign lives in `config/campaign.txt` — one line, edited once when the
campaign turns over. The date is used only to supply a sensible default and to
warn when the setting looks stale; it never overrides what the file says.
"""

import datetime as dt
import re
from pathlib import Path
from typing import Optional

BASE_DIR = Path(__file__).resolve().parent
CAMPAIGN_FILE = BASE_DIR.parent / "config" / "campaign.txt"

# The financial year starts on 1 October.
FY_START_MONTH = 10
FY_START_DAY = 1

# "FY" followed by exactly two digits. Deliberately strict: a typo like 'F26'
# or 'FY2026' must fail loudly, because an unrecognised campaign would match no
# cached grade and quietly produce an empty report.
CAMPAIGN_PATTERN = re.compile(r"^FY\d{2}$")


def fy_for_date(date: Optional[dt.date] = None) -> str:
    """The campaign label for a date, e.g. FY26 for 15 Aug 2026.

    A financial year is named for the calendar year it ends in: 1 Oct 2025 to
    30 Sep 2026 is FY26, so anything from 1 October belongs to the next one.
    """
    date = date or dt.date.today()
    year = date.year
    if (date.month, date.day) >= (FY_START_MONTH, FY_START_DAY):
        year += 1
    return f"FY{year % 100:02d}"


def active_campaign(path: Optional[Path] = None) -> str:
    """The campaign this run belongs to, from config/campaign.txt.

    Falls back to the date-implied campaign when the file is missing or empty,
    so a fresh checkout still runs. Raises on a malformed value rather than
    guessing — a campaign nothing matches would produce a silently empty report,
    which is far worse than stopping.
    """
    campaign_file = path or CAMPAIGN_FILE

    try:
        raw = campaign_file.read_text(encoding="utf-8")
    except OSError:
        return fy_for_date()

    # Ignore blank lines and #-comments so the file can explain itself.
    for line in raw.splitlines():
        value = line.split("#", 1)[0].strip().upper()
        if not value:
            continue
        if not CAMPAIGN_PATTERN.match(value):
            raise ValueError(
                f"'{campaign_file}' contains '{value}', which is not a valid "
                f"campaign. Expected 'FY' followed by two digits, e.g. 'FY26'."
            )
        return value

    return fy_for_date()


def looks_stale(campaign: str, date: Optional[dt.date] = None) -> bool:
    """True when the configured campaign is behind what the date implies.

    Only ever a warning. Being *ahead* of the calendar is legitimate and
    expected — September arrivals for the next FY are exactly why this setting
    is manual — so only a campaign lagging the date is worth flagging.
    """
    return campaign < fy_for_date(date)
