"""The re-application embargo: unsuccessful candidates must wait six months.

A candidate who applied in an earlier campaign and applies again within six
months of that earlier submission is flagged. The rule is deliberately blunt in
two ways, both chosen rather than accidental:

  - **Any role counts.** Applying for TRI in one campaign and LTC in the next is
    still a re-application. The embargo attaches to the person, not the post.
  - **Every prior applicant counts.** The pipeline has no reliable record of who
    was actually recruited, so it does not try to guess. A candidate who was
    successful is not applying again anyway, and one flagged wrongly is corrected
    by a human reading the report.

Campaign boundaries do the work of separating a re-application from a double
application. Candidates routinely apply for two roles in the *same* campaign,
days apart — that is the Double Application column's business, not this one — so
only an application from a **strictly earlier campaign** can trigger an embargo.
Reusing `campaign.fy_for_date` for that keeps one definition of a campaign year.

Nothing here blocks or skips anything. The embargo is reported, and a human
decides.
"""

import calendar
import datetime as dt
from typing import Dict, List, NamedTuple, Optional

from campaign import fy_for_date
from recruitment_list import Application, by_staff_number

# The embargo period, measured from the earlier submission date.
EMBARGO_MONTHS = 6


class Embargo(NamedTuple):
    """A current application that falls inside the embargo period."""

    staff_number: str
    current: Application  # the application being assessed now
    prior: Application  # the earlier application that triggers the embargo
    days_apart: int

    @property
    def prior_campaign(self) -> str:
        return fy_for_date(self.prior.submitted_at)


def subtract_months(date: dt.date, months: int) -> dt.date:
    """The same day-of-month `months` earlier, clamped to a real date.

    31 August minus 6 months is 28/29 February, not an error. Clamping to the
    last valid day is the conventional reading of "six months earlier" and errs
    toward a slightly *shorter* window, so it never extends an embargo beyond
    what the policy states.
    """
    month_index = date.month - 1 - months
    year = date.year + month_index // 12
    month = month_index % 12 + 1
    day = min(date.day, calendar.monthrange(year, month)[1])
    return dt.date(year, month, day)


def find_embargoes(
    applications: List[Application],
    campaign: str,
    window_months: int = EMBARGO_MONTHS,
) -> Dict[str, Embargo]:
    """Maps staff number -> embargo, for candidates applying in `campaign`.

    Only candidates with an application in `campaign` are considered, and only
    applications from strictly earlier campaigns can trigger one. Where a
    candidate has several qualifying pairs, the **shortest** gap is reported:
    it is the clearest statement of how soon they re-applied.

    Returns an empty dict when nothing is flagged, so a caller can treat a
    missing key as "no embargo" without special-casing.
    """
    embargoes: Dict[str, Embargo] = {}

    for staff_number, history in by_staff_number(applications).items():
        current_apps = [
            a for a in history if fy_for_date(a.submitted_at) == campaign
        ]
        if not current_apps:
            continue

        best: Optional[Embargo] = None
        for current in current_apps:
            cutoff = subtract_months(current.submitted_at, window_months)
            for prior in history:
                if fy_for_date(prior.submitted_at) >= campaign:
                    continue  # same campaign or later: not a re-application
                if not cutoff <= prior.submitted_at <= current.submitted_at:
                    continue  # outside the window, or somehow in the future
                gap = (current.submitted_at - prior.submitted_at).days
                if best is None or gap < best.days_apart:
                    best = Embargo(staff_number, current, prior, gap)

        if best is not None:
            embargoes[staff_number] = best

    return embargoes


def describe(embargo: Embargo) -> str:
    """The report cell text for one embargo.

    Names the prior campaign, how long ago it was, and which role it was for,
    because those are the three things a reviewer needs before overriding it.
    """
    months = embargo.days_apart / 30.44
    role = embargo.prior.role or "unknown role"
    return (
        f"⚠ Re-applied {embargo.days_apart}d ({months:.1f} months) after "
        f"{embargo.prior_campaign} application on "
        f"{embargo.prior.submitted_at.strftime('%d %b %Y')} ({role})"
    )
