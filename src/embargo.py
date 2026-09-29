"""The re-application embargo: rejected candidates must wait six months.

An application is flagged in two situations:

  - **Re-application.** The candidate applies within six months *after* a
    rejection. The six months run from the rejected application's
    **interview date** (the List's ACTUALINTERVIEWDATE) — or, when there was
    no interview because the application form itself was rejected, from its
    **submission date** (`Created`). The new application is placed by its
    own submission date.
  - **Double application.** Another of the candidate's applications, live
    at the same time as this one (`recruitment_list.overlapped`), was
    rejected. A rejection on one half of a double application counts against
    the other automatically, whichever was submitted or decided first.

Two further rules, both chosen rather than accidental:

  - **Any role counts.** Applying for TRI in one campaign and LTC in the next is
    still a re-application. The embargo attaches to the person, not the post.
  - **Only a rejected prior application counts.** A prior application must show
    a rejection at some stage — not taken to interview (`invited_to_interview`
    "NO"), `interview_decision` or `idp_sim_decision` equal to "NO", or
    `final_approval` or `final_idp_approval` equal to "REJECTED" (the
    decision fields and the approval fields use different enumerations —
    see `Application`'s field comments and `recruitment_list.was_rejected`). A candidate who was successful, or
    whose prior application is still pending a decision, does not trigger this.

There is **no campaign boundary** on a re-application: an earlier rejected
application counts whether it was in an earlier campaign or the *same* one.
The separate Double Application column (`main._apply_double_applications`)
answers a different question — are two applications *still open* at once —
so a candidate whose double application ends in a rejection moves from that
column to this one.

The flag belongs to the application being assessed — keyed by (staff
number, job number) — not to every application that person has: the
rejected job's own row is not flagged by its own rejection.

Nothing here blocks or skips anything. The embargo is reported, and a human
decides.
"""

import calendar
import datetime as dt
from typing import Dict, List, NamedTuple, Optional, Tuple

from recruitment_list import (
    Application,
    by_staff_number,
    campaign_of_application,
    closed_on,
    overlapped,
    was_rejected,
)

# The embargo period, measured from the rejected application's interview date,
# falling back to its submission date when there was no interview.
EMBARGO_MONTHS = 6


class Embargo(NamedTuple):
    """A current application that falls inside the embargo period."""

    staff_number: str
    current: Application  # the application being assessed now
    prior: Application  # the earlier application that triggers the embargo
    days_apart: int  # from the prior's embargo start to the current submission
    measured_from: str = "application"  # "interview" or "application"
    # True when the prior is the other half of a double application
    # (`recruitment_list.overlapped`), rather than one made before this.
    is_double: bool = False

    @property
    def prior_campaign(self) -> str:
        return campaign_of_application(self.prior)


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


def _embargo_start(application: Application) -> Tuple[dt.date, str]:
    """Where the six months begin for a rejected application, and which date
    that is — its closing date (`recruitment_list.closed_on`): the interview,
    or the submission when there was none. A rejected application is always
    closed, so this never comes back empty."""
    return closed_on(application)


EmbargoKey = Tuple[str, str]  # (staff number, job number of the current application)


def find_embargoes(
    applications: List[Application],
    campaign: str,
    window_months: int = EMBARGO_MONTHS,
) -> Dict[EmbargoKey, Embargo]:
    """Maps (staff number, job number) -> embargo, for applications in `campaign`.

    Only applications in `campaign` are assessed; the rejected application
    that triggers one can be in any campaign for a re-application, or the
    same/neighbouring one for a double application — see the module
    docstring. Where an application has several triggers, a re-application
    beats a double application, and the shortest gap wins within each: it is
    the clearest statement of how soon they re-applied.

    Returns an empty dict when nothing is flagged, so a caller can treat a
    missing key as "no embargo" without special-casing.
    """
    embargoes: Dict[EmbargoKey, Embargo] = {}

    for staff_number, history in by_staff_number(applications).items():
        current_apps = [
            a for a in history if campaign_of_application(a) == campaign
        ]
        if not current_apps:
            continue

        for current in current_apps:
            key = (staff_number, current.job_number)
            cutoff = subtract_months(current.submitted_at, window_months)
            for prior in history:
                if prior is current:
                    continue  # an application is never its own trigger
                if not was_rejected(prior):
                    continue
                start, basis = _embargo_start(prior)
                # The other half of a double application: its rejection counts
                # against this one whichever came first.
                double = overlapped(current, prior)
                if not double and not cutoff <= start <= current.submitted_at:
                    continue  # outside the window, or not yet rejected then
                found = Embargo(
                    staff_number, current, prior,
                    (current.submitted_at - start).days, basis, double,
                )
                best = embargoes.get(key)
                if best is None or _rank(found) < _rank(best):
                    embargoes[key] = found

    return embargoes


def _rank(embargo: Embargo) -> Tuple[bool, int]:
    """Sort key for competing triggers: re-applications first, then the
    shortest gap."""
    return embargo.is_double, abs(embargo.days_apart)


def embargo_for(
    embargoes: Dict[EmbargoKey, Embargo], staff_number: str, job_number: str = ""
) -> Optional[Embargo]:
    """The embargo for one report row, or None if it is clear.

    Matched on the job number when the row has one. A row without one (its
    Job Number join failed) falls back to any embargo for that person — the
    safe direction, since a missing join must never read as a clean bill.
    """
    if job_number:
        return embargoes.get((staff_number, job_number))
    found = [e for (staff, _), e in embargoes.items() if staff == staff_number]
    return min(found, key=_rank) if found else None


def describe(embargo: Embargo) -> str:
    """The report detail text for one embargo.

    For a re-application, names the prior campaign, how long ago it was,
    whether that is measured from its interview or (with no interview) its
    application, and which role it was for; for a double application, the
    other job and when it was rejected. Either way it ends with
    the decision that made it count — what a reviewer needs to sanity-check
    the flag, since a rejection on any one of five fields is enough to
    trigger it (see `recruitment_list.was_rejected`).

    Every value is printed as the List records them — PENDING and HOLD appear
    as themselves, and an empty field is called out as unrecorded rather than
    quietly rendered as a blank. The IDP pair is appended only when the List
    actually carries a value for it: most applications never reach that
    stage, and showing "not recorded" for both on every row would be noise.
    """
    start, basis = _embargo_start(embargo.prior)
    role = embargo.prior.role or "unknown role"
    if embargo.is_double:
        job = f" job {embargo.prior.job_number}" if embargo.prior.job_number else ""
        if basis == "interview":
            rejected = f" rejected at interview on {start.strftime('%d %b %Y')}"
        elif embargo.prior.invited_to_interview == "NO":
            rejected = ""  # the outcome below already says "not taken to interview"
        else:
            rejected = " rejected before interview"  # the List has no date for it
        lead = (
            f"⚠ Double application — {role}{job} ({embargo.prior_campaign})"
            f"{rejected}"
        )
    else:
        months = embargo.days_apart / 30.44
        lead = (
            f"⚠ Re-applied {embargo.days_apart}d ({months:.1f} months) after "
            f"{embargo.prior_campaign} {basis} on "
            f"{start.strftime('%d %b %Y')} ({role})"
        )
    if embargo.prior.invited_to_interview == "NO":
        outcome = "not taken to interview"
    else:
        outcome = f"interview: {embargo.prior.interview_decision or 'not recorded'}"
    detail = (
        f"{lead} — {outcome}, "
        f"approval: {embargo.prior.final_approval or 'not recorded'}"
    )
    if embargo.prior.idp_sim_decision or embargo.prior.final_idp_approval:
        detail += (
            f", IDP sim: {embargo.prior.idp_sim_decision or 'not recorded'}, "
            f"final IDP approval: {embargo.prior.final_idp_approval or 'not recorded'}"
        )
    return detail
