"""The re-application embargo: rejected candidates must wait six months.

A candidate who was **rejected** and applies again within six months of that
rejection is flagged. The six months run from the rejected application's
**interview date** (the List's ACTUALINTERVIEWDATE) — or, when there was no
interview because the application form itself was rejected, from its
**submission date** (`Created`). The new application is always placed by its
submission date, and only an application made **on or after** that date
counts: someone who applied for two jobs at once and was later rejected on
one did not apply again after a rejection — that is a Double Application,
not an embargo. Two further rules, both chosen rather than accidental:

  - **Any role counts.** Applying for TRI in one campaign and LTC in the next is
    still a re-application. The embargo attaches to the person, not the post.
  - **Only a rejected prior application counts.** A prior application must show
    a rejection on at least one of its four decision fields —
    `interview_decision` or `idp_sim_decision` equal to "NO", or
    `final_approval` or `final_idp_approval` equal to "REJECTED" (the two
    decision fields and the two approval fields use different enumerations —
    see `Application`'s field comments). A candidate who was successful, or
    whose prior application is still pending a decision, does not trigger this.

There is **no campaign boundary** on the prior side: an earlier rejected
application counts whether it was in an earlier campaign or the *same* one.
This deliberately overlaps with the separately-computed Double Application
column (`report_writer._double_application_numbers`), which flags same-
campaign repeats regardless of outcome — the two columns answer different
questions and a candidate can appear in both, or either alone. What still
governs "earlier" is the submission timestamp itself, not the campaign label
or the interview date
— see `recruitment_list.campaign_of_application` for how a campaign is
resolved (declared FINANCIALYEAR, falling back to the date) where that label
is still used elsewhere in this module (naming which campaign a trigger came
from, e.g. in `describe()`).

The flag belongs to the application made inside the window — keyed by
(staff number, job number) — not to every application that person has: the
rejected job's own row is not "applying again".

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
    that is.

    The interview date when the candidate was interviewed; otherwise — the
    application form was rejected with no interview — the submission date.

    An interview dated before its own application is a data-entry error (the
    live List has one in 2023 against a 2026 application) and is ignored in
    favour of the submission date: trusting it would close the window early
    and clear the candidate.
    """
    if (application.interview_date is not None
            and application.interview_date >= application.submitted_at):
        return application.interview_date, "interview"
    return application.submitted_at, "application"


def _was_rejected(application: Application) -> bool:
    """True when this application's outcome, on any of its four decision
    fields, was a rejection.

    The two decision fields (interview_decision, idp_sim_decision) use
    YES/NO/PENDING/HOLD; the two approval fields (final_approval,
    final_idp_approval) use APPROVED/REJECTED/PENDING — different
    enumerations for the same underlying "no" answer.
    """
    return (
        application.interview_decision == "NO"
        or application.idp_sim_decision == "NO"
        or application.final_approval == "REJECTED"
        or application.final_idp_approval == "REJECTED"
    )


EmbargoKey = Tuple[str, str]  # (staff number, job number of the current application)


def find_embargoes(
    applications: List[Application],
    campaign: str,
    window_months: int = EMBARGO_MONTHS,
) -> Dict[EmbargoKey, Embargo]:
    """Maps (staff number, job number) -> embargo, for applications in `campaign`.

    Only candidates with an application in `campaign` are considered. A prior
    application can trigger an embargo whether it was in an earlier campaign
    or the same one — see the module docstring — as long as it was rejected
    (`_was_rejected`) and the current submission falls within the window
    after its start (`_embargo_start`) — never before it. Where an
    application has several qualifying priors, the **shortest** gap is
    reported: it is the clearest statement of how soon they re-applied.

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
                if not _was_rejected(prior):
                    continue
                start, basis = _embargo_start(prior)
                if not cutoff <= start <= current.submitted_at:
                    # Outside the window, or applied before this rejection —
                    # the latter is a Double Application, not an embargo.
                    continue
                gap = (current.submitted_at - start).days
                best = embargoes.get(key)
                if best is None or gap < best.days_apart:
                    embargoes[key] = Embargo(staff_number, current, prior, gap, basis)

    return embargoes


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
    return min(found, key=lambda e: e.days_apart) if found else None


def describe(embargo: Embargo) -> str:
    """The report detail text for one embargo.

    Names the prior campaign, how long ago it was, whether that is measured
    from its interview or (with no interview) its application, which role it
    was for, and
    the decision that made it count — what a reviewer needs to sanity-check
    the flag, since a rejection on any one of four fields is enough to
    trigger it (see `_was_rejected`).

    Every value is printed as the List records them — PENDING and HOLD appear
    as themselves, and an empty field is called out as unrecorded rather than
    quietly rendered as a blank. The IDP pair is appended only when the List
    actually carries a value for it: most applications never reach that
    stage, and showing "not recorded" for both on every row would be noise.
    """
    start, basis = _embargo_start(embargo.prior)
    role = embargo.prior.role or "unknown role"
    months = embargo.days_apart / 30.44
    detail = (
        f"⚠ Re-applied {embargo.days_apart}d ({months:.1f} months) after "
        f"{embargo.prior_campaign} {basis} on "
        f"{start.strftime('%d %b %Y')} ({role}) — "
        f"interview: {embargo.prior.interview_decision or 'not recorded'}, "
        f"approval: {embargo.prior.final_approval or 'not recorded'}"
    )
    if embargo.prior.idp_sim_decision or embargo.prior.final_idp_approval:
        detail += (
            f", IDP sim: {embargo.prior.idp_sim_decision or 'not recorded'}, "
            f"final IDP approval: {embargo.prior.final_idp_approval or 'not recorded'}"
        )
    return detail
