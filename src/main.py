"""Main pipeline controller.

Flow:  PDFs in input/essays/  →  Claude grading  →  plagiarism screen  →  Excel report

Run `python src/main.py` to process everything, or scope a rubric-driven
regrade to particular roles with `--roles TRI`. New and edited submissions are
always graded regardless of the scope — see grading_cache.classify().

`--dry-run` reports what would be graded, and why, without spending anything.
Worth doing before any rubric *or extractor* change: reuse is keyed on a hash
of the extracted text, so changing how a PDF is read invalidates grades exactly
as a rubric edit does.
"""

import argparse
from pathlib import Path
from typing import Optional, Set

from campaign import active_campaign, fy_for_date, looks_stale
from pdf_loader import load_essays
from essay_grader import grade_essays, DEFAULT_MODEL, _load_grading_prompt
from grading_cache import (
    load_cache,
    save_cache,
    fingerprint,
    rubric_version,
    record_grade,
    classify,
    partition,
    merge_and_update,
    history,
    campaign_of,
    stale_keys,
    stale_extractions,
    GRADE_REASONS,
    REASON_CHANGED,
    REASON_NEW,
    REASON_REEXTRACTED,
    REASON_LABELS,
)
from plagiarism_checker import check_plagiarism, apply_plagiarism_overrides
from recruitment_list import (
    load_report as load_recruitment_list,
    submitted_dates,
    submitted_for,
    find_export,
    DEFAULT_LIST_DIR,
)
from embargo import find_embargoes, describe as describe_embargo, EMBARGO_MONTHS
from report_writer import write_report


def _report_dry_run(
    essays: list, cache: dict, current_prompt_hash: str, version: str, roles,
    campaign: str = "",
) -> None:
    """Prints what a real run would grade, and why, without grading anything.

    Reads the same classify() the real run uses, so the report cannot drift
    from what would actually happen.
    """
    from collections import Counter

    classified = classify(essays, cache, current_prompt_hash, roles, campaign)
    counts = Counter(reason for _, reason in classified)
    would_grade = sum(counts[r] for r in GRADE_REASONS)

    print()
    print("🔎 Dry run — nothing will be graded, no API calls made")
    print(f"   Rubric: {version or '(unversioned)'}")
    if campaign:
        other = sum(
            1 for e in cache.get("candidates", {}).values()
            if campaign_of(e) != campaign
        )
        print(f"   Campaign: {campaign}" + (
            f" ({other} cached grade(s) from earlier campaigns excluded)"
            if other else ""
        ))
    if roles is not None:
        print(f"   Regrade scoped to: {', '.join(sorted(roles))}")
    print()
    print(f"   Would grade {would_grade}:")
    for reason in GRADE_REASONS:
        if counts[reason]:
            print(f"      {counts[reason]:4d}  {REASON_LABELS[reason]}")
    print(f"   Would reuse {len(classified) - would_grade}:")
    for reason, count in counts.items():
        if reason not in GRADE_REASONS and count:
            print(f"      {count:4d}  {REASON_LABELS[reason]}")

    # Resubmissions bypass --roles, so name them rather than just counting.
    changed = [e for e, r in classified if r == REASON_CHANGED]
    if changed:
        print()
        print(
            f"   ⚠ {len(changed)} candidate(s) submitted a different file since "
            f"their last grade. This ignores --roles:"
        )
        for essay in changed:
            print(f"      {essay['candidate_number']}|{essay['role']}")

    _warn_stale_extractions(essays, cache, campaign)

    print()
    print(f"   → {would_grade} API call(s) if run for real. Nothing was spent.")


def _warn_stale_extractions(essays: list, cache: dict, campaign: str = "") -> None:
    """Warns when extraction drifted but EXTRACTOR_VERSION was not bumped.

    Grades are keyed on the file, so this costs nothing — which is the point.
    But it means the stored grades were produced from different text than we
    read today, and staying silent about that would trade a cost surprise for
    a correctness one.
    """
    drifted = stale_extractions(essays, cache, campaign)
    if not drifted:
        return
    print()
    print(
        f"   ⚠ {len(drifted)} file(s) now extract differently than when they "
        f"were graded, but EXTRACTOR_VERSION was not bumped, so their grades "
        f"will NOT refresh. Bump it in text_extractors.py if the new "
        f"extraction is better:"
    )
    for essay in drifted[:10]:
        print(f"      {essay['candidate_number']}|{essay['role']}")
    if len(drifted) > 10:
        print(f"      ... and {len(drifted) - 10} more")


NOT_CHECKED = "? embargo not checked — recruitment list missing"
NOT_LISTED = "? not in recruitment list"


def _load_applications(list_path) -> Optional[list]:
    """Reads the recruitment list once, for the embargo and the dates alike.

    Returns None when the export is absent, which callers treat as "cannot be
    checked" rather than as "nothing found".
    """
    try:
        # An explicit path wins; otherwise the newest dated export in input/.
        list_file = Path(list_path) if list_path else find_export()
        applications, skipped = load_recruitment_list(list_file)
    except FileNotFoundError as missing:
        print(
            f"   ⚠ {missing} The {EMBARGO_MONTHS}-month re-application embargo "
            f"was NOT checked, and no submission dates are available."
        )
        return None

    print(f"   ✓ Recruitment list: {list_file.name}")
    if skipped:
        # These rows carry no staff number or no readable date, so they cannot
        # place an application in time. Say so — a silently dropped row is a
        # candidate who could clear the embargo when they should not.
        print(f"   ⚠ {skipped} recruitment list row(s) skipped (no staff number or date)")
    return applications


def _apply_submission_dates(rows: list, applications: Optional[list]) -> None:
    """Annotates rows with the date their application arrived, in place.

    Each row supplies its own campaign, so this works for a Summary row (one
    campaign) and a History row (any campaign) without distinction. A date that
    cannot be resolved is left blank rather than approximated.
    """
    if not applications:
        for row in rows:
            row.setdefault("submitted", "")
        return

    dates = submitted_dates(applications)
    for row in rows:
        found = submitted_for(
            dates,
            str(row.get("candidate_number", "")),
            row.get("campaign", ""),
            row.get("Role", row.get("role", "")),
        )
        # A real date object, not a formatted string: the report_writer gives
        # it a display format, and Excel then sorts and filters it as a date.
        # As text, '02 Jul' would sort above '29 Jun'.
        row["submitted"] = found or ""


def _apply_embargoes(results: list, campaign: str, applications: Optional[list]) -> None:
    """Annotates each result with its re-application embargo status, in place.

    A blank cell must mean "checked, and clear" — never "not checked". Any
    candidate we could not judge is therefore marked explicitly, because a
    blank in a workbook that circulates would be read as a clean bill of
    health, which is exactly the mistake this column exists to prevent.
    """
    if applications is None:
        for result in results:
            result["embargo"] = NOT_CHECKED
        return

    embargoes = find_embargoes(applications, campaign)
    listed = {a.staff_number for a in applications}

    flagged = unlisted = 0
    for result in results:
        number = str(result.get("candidate_number", ""))
        if number in embargoes:
            result["embargo"] = describe_embargo(embargoes[number])
            flagged += 1
        elif number in listed:
            result["embargo"] = ""  # checked and clear
        else:
            result["embargo"] = NOT_LISTED
            unlisted += 1

    print(
        f"   ✓ Embargo check ({EMBARGO_MONTHS} months, any role): "
        f"{flagged} flagged, {len(results) - flagged - unlisted} clear, "
        f"{unlisted} not in the list"
    )
    if unlisted:
        print(
            "     Candidates absent from the list cannot be checked — the "
            "export may predate their application."
        )


def run_pipeline(
    roles: Optional[Set[str]] = None,
    dry_run: bool = False,
    report_only: bool = False,
    fy: Optional[str] = None,
    recruitment_list: Optional[str] = None,
) -> None:
    # ============================================================
    # PATHS  (project root = parent of src/)
    # ============================================================
    base_dir = Path(__file__).resolve().parent.parent

    essays_dir = base_dir / "input" / "essays"

    output_dir = base_dir / "output"
    output_dir.mkdir(parents=True, exist_ok=True)

    report_file = output_dir / "ai_essay_grading_report.xlsx"
    cache_file = output_dir / "grading_cache.json"

    print("🚀 Pipeline starting...")

    # The campaign governs what lands in the report, so state it every run —
    # this banner is the safeguard against the setting being silently wrong.
    override = (fy or "").strip().upper()
    campaign = override or active_campaign()
    source = "--fy" if override else "config/campaign.txt"
    print(f"📅 Campaign: {campaign}   (from {source})")
    if looks_stale(campaign):
        print(
            f"   ⚠ Today falls in {fy_for_date()}, but the campaign is set to "
            f"{campaign}. Edit config/campaign.txt when the new campaign starts."
        )

    # ============================================================
    # STEP 1 — LOAD ESSAYS FROM PDFs
    # ============================================================
    print(f"📄 Loading essays from {essays_dir}...")
    essays = load_essays(str(essays_dir))
    print(f"   ✓ Loaded {len(essays)} essay(s)")

    # ============================================================
    # STEP 2 — CLAUDE GRADING (incremental — cached grades reused)
    # ============================================================
    # Only new or changed essays are graded. If the rubric changed, the whole
    # cache is stale and everything is regraded (handled inside partition()).
    cache = load_cache(str(cache_file))
    # Fingerprint covers the rubric AND the model — a grade is only reusable
    # when both are unchanged.
    prompt_text = _load_grading_prompt()
    current_prompt_hash = fingerprint(prompt_text, DEFAULT_MODEL)
    version = rubric_version(prompt_text)

    # Return before grade_essays — the only place spend occurs. Essays are
    # still loaded above, because extraction is what produces the hashes the
    # report is about.
    if dry_run:
        _report_dry_run(
            essays, cache, current_prompt_hash, version, roles, campaign
        )
        return

    if report_only:
        # Rebuild the workbook from what is already cached. Skips grading
        # entirely — the point is to pick up report changes without paying to
        # regrade answers whose criteria have not changed.
        print(
            "📄 Report-only: rebuilding from cached grades. Nothing is graded; "
            "the plagiarism screen still runs (a few calls at most)."
        )
        to_grade, reused = [], []
        # An essay with no usable grade cannot appear in a report built from
        # the cache. Only a missing or superseded grade counts here — a grade
        # under an older rubric is exactly what this mode is for reusing, so
        # it is not "ungraded".
        ungraded = [
            essay
            for essay, reason in classify(
                essays, cache, current_prompt_hash, roles, campaign
            )
            if reason in (REASON_NEW, REASON_CHANGED, REASON_REEXTRACTED)
        ]
        if ungraded:
            print(
                f"   ⚠ {len(ungraded)} essay(s) have no usable grade and are "
                f"left out of the report. Run without --report-only to grade them:"
            )
            for essay in ungraded[:10]:
                print(f"      {essay['candidate_number']}|{essay['role']}")
            if len(ungraded) > 10:
                print(f"      ... and {len(ungraded) - 10} more")
    else:
        to_grade, reused = partition(
            essays, cache, current_prompt_hash, roles, campaign
        )
        print(
            f"🗃️  Reusing {len(reused)} cached grade(s); "
            f"grading {len(to_grade)} new/changed essay(s)"
        )
    _warn_stale_extractions(essays, cache, campaign)
    if roles is not None:
        # A scoped run must say plainly what it left behind, or a reviewer has
        # no way to know the report mixes rubrics.
        left = len(stale_keys(cache, current_prompt_hash, campaign)) - len(to_grade)
        print(
            f"   ⚠ Regrade scoped to {', '.join(sorted(roles))} — "
            f"{max(left, 0)} grade(s) from an older rubric left untouched"
        )

    if to_grade:
        print("📤 Sending essays to Claude...")

        def _persist(essay: dict, result: dict) -> None:
            """Save each grade the moment it lands.

            A full cold run takes hours; without this, one rate-limit error
            near the end would throw away every grade before it.
            """
            record_grade(
                cache, essay, result, current_prompt_hash, version, campaign
            )
            save_cache(str(cache_file), cache)

        new_results = grade_essays(to_grade, on_result=_persist)
    else:
        print("   ✓ Nothing to grade — all essays served from cache")
        new_results = []

    graded = list(zip(to_grade, new_results))
    results, plagiarism_essays = merge_and_update(
        cache, essays, graded, current_prompt_hash, version, campaign
    )
    save_cache(str(cache_file), cache)

    excluded = len(cache["candidates"]) - len(results)
    if excluded > 0:
        # A short report is otherwise alarming; say why it is short.
        print(
            f"   ℹ {excluded} grade(s) from earlier campaigns excluded "
            f"(still in the cache; report them with --fy)"
        )

    if not results:
        raise ValueError("No results available (no essays graded or cached)")

    # ============================================================
    # STEP 3 — PLAGIARISM SCREEN
    # ============================================================
    # Runs across the full corpus (folder + cached essays), not just the new
    # ones. Cheap lexical screen over every pair; only flagged pairs go to
    # Claude. High-risk matches downgrade 'Priority Interview' to 'Maybe'.
    print("🔍 Screening essay pairs for plagiarism...")
    similarity_pairs = check_plagiarism(plagiarism_essays)
    apply_plagiarism_overrides(results, similarity_pairs)
    print(f"   ✓ {len(similarity_pairs)} pair(s) flagged for review")

    # ============================================================
    # STEP 4 — RE-APPLICATION EMBARGO AND SUBMISSION DATES
    # ============================================================
    # Reported, never enforced: the row is graded and written either way, and a
    # human decides what the flag is worth. The recruitment list is read once
    # here and serves the embargo, the Submitted column and the History sheet.
    print("📋 Checking the re-application embargo...")
    applications = _load_applications(recruitment_list)
    _apply_embargoes(results, campaign, applications)
    _apply_submission_dates(results, applications)

    # Every campaign, deliberately — this is the cross-FY view.
    candidate_history = history(cache)
    _apply_submission_dates(candidate_history, applications)

    # ============================================================
    # STEP 5 — REPORT GENERATION
    # ============================================================
    print("📝 Writing Excel report...")
    write_report(
        results=results,
        output_path=str(report_file),
        similarity_pairs=similarity_pairs,
        history=candidate_history,
    )

    # ============================================================
    # DONE
    # ============================================================
    print("✅ Pipeline complete!")
    print(f"📄 Report saved at: {report_file}")

def _parse_args(argv=None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--roles",
        default=None,
        help=(
            "Comma-separated roles (e.g. TRI or TRI,TFO) to regrade when the "
            "rubric has changed. New and edited submissions are always graded "
            "whatever their role. Omit to regrade every stale grade."
        ),
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help=(
            "Report what would be graded and why, then exit without calling "
            "the API. Use before any rubric or extractor change to see what it "
            "will cost."
        ),
    )
    parser.add_argument(
        "--report-only",
        action="store_true",
        help=(
            "Rebuild the Excel report from cached grades without grading "
            "anything. Use to pick up report changes without paying to regrade "
            "answers whose criteria have not changed. Still runs the plagiarism "
            "screen, which the Similarity sheet needs."
        ),
    )
    parser.add_argument(
        "--fy",
        default=None,
        help=(
            "Report on a specific campaign, e.g. FY26, overriding "
            "config/campaign.txt for this run. Earlier campaigns stay in the "
            "cache, so a past year's report can be regenerated at any time."
        ),
    )
    parser.add_argument(
        "--recruitment-list",
        default=None,
        help=(
            "Path to the Instructor Recruitment Master List CSV export, which "
            "supplies the true submission dates the re-application embargo is "
            "measured from. Defaults to the newest "
            f"Recruitment_Export_<date>.csv in {DEFAULT_LIST_DIR}. Without it "
            "the embargo cannot be checked and every row says so."
        ),
    )
    return parser.parse_args(argv)


if __name__ == "__main__":
    args = _parse_args()
    selected = (
        {r.strip().upper() for r in args.roles.split(",") if r.strip()}
        if args.roles
        else None
    )
    run_pipeline(
        roles=selected,
        dry_run=args.dry_run,
        report_only=args.report_only,
        fy=args.fy,
        recruitment_list=args.recruitment_list,
    )
