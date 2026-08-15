"""Persistent grading cache for incremental runs.

The pipeline is expected to run repeatedly as new PDFs are dropped into
`input/essays/`. Grading is one Claude call per essay, so regrading essays that
haven't changed is wasted spend. This module persists prior grades (keyed by
candidate + role + a hash of the essay text) so each run only grades new or
edited submissions.

Design notes:
  - The cache is the source of truth, not the .xlsx report. It also stores each
    essay's text, so plagiarism can still compare against a candidate whose PDF
    was later removed from the folder, and the report is always rebuilt from the
    full merged set.
  - A fingerprint of the grading prompt (`config/essay_prompt.txt`) AND the
    model is stored on **every entry**. A grade is reusable only when the
    rubric and the model behind it are unchanged — grades must never silently
    mix rubrics or models, and one hiring report should never contain some
    candidates judged by a weaker model than others.
  - The hash is per entry rather than global so a run can be scoped to a subset
    of roles (see `partition(roles=...)`). Rows outside that scope keep the
    hash they were actually graded under, so the cache stays honest about which
    grades are current and which are stale. `stale_keys()` reports the
    difference; the top-level `prompt_sha256` records the most recent run only
    and is not used to decide reuse.
  - Grades are written one at a time as they complete (`record_grade`), not in
    one batch at the end, so an interrupted run keeps everything it finished.
    A full cold run takes hours; losing it to a rate limit is not acceptable.
  - Cache entries are keyed by (campaign, candidate_number, role). A candidate
    may legitimately apply for more than one role, exactly as pdf_loader dedups,
    and may also apply again in a later campaign — without the campaign in the
    key, that re-application would overwrite the earlier grade, destroying the
    history a reviewer wants when the embargo flags them as a re-applicant.
"""

import datetime as dt
import hashlib
import json
import os
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple

from text_extractors import EXTRACTOR_VERSION


# ============================================================
# HASHING
# ============================================================
def prompt_hash(prompt_text: str) -> str:
    """sha256 of the grading prompt. Any rubric edit changes this."""
    return hashlib.sha256(prompt_text.encode("utf-8")).hexdigest()


def fingerprint(prompt_text: str, model: str) -> str:
    """sha256 of the grading prompt plus the model that graded with it.

    Stored in the cache's `prompt_sha256` field. Keyed on both because a grade
    is only reusable when the rubric AND the model behind it are unchanged.
    """
    return hashlib.sha256(f"{model}\n{prompt_text}".encode("utf-8")).hexdigest()


def rubric_version(prompt_text: str) -> str:
    """The version label from the prompt's `## Version` section, e.g. 'v9.3'.

    Stored on each cache entry so the report can say which rubric judged a row
    without re-reading (or having to keep) the prompt that produced it. Returns
    '' when the prompt has no recognisable version line — a missing label must
    never stop a run.
    """
    lines = prompt_text.splitlines()
    for i, line in enumerate(lines):
        if line.strip().lower().startswith("## version"):
            for candidate in lines[i + 1:]:
                token = candidate.strip().split(" ", 1)[0].strip()
                if token:
                    return token
            break
    return ""


def essay_hash(essay_text: str) -> str:
    """sha256 of an essay's text, normalised so trivial whitespace-only
    differences (trailing newline from a re-export) don't force a regrade."""
    normalised = "\n".join(line.rstrip() for line in essay_text.strip().splitlines())
    return hashlib.sha256(normalised.encode("utf-8")).hexdigest()


def _cache_key(campaign: str, candidate_number: str, role: str) -> str:
    """JSON-safe key for one graded submission: campaign, candidate and role.

    The campaign is part of the identity because a candidate may apply in more
    than one of them. Without it, an FY27 re-application for a role the
    candidate already applied for in FY26 lands on the same key and overwrites
    last year's grade — destroying exactly the history a reviewer wants when
    the embargo flags that candidate as a re-applicant.
    """
    return f"{campaign or LEGACY_CAMPAIGN}|{candidate_number}|{role}"


# The campaign that grades predate the `campaign` field belong to. Every grade
# written before it existed was produced during FY26, so backfilling to this is
# exact rather than a guess.
LEGACY_CAMPAIGN = "FY26"


# ============================================================
# CLASSIFICATION REASONS
# ============================================================
# Why an essay will or won't be graded this run. Reported by --dry-run so the
# cost of a run is visible before it is incurred.
REASON_NEW = "new"                       # no cache entry
REASON_CHANGED = "changed"               # the submitted file itself differs
REASON_REEXTRACTED = "reextracted"       # same file, EXTRACTOR_VERSION bumped
REASON_STALE_RUBRIC = "stale_rubric"     # graded under an older rubric/model
REASON_CURRENT = "current"               # already graded under this rubric
REASON_OUT_OF_SCOPE = "out_of_scope"     # stale, but its role isn't in this run

# The reasons that cost an API call.
GRADE_REASONS = (
    REASON_NEW,
    REASON_CHANGED,
    REASON_REEXTRACTED,
    REASON_STALE_RUBRIC,
)

# The subset that is genuinely *new work* — a submission with no usable grade —
# as opposed to regrading something already graded because the rubric moved.
# The distinction is what "incremental" means, and what the run confirmation
# offers to take on its own.
INCREMENTAL_REASONS = (
    REASON_NEW,
    REASON_CHANGED,
    REASON_REEXTRACTED,
)

# Human-readable labels for the dry-run report.
REASON_LABELS = {
    REASON_NEW: "new submission",
    REASON_CHANGED: "candidate resubmitted (file changed)",
    REASON_REEXTRACTED: "extractor upgraded (same file, re-read)",
    REASON_STALE_RUBRIC: "older rubric",
    REASON_CURRENT: "already current",
    REASON_OUT_OF_SCOPE: "older rubric, role not in scope",
}


# ============================================================
# LOAD / SAVE
# ============================================================
def _empty_cache() -> Dict:
    return {"prompt_sha256": "", "candidates": {}}


def load_cache(path: str) -> Dict:
    """Loads the cache, returning an empty skeleton if it's missing or corrupt.

    A corrupt cache is treated as no cache (everything regrades) rather than
    crashing the run — the worst case is one wasted full run, not a lost report.
    """
    cache_file = Path(path)
    if not cache_file.exists():
        return _empty_cache()
    try:
        data = json.loads(cache_file.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError):
        return _empty_cache()
    if not isinstance(data, dict) or "candidates" not in data:
        return _empty_cache()
    data.setdefault("prompt_sha256", "")
    if not isinstance(data["candidates"], dict):
        data["candidates"] = {}
    # Migration: caches written before the hash moved onto each entry carry it
    # only at the top level. Backfill it so those grades stay reusable instead
    # of forcing an unnecessary full regrade on first run of the new code.
    for entry in data["candidates"].values():
        if isinstance(entry, dict):
            entry.setdefault("prompt_sha256", data["prompt_sha256"])
    data["candidates"] = _migrate_keys(data["candidates"])
    return data


def _migrate_keys(candidates: Dict) -> Dict:
    """Rewrites pre-campaign 'number|role' keys as 'campaign|number|role'.

    Keys written before the campaign joined them identify only a candidate and
    a role, so a later campaign's submission would overwrite them. The campaign
    is taken from the entry's own `campaign` field via campaign_of(), which
    backfills to FY26 — exact for every grade written so far, since FY26 is the
    only campaign there has been.

    Detection is by shape, not by looking for 'FY': a key with two fields is
    old, three is current. That way a role containing the separator, or a
    campaign label that stops starting with FY, cannot cause a second migration
    of an already-migrated key.
    """
    migrated = {}
    for key, entry in candidates.items():
        if isinstance(entry, dict) and key.count("|") == 1:
            key = f"{campaign_of(entry)}|{key}"
        migrated[key] = entry
    return migrated


def save_cache(path: str, cache: Dict) -> None:
    """Atomically writes the cache (temp file + os.replace) so an interrupted
    write can never leave a truncated, unreadable cache behind."""
    cache_file = Path(path)
    cache_file.parent.mkdir(parents=True, exist_ok=True)
    tmp = cache_file.with_suffix(cache_file.suffix + ".tmp")
    tmp.write_text(json.dumps(cache, ensure_ascii=False, indent=2), encoding="utf-8")
    os.replace(tmp, cache_file)


def campaign_of(entry: Dict) -> str:
    """The campaign an entry belongs to, defaulting for pre-campaign entries.

    Grades written before campaigns existed all belong to FY26 — the campaign
    live when the field was introduced — so this is an exact backfill, not a
    guess. Keeping the default in one place stops it drifting.
    """
    return entry.get("campaign") or LEGACY_CAMPAIGN


def _entry(
    essay: Dict,
    result: Dict,
    prompt_sha256: str,
    version: str = "",
    campaign: str = "",
    graded_at: str = "",
) -> Dict:
    """The stored shape of one cached grade.

    prompt_sha256 is the fingerprint the grade was actually produced under —
    not necessarily the current run's. A role-scoped run leaves out-of-scope
    entries on their original hash so the cache stays honest about what is
    current and what is stale.
    """
    return {
        "candidate_number": essay["candidate_number"],
        "role": essay["role"],
        "source_file": essay.get("source_file", ""),
        # The submission's identity: the file itself. essay_sha256 is kept
        # alongside it as a tripwire for extraction drift, not as a key.
        "file_sha256": essay.get("file_sha256", ""),
        "essay_sha256": essay_hash(essay["essay_text"]),
        "extractor_version": EXTRACTOR_VERSION,
        "essay_text": essay["essay_text"],
        "prompt_sha256": prompt_sha256,
        "rubric_version": version,
        # Which recruitment campaign this grade belongs to, and when it was
        # produced. Recorded at grade time because neither can be recovered
        # afterwards — submission files carry no usable date.
        "campaign": campaign or LEGACY_CAMPAIGN,
        "graded_at": graded_at or dt.date.today().isoformat(),
        "result": result,
    }


def record_grade(
    cache: Dict,
    essay: Dict,
    result: Dict,
    current_prompt_hash: str,
    version: str = "",
    campaign: str = "",
) -> None:
    """Records one freshly graded essay so it survives an interrupted run.

    The caller saves the cache afterwards; `save_cache` is atomic, so being
    killed mid-write cannot corrupt what was already stored.
    """
    cache["prompt_sha256"] = current_prompt_hash
    key = _cache_key(campaign, essay["candidate_number"], essay["role"])
    cache["candidates"][key] = _entry(
        essay, result, current_prompt_hash, version, campaign
    )


# ============================================================
# PARTITION / MERGE
# ============================================================
def stale_keys(
    cache: Dict, current_prompt_hash: str, campaign: str = ""
) -> List[str]:
    """Cache keys whose grade was produced under an older rubric or model.

    Scoped to one campaign when given. Counting every campaign would report
    last year's grades as work this run left undone, which is neither true nor
    actionable — they are out of scope by design.
    """
    return [
        key
        for key, entry in cache.get("candidates", {}).items()
        if entry.get("prompt_sha256") != current_prompt_hash
        and (not campaign or campaign_of(entry) == campaign)
    ]


def classify(
    essays: List[Dict],
    cache: Dict,
    current_prompt_hash: str,
    roles: Optional[Set[str]] = None,
    campaign: str = "",
) -> List[Tuple[Dict, str]]:
    """Decides what happens to each essay, and why, in folder order.

    This is the single source of truth for reuse. `partition()` is built on it
    rather than repeating the rules, so a `--dry-run` report can never disagree
    with what a real run would actually do.

    A submission's identity is its **file bytes**, not the text we read out of
    it. That distinction is the whole point: if the PDF is unchanged then the
    candidate did not resubmit, however our extraction code may have changed in
    the meantime. Re-reading files is instead opted into deliberately by bumping
    text_extractors.EXTRACTOR_VERSION.

    Reasons:

      REASON_NEW           no cache entry at all
      REASON_CHANGED       the submitted file itself differs — a resubmission
      REASON_REEXTRACTED   same file, but EXTRACTOR_VERSION was bumped, so we
                           deliberately want it re-read and regraded
      REASON_STALE_RUBRIC  valid grade under an older rubric/model, role in scope
      REASON_CURRENT       valid grade under the current rubric — reuse
      REASON_OUT_OF_SCOPE  stale grade whose role is excluded this run — reuse

    `roles` scopes *regrading*, never new work. NEW, CHANGED and REEXTRACTED are
    decided before the role filter is consulted: none has a usable grade to
    keep, and skipping an out-of-scope one would leave a hole in the report.
    Only STALE_RUBRIC is gated by scope. roles=None means every stale grade is
    in scope, which is the whole-corpus behaviour.

    Lookups are scoped to `campaign`, so a candidate returning in a new campaign
    is NEW rather than CHANGED. Both grade, so the cost is identical; "new" is
    simply the truer word for a fresh campaign's submission, and it is what
    keeps last year's grade from being treated as this year's starting point.
    """
    candidates = cache["candidates"]
    classified: List[Tuple[Dict, str]] = []

    for essay in essays:
        entry = candidates.get(
            _cache_key(campaign, essay["candidate_number"], essay["role"])
        )

        if not entry:
            reason = REASON_NEW
        elif _file_changed(entry, essay):
            reason = REASON_CHANGED
        elif entry.get("extractor_version", EXTRACTOR_VERSION) != EXTRACTOR_VERSION:
            reason = REASON_REEXTRACTED
        elif entry.get("prompt_sha256") == current_prompt_hash:
            reason = REASON_CURRENT
        elif roles is None or essay["role"] in roles:
            reason = REASON_STALE_RUBRIC
        else:
            reason = REASON_OUT_OF_SCOPE

        classified.append((essay, reason))

    return classified


def _file_changed(entry: Dict, essay: Dict) -> bool:
    """True when the candidate submitted a different file.

    Compares raw file bytes where both sides have them. Entries written before
    the file hash existed, and files we could not read, fall back to comparing
    extracted text — the old behaviour, so no cache is invalidated by the
    upgrade itself.
    """
    cached_file = entry.get("file_sha256")
    current_file = essay.get("file_sha256")
    if cached_file and current_file:
        return cached_file != current_file
    return entry.get("essay_sha256") != essay_hash(essay.get("essay_text", ""))


def stale_extractions(
    essays: List[Dict], cache: Dict, campaign: str = ""
) -> List[Dict]:
    """Essays whose extracted text drifted without EXTRACTOR_VERSION changing.

    The file is the same and the extractor version is the same, yet the text
    differs from what produced the cached grade — so extraction behaviour
    changed and nobody bumped the version. Those grades are still reused (that
    is the point of keying on the file), but they were derived from different
    text, so the run must say so rather than pass silently.

    This is what keeps `essay_sha256` earning its place: it is no longer an
    identity key, it is the tripwire for exactly this mistake.
    """
    candidates = cache.get("candidates", {})
    drifted = []
    for essay in essays:
        entry = candidates.get(
            _cache_key(campaign, essay["candidate_number"], essay["role"])
        )
        if not entry:
            continue
        if _file_changed(entry, essay):
            continue  # a real resubmission, already regrading
        if entry.get("extractor_version", EXTRACTOR_VERSION) != EXTRACTOR_VERSION:
            continue  # deliberate re-extraction, already regrading
        if entry.get("essay_sha256") != essay_hash(essay.get("essay_text", "")):
            drifted.append(essay)
    return drifted


def partition(
    essays: List[Dict],
    cache: Dict,
    current_prompt_hash: str,
    roles: Optional[Set[str]] = None,
    campaign: str = "",
) -> Tuple[List[Dict], List[Dict]]:
    """Splits the loaded essays into (to_grade, reused_results).

    A thin view over `classify()` — see there for the rules. Both lists stay in
    folder order, which callers rely on: main.py zips `to_grade` against the
    grading results, so the two must remain aligned.
    """
    candidates = cache["candidates"]
    to_grade: List[Dict] = []
    reused: List[Dict] = []

    for essay, reason in classify(
        essays, cache, current_prompt_hash, roles, campaign
    ):
        if reason in GRADE_REASONS:
            to_grade.append(essay)
        else:
            key = _cache_key(campaign, essay["candidate_number"], essay["role"])
            reused.append(candidates[key]["result"])

    return to_grade, reused


def merge_and_update(
    cache: Dict,
    essays: List[Dict],
    graded: List[Tuple[Dict, Dict]],
    current_prompt_hash: str,
    version: str = "",
    campaign: str = "",
) -> Tuple[List[Dict], List[Dict]]:
    """Folds freshly graded results into the cache and returns the current set.

    Parameters
    ----------
    cache : the cache dict to mutate in place (caller saves it afterwards).
    essays : every essay loaded from the folder this run.
    graded : (essay, result) pairs for the essays graded this run.
    current_prompt_hash : hash of the rubric used for this run.
    campaign : the recruitment campaign to report on. Entries from earlier
        campaigns stay in the cache but are left out of both returned lists —
        each campaign starts from scratch, and cache-only entries are exactly
        how last year's candidates would otherwise haunt this year's report.
        Empty means no filtering (every campaign included).

    Returns
    -------
    (all_results, plagiarism_essays)
        all_results       : one result per candidate in the campaign — folder
                            essays first (in load order), then any cache-only
                            entries whose PDF is no longer in the folder.
        plagiarism_essays : matching essay dicts (with text) for the same set,
                            so the screen compares within the campaign only.
    """
    cache["prompt_sha256"] = current_prompt_hash
    candidates = cache["candidates"]

    graded_by_key = {
        _cache_key(campaign, essay["candidate_number"], essay["role"]): (essay, result)
        for essay, result in graded
    }

    # Write/refresh a cache entry for every essay currently in the folder.
    for essay in essays:
        key = _cache_key(campaign, essay["candidate_number"], essay["role"])
        if key in graded_by_key:
            result = graded_by_key[key][1]
            entry_hash, entry_version = current_prompt_hash, version
            # A grade produced this run belongs to this run's campaign.
            entry_campaign = campaign or LEGACY_CAMPAIGN
            entry_graded_at = ""  # _entry stamps today
        elif key in candidates:
            result = candidates[key]["result"]  # reused unchanged
            # Keep the hash this grade was actually produced under. A
            # role-scoped run must not silently promote out-of-scope rows to
            # the current rubric — that is exactly the lie the per-entry hash
            # exists to prevent. The campaign and date are the grade's
            # provenance too, so they are preserved for the same reason.
            entry_hash = candidates[key].get("prompt_sha256", "")
            entry_version = candidates[key].get("rubric_version", "")
            entry_campaign = campaign_of(candidates[key])
            entry_graded_at = candidates[key].get("graded_at", "")
        else:
            continue  # shouldn't happen: every folder essay is graded or cached

        # The cached grade stays valid when the essay text is unchanged, but
        # the file carrying it may have been resubmitted in a different
        # format. The label describes the file, not the grade, so always take
        # it from this run's load rather than the cache.
        if "format_label" in essay:
            result = {**result, "format_label": essay["format_label"]}

        candidates[key] = _entry(
            essay, result, entry_hash, entry_version,
            entry_campaign, entry_graded_at,
        )

    folder_keys = [
        _cache_key(campaign, e["candidate_number"], e["role"]) for e in essays
    ]
    # An essay in the folder with no cache entry has no grade to report. That
    # happens on a --report-only rebuild when a new PDF has been added but not
    # yet graded; without this it would raise a KeyError below.
    folder_keys = [k for k in folder_keys if k in candidates]
    folder_key_set = set(folder_keys)
    # Cache-only entries: previously graded candidates whose PDF was removed.
    extra_keys = [k for k in candidates if k not in folder_key_set]

    # Each campaign starts from scratch. Earlier campaigns stay in the cache —
    # nothing is deleted, and --fy can still report on them — but they are left
    # out of this run's results and out of the plagiarism corpus.
    if campaign:
        folder_keys = [k for k in folder_keys if campaign_of(candidates[k]) == campaign]
        extra_keys = [k for k in extra_keys if campaign_of(candidates[k]) == campaign]

    all_results: List[Dict] = []
    plagiarism_essays: List[Dict] = []
    for key in folder_keys + extra_keys:
        entry = candidates[key]
        # Surface the rubric that actually produced this grade so the report
        # can show a scoped run's older rows for what they are.
        all_results.append({
            **entry["result"],
            "rubric_version": entry.get("rubric_version", ""),
            "rubric_is_current": entry.get("prompt_sha256") == current_prompt_hash,
            "campaign": campaign_of(entry),
        })
        plagiarism_essays.append({
            "candidate_number": entry["candidate_number"],
            "role": entry["role"],
            "essay_text": entry["essay_text"],
            "source_file": entry.get("source_file", ""),
        })

    return all_results, plagiarism_essays
