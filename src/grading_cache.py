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
  - Cache entries are keyed by (candidate_number, role): a candidate may
    legitimately apply for more than one role, exactly as pdf_loader dedups.
"""

import hashlib
import json
import os
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple


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


def _cache_key(candidate_number: str, role: str) -> str:
    """JSON-safe key for the (candidate_number, role) pair."""
    return f"{candidate_number}|{role}"


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
    return data


def save_cache(path: str, cache: Dict) -> None:
    """Atomically writes the cache (temp file + os.replace) so an interrupted
    write can never leave a truncated, unreadable cache behind."""
    cache_file = Path(path)
    cache_file.parent.mkdir(parents=True, exist_ok=True)
    tmp = cache_file.with_suffix(cache_file.suffix + ".tmp")
    tmp.write_text(json.dumps(cache, ensure_ascii=False, indent=2), encoding="utf-8")
    os.replace(tmp, cache_file)


def _entry(
    essay: Dict, result: Dict, prompt_sha256: str, version: str = ""
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
        "essay_sha256": essay_hash(essay["essay_text"]),
        "essay_text": essay["essay_text"],
        "prompt_sha256": prompt_sha256,
        "rubric_version": version,
        "result": result,
    }


def record_grade(
    cache: Dict,
    essay: Dict,
    result: Dict,
    current_prompt_hash: str,
    version: str = "",
) -> None:
    """Records one freshly graded essay so it survives an interrupted run.

    The caller saves the cache afterwards; `save_cache` is atomic, so being
    killed mid-write cannot corrupt what was already stored.
    """
    cache["prompt_sha256"] = current_prompt_hash
    cache["candidates"][_cache_key(essay["candidate_number"], essay["role"])] = _entry(
        essay, result, current_prompt_hash, version
    )


# ============================================================
# PARTITION / MERGE
# ============================================================
def stale_keys(cache: Dict, current_prompt_hash: str) -> List[str]:
    """Cache keys whose grade was produced under an older rubric or model."""
    return [
        key
        for key, entry in cache.get("candidates", {}).items()
        if entry.get("prompt_sha256") != current_prompt_hash
    ]


def partition(
    essays: List[Dict],
    cache: Dict,
    current_prompt_hash: str,
    roles: Optional[Set[str]] = None,
) -> Tuple[List[Dict], List[Dict]]:
    """Splits the loaded essays into (to_grade, reused_results).

    An essay is reused when a cache entry exists for its (candidate, role), the
    essay text hash matches, AND the entry was graded under the current rubric
    and model.

    `roles` scopes *regrading*, never new work:

      - An essay with no cache entry, or whose text has changed, is **always**
        graded whatever its role. It has no usable grade, and skipping a new
        out-of-scope submission would leave a hole in the report. This is the
        incremental path and the role filter must never suppress it.
      - An essay whose grade is stale only because the rubric or model changed
        is regraded only when its role is in `roles`. Otherwise its existing
        grade is reused as-is, keeping the hash it was graded under.

    Passing roles=None (the default) means every stale grade is in scope, which
    is the original whole-corpus behaviour.
    """
    candidates = cache["candidates"]
    to_grade: List[Dict] = []
    reused: List[Dict] = []

    for essay in essays:
        key = _cache_key(essay["candidate_number"], essay["role"])
        entry = candidates.get(key)

        if not entry or entry.get("essay_sha256") != essay_hash(essay["essay_text"]):
            to_grade.append(essay)  # new or edited — always graded
            continue

        if entry.get("prompt_sha256") == current_prompt_hash:
            reused.append(entry["result"])  # current, nothing to do
        elif roles is None or essay["role"] in roles:
            to_grade.append(essay)  # stale and in scope — regrade
        else:
            reused.append(entry["result"])  # stale but out of scope this run

    return to_grade, reused


def merge_and_update(
    cache: Dict,
    essays: List[Dict],
    graded: List[Tuple[Dict, Dict]],
    current_prompt_hash: str,
    version: str = "",
) -> Tuple[List[Dict], List[Dict]]:
    """Folds freshly graded results into the cache and returns the full set.

    Parameters
    ----------
    cache : the cache dict to mutate in place (caller saves it afterwards).
    essays : every essay loaded from the folder this run.
    graded : (essay, result) pairs for the essays graded this run.
    current_prompt_hash : hash of the rubric used for this run.

    Returns
    -------
    (all_results, plagiarism_essays)
        all_results       : one result per candidate — folder essays first (in
                            load order), then any cache-only entries whose PDF
                            is no longer in the folder.
        plagiarism_essays : matching essay dicts (with text) for the same set,
                            so the plagiarism screen sees the full corpus.
    """
    cache["prompt_sha256"] = current_prompt_hash
    candidates = cache["candidates"]

    graded_by_key = {
        _cache_key(essay["candidate_number"], essay["role"]): (essay, result)
        for essay, result in graded
    }

    # Write/refresh a cache entry for every essay currently in the folder.
    for essay in essays:
        key = _cache_key(essay["candidate_number"], essay["role"])
        if key in graded_by_key:
            result = graded_by_key[key][1]
            entry_hash, entry_version = current_prompt_hash, version
        elif key in candidates:
            result = candidates[key]["result"]  # reused unchanged
            # Keep the hash this grade was actually produced under. A
            # role-scoped run must not silently promote out-of-scope rows to
            # the current rubric — that is exactly the lie the per-entry hash
            # exists to prevent.
            entry_hash = candidates[key].get("prompt_sha256", "")
            entry_version = candidates[key].get("rubric_version", "")
        else:
            continue  # shouldn't happen: every folder essay is graded or cached

        # The cached grade stays valid when the essay text is unchanged, but
        # the file carrying it may have been resubmitted in a different
        # format. The label describes the file, not the grade, so always take
        # it from this run's load rather than the cache.
        if "format_label" in essay:
            result = {**result, "format_label": essay["format_label"]}

        candidates[key] = _entry(essay, result, entry_hash, entry_version)

    folder_keys = [
        _cache_key(e["candidate_number"], e["role"]) for e in essays
    ]
    folder_key_set = set(folder_keys)
    # Cache-only entries: previously graded candidates whose PDF was removed.
    extra_keys = [k for k in candidates if k not in folder_key_set]

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
        })
        plagiarism_essays.append({
            "candidate_number": entry["candidate_number"],
            "role": entry["role"],
            "essay_text": entry["essay_text"],
            "source_file": entry.get("source_file", ""),
        })

    return all_results, plagiarism_essays
