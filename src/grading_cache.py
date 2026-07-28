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
    model is stored too (in `prompt_sha256`). If either changes, every cached
    grade is stale and the whole cache is discarded so everything is regraded —
    grades must never silently mix rubrics or models. One hiring report should
    never contain some candidates judged by a weaker model than others.
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
from typing import Dict, List, Tuple


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
    return data


def save_cache(path: str, cache: Dict) -> None:
    """Atomically writes the cache (temp file + os.replace) so an interrupted
    write can never leave a truncated, unreadable cache behind."""
    cache_file = Path(path)
    cache_file.parent.mkdir(parents=True, exist_ok=True)
    tmp = cache_file.with_suffix(cache_file.suffix + ".tmp")
    tmp.write_text(json.dumps(cache, ensure_ascii=False, indent=2), encoding="utf-8")
    os.replace(tmp, cache_file)


def _entry(essay: Dict, result: Dict) -> Dict:
    """The stored shape of one cached grade."""
    return {
        "candidate_number": essay["candidate_number"],
        "role": essay["role"],
        "source_file": essay.get("source_file", ""),
        "essay_sha256": essay_hash(essay["essay_text"]),
        "essay_text": essay["essay_text"],
        "result": result,
    }


def record_grade(
    cache: Dict, essay: Dict, result: Dict, current_prompt_hash: str
) -> None:
    """Records one freshly graded essay so it survives an interrupted run.

    The caller saves the cache afterwards; `save_cache` is atomic, so being
    killed mid-write cannot corrupt what was already stored.
    """
    cache["prompt_sha256"] = current_prompt_hash
    cache["candidates"][_cache_key(essay["candidate_number"], essay["role"])] = _entry(
        essay, result
    )


# ============================================================
# PARTITION / MERGE
# ============================================================
def partition(
    essays: List[Dict], cache: Dict, current_prompt_hash: str
) -> Tuple[List[Dict], List[Dict]]:
    """Splits the loaded essays into (to_grade, reused_results).

    An essay is reused only when the prompt is unchanged AND a cache entry
    exists for its (candidate, role) AND the essay text hash matches. When the
    prompt hash differs, the whole cache is stale and every essay is regraded.
    """
    if cache.get("prompt_sha256") != current_prompt_hash:
        return list(essays), []

    candidates = cache["candidates"]
    to_grade: List[Dict] = []
    reused: List[Dict] = []

    for essay in essays:
        key = _cache_key(essay["candidate_number"], essay["role"])
        entry = candidates.get(key)
        if entry and entry.get("essay_sha256") == essay_hash(essay["essay_text"]):
            reused.append(entry["result"])
        else:
            to_grade.append(essay)

    return to_grade, reused


def merge_and_update(
    cache: Dict,
    essays: List[Dict],
    graded: List[Tuple[Dict, Dict]],
    current_prompt_hash: str,
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
        elif key in candidates:
            result = candidates[key]["result"]  # reused unchanged
        else:
            continue  # shouldn't happen: every folder essay is graded or cached

        # The cached grade stays valid when the essay text is unchanged, but
        # the file carrying it may have been resubmitted in a different
        # format. The label describes the file, not the grade, so always take
        # it from this run's load rather than the cache.
        if "format_label" in essay:
            result = {**result, "format_label": essay["format_label"]}

        candidates[key] = _entry(essay, result)

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
        all_results.append(entry["result"])
        plagiarism_essays.append({
            "candidate_number": entry["candidate_number"],
            "role": entry["role"],
            "essay_text": entry["essay_text"],
            "source_file": entry.get("source_file", ""),
        })

    return all_results, plagiarism_essays
