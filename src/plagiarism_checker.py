"""Pairwise plagiarism screening across essays.

Strategy (hybrid, cheap-first):
  1. LEXICAL SCREEN (pure Python, no API cost) — every pair of essays is
     compared with two signals:
       - Jaccard overlap of 5-word shingles  → catches copy-paste / light edits
       - TF-IDF cosine similarity            → catches shared unusual vocabulary
                                               even with reordered sentences
  2. CLAUDE VERDICT (API, flagged pairs only) — pairs that cross either
     screening threshold are sent to Claude, which judges whether the
     similarity indicates copying / a shared source or is just two candidates
     answering the same prompt, and quotes the shared evidence.

Only flagged pairs incur API cost, so the expensive step stays O(flagged)
rather than O(n²).

Pairs are always reported in canonical order (lower candidate number first).
"""

import json
import math
import re
import time
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import anthropic

from essay_grader import (
    DEFAULT_MODEL,
    RETRYABLE_EXCEPTIONS,
    MAX_RETRIES,
    _build_client,
    _extract_first_json_object,
)


# ============================================================
# CONFIG
# ============================================================
# Word-shingle size for the lexical overlap metric. 5-word sequences are
# long enough that independent authors essentially never share them except
# through question wording or copying.
SHINGLE_SIZE = 5

# Screening thresholds — a pair crossing EITHER goes to Claude for a verdict.
# Calibrated against the real 2026 campaign corpus (68 essays, 2,278 pairs):
# shared aviation jargon and identical questions push the TF-IDF cosine
# baseline to ~75% between innocent same-role essays, while their shingle
# overlap never exceeds 5%. These values sit just above those baselines;
# lower settings flooded Claude with hundreds of coincidental pairs.
LEXICAL_SCREEN_THRESHOLD = 0.10   # Jaccard of shingles (0..1)
SEMANTIC_SCREEN_THRESHOLD = 0.75  # TF-IDF cosine (0..1)

# Verbatim overlap this high is treated as High risk even if Claude equivocates.
LEXICAL_HIGH_THRESHOLD = 0.20

# Claude verdict → risk band.
_VERDICT_RISK = {
    "confirmed": "High",
    "suspicious": "Medium",
    "likely_coincidence": "Low",
}

# Output cap for the pair-review call — the verdict JSON is short.
PAIR_REVIEW_MAX_TOKENS = 1500

# How many corrective re-asks we allow when Claude returns invalid JSON.
# Plain retries are pointless at temperature 0 (same input → same output),
# so each re-ask feeds the invalid output and parse error back to Claude.
PARSE_FIX_ATTEMPTS = 2

# The pair-review prompt lives next to the grading prompt in config/.
BASE_DIR = Path(__file__).resolve().parent
PROMPT_FILE = BASE_DIR.parent / "config" / "plagiarism_prompt.txt"

# Flag text shown in the Summary sheet for essays that could not be screened.
NOT_SCREENED_FLAG = "not screened (empty essay)"


# ============================================================
# PUBLIC API
# ============================================================
def check_plagiarism(
    essays: List[Dict],
    client: Optional[anthropic.Anthropic] = None,
    model: str = DEFAULT_MODEL,
) -> List[Dict]:
    """Screens every pair of essays and returns one dict per flagged pair.

    Each returned pair dict has:
        - candidate_a / candidate_b : str (canonical order, lower number first)
        - lexical_pct               : float (shingle Jaccard × 100)
        - semantic_pct              : float (TF-IDF cosine × 100)
        - risk                      : "High" | "Medium" | "Low"
        - claude_verdict            : "confirmed" | "suspicious" | "likely_coincidence"
        - claude_explanation        : str
        - shared_evidence           : List[str]

    Pairs below both screening thresholds are omitted entirely — baseline
    similarity between essays answering the same prompt is not evidence.
    The Anthropic client is only built (and the prompt only loaded) when at
    least one pair needs a Claude verdict.
    """
    screened = [e for e in essays if (e.get("essay_text") or "").strip()]

    # Precompute per-essay features once; pairwise steps then reuse them.
    shingle_sets = [
        _shingles(_tokenize(e["essay_text"]), SHINGLE_SIZE) for e in screened
    ]
    tfidf_vectors = _tfidf_vectors([_tokenize(e["essay_text"]) for e in screened])

    flagged: List[Tuple[Dict, Dict, float, float]] = []
    for i in range(len(screened)):
        for j in range(i + 1, len(screened)):
            # A candidate may submit for two roles; comparing their own
            # submissions to each other is reuse, not plagiarism, and must
            # not flag or downgrade them.
            if screened[i]["candidate_number"] == screened[j]["candidate_number"]:
                continue
            lexical = _jaccard(shingle_sets[i], shingle_sets[j])
            semantic = _cosine(tfidf_vectors[i], tfidf_vectors[j])
            if lexical >= LEXICAL_SCREEN_THRESHOLD or semantic >= SEMANTIC_SCREEN_THRESHOLD:
                flagged.append((screened[i], screened[j], lexical, semantic))

    if not flagged:
        return []

    if client is None:
        client = _build_client()
    review_prompt = _load_pair_review_prompt()

    pair_results: List[Dict] = []
    for essay_a, essay_b, lexical, semantic in flagged:
        essay_a, essay_b = _canonical_pair(essay_a, essay_b)
        # ASCII-only: Windows consoles running cp1252 choke on arrow glyphs.
        print(
            f"  -> Reviewing pair {essay_a['candidate_number']} <-> "
            f"{essay_b['candidate_number']} "
            f"(lexical {lexical:.0%}, semantic {semantic:.0%})"
        )
        verdict = _claude_pair_verdict(
            client=client,
            model=model,
            system=review_prompt,
            essay_a=essay_a,
            essay_b=essay_b,
        )
        pair_results.append({
            "candidate_a": essay_a["candidate_number"],
            "candidate_b": essay_b["candidate_number"],
            "lexical_pct": round(lexical * 100, 1),
            "semantic_pct": round(semantic * 100, 1),
            "risk": _risk_band(verdict["verdict"], lexical),
            "claude_verdict": verdict["verdict"],
            "claude_explanation": verdict["explanation"],
            "shared_evidence": verdict["shared_evidence"],
        })

    return pair_results


def apply_plagiarism_overrides(results: List[Dict], pair_results: List[Dict]) -> None:
    """Annotates grading results in place with plagiarism flags and downgrades.

    - Every result gains a 'plagiarism_flag' key:
        ""                              → screened, no match (clean)
        "not screened (empty essay)"    → could not be screened
        "⚠ HIGH: ..." / "⚠ MEDIUM: ..." → flagged (one line per matched pair)
      Low-risk pairs (Claude judged the similarity coincidental) appear on the
      Similarity sheet only, not as a Summary flag.
    - Candidates in a High-risk pair have their classification downgraded to
      'Maybe' (unless already 'Maybe' or 'Do Not Interview' — plagiarism never
      upgrades a candidate), with the override recorded in the rationale.
    """
    flags: Dict[str, List[str]] = {}
    high_risk_partner: Dict[str, Dict] = {}

    for pair in pair_results:
        if pair["risk"] not in ("High", "Medium"):
            continue
        for cand, other in (
            (pair["candidate_a"], pair["candidate_b"]),
            (pair["candidate_b"], pair["candidate_a"]),
        ):
            flags.setdefault(cand, []).append(
                f"⚠ {pair['risk'].upper()}: lexical {pair['lexical_pct']:.0f}% / "
                f"semantic {pair['semantic_pct']:.0f}% with {other} — "
                f"Claude: {pair['claude_verdict']}"
            )
            if pair["risk"] == "High" and cand not in high_risk_partner:
                high_risk_partner[cand] = {"other": other, "pair": pair}

    for result in results:
        cand = result.get("candidate_number", "")

        if result.get("classification") == "error":
            result["plagiarism_flag"] = NOT_SCREENED_FLAG
            continue

        result["plagiarism_flag"] = "\n".join(flags.get(cand, []))

        if cand in high_risk_partner:
            _downgrade_classification(result, high_risk_partner[cand])


# ============================================================
# CLASSIFICATION OVERRIDE
# ============================================================
def _downgrade_classification(result: Dict, match: Dict) -> None:
    """Downgrades a high-plagiarism candidate to 'Maybe' and records why."""
    original = result.get("classification", "")

    # Never upgrade: 'Do Not Interview' stays; 'Maybe' is already there.
    if original == "Maybe" or "Do Not Interview" in original:
        return

    pair = match["pair"]
    result["classification"] = "Maybe"
    note = (
        f"[Plagiarism override] Classification downgraded from '{original}' to "
        f"'Maybe' due to high-similarity match with candidate {match['other']} "
        f"(lexical {pair['lexical_pct']:.0f}%, semantic {pair['semantic_pct']:.0f}%, "
        f"Claude verdict: {pair['claude_verdict']})."
    )
    rationale = result.get("rationale", "")
    result["rationale"] = f"{rationale}\n\n{note}".strip()


def _risk_band(claude_verdict: str, lexical: float) -> str:
    """Maps a Claude verdict (plus extreme verbatim overlap) to a risk band."""
    if lexical >= LEXICAL_HIGH_THRESHOLD:
        return "High"
    return _VERDICT_RISK.get(claude_verdict, "Medium")


# ============================================================
# LEXICAL METRICS (pure Python, no dependencies)
# ============================================================
def _tokenize(text: str) -> List[str]:
    """Lowercased word tokens; punctuation-insensitive so light edits
    (added commas, case changes) don't hide copying."""
    return re.findall(r"[a-z0-9']+", text.lower())


def _shingles(tokens: List[str], size: int) -> frozenset:
    """The set of all contiguous `size`-word sequences in the token stream."""
    if len(tokens) < size:
        return frozenset({" ".join(tokens)} if tokens else set())
    return frozenset(
        " ".join(tokens[i : i + size]) for i in range(len(tokens) - size + 1)
    )


def _jaccard(a: frozenset, b: frozenset) -> float:
    """Set overlap: |A ∩ B| / |A ∪ B|."""
    if not a or not b:
        return 0.0
    return len(a & b) / len(a | b)


def _tfidf_vectors(documents: List[List[str]]) -> List[Dict[str, float]]:
    """TF-IDF vector per document, with idf computed across the whole batch.

    Uses sklearn-style smoothing (idf = ln((1+N)/(1+df)) + 1) so terms present
    in every document still carry some weight — important for tiny batches.
    """
    n_docs = len(documents)
    doc_freq: Dict[str, int] = {}
    for tokens in documents:
        for term in set(tokens):
            doc_freq[term] = doc_freq.get(term, 0) + 1

    vectors: List[Dict[str, float]] = []
    for tokens in documents:
        counts: Dict[str, int] = {}
        for term in tokens:
            counts[term] = counts.get(term, 0) + 1
        total = len(tokens) or 1
        vectors.append({
            term: (count / total) * (math.log((1 + n_docs) / (1 + doc_freq[term])) + 1)
            for term, count in counts.items()
        })
    return vectors


def _cosine(a: Dict[str, float], b: Dict[str, float]) -> float:
    """Cosine similarity between two sparse vectors."""
    if not a or not b:
        return 0.0
    # Iterate the smaller vector for the dot product.
    if len(a) > len(b):
        a, b = b, a
    dot = sum(weight * b.get(term, 0.0) for term, weight in a.items())
    norm_a = math.sqrt(sum(w * w for w in a.values()))
    norm_b = math.sqrt(sum(w * w for w in b.values()))
    if norm_a == 0.0 or norm_b == 0.0:
        return 0.0
    return dot / (norm_a * norm_b)


# ============================================================
# CLAUDE PAIR REVIEW
# ============================================================
def _load_pair_review_prompt() -> str:
    """Loads the pair-review prompt at call time, mirroring essay_grader."""
    if not PROMPT_FILE.exists():
        raise FileNotFoundError(
            f"Plagiarism prompt not found: {PROMPT_FILE}. "
            f"Expected at <project_root>/config/plagiarism_prompt.txt"
        )
    return PROMPT_FILE.read_text(encoding="utf-8")


def _claude_pair_verdict(
    client: anthropic.Anthropic,
    model: str,
    system: str,
    essay_a: Dict,
    essay_b: Dict,
    max_retries: int = MAX_RETRIES,
) -> Dict:
    """Asks Claude for a plagiarism verdict on one pair of essays.

    Transient connection errors are retried with exponential backoff.
    Invalid JSON gets corrective re-asks (the invalid output and parse error
    are fed back to Claude); if that still fails, the pair falls back to a
    'suspicious — manual review required' verdict rather than aborting a run
    whose grading cost has already been spent.
    """
    pair_label = f"{essay_a['candidate_number']} <-> {essay_b['candidate_number']}"
    messages = [{
        "role": "user",
        "content": (
            f"=== Submission from candidate {essay_a['candidate_number']} "
            f"(role: {essay_a['role']}) ===\n"
            f"{essay_a['essay_text']}\n\n"
            f"=== Submission from candidate {essay_b['candidate_number']} "
            f"(role: {essay_b['role']}) ===\n"
            f"{essay_b['essay_text']}"
        ),
    }]

    last_parse_error = ""
    for parse_attempt in range(1 + PARSE_FIX_ATTEMPTS):
        raw_output = _stream_with_retry(
            client=client,
            model=model,
            system=system,
            messages=messages,
            pair_label=pair_label,
            max_retries=max_retries,
        )

        try:
            verdict = json.loads(_extract_first_json_object(raw_output))
        except (ValueError, json.JSONDecodeError) as exc:
            last_parse_error = str(exc)
            if parse_attempt < PARSE_FIX_ATTEMPTS:
                print(
                    f"     ! Invalid JSON verdict for pair {pair_label} "
                    f"({exc}); asking Claude to correct it..."
                )
                messages = messages + [
                    {"role": "assistant", "content": raw_output},
                    {
                        "role": "user",
                        "content": (
                            f"Your previous response was not valid JSON "
                            f"(parser error: {exc}). Re-send the same verdict "
                            f"as strictly valid JSON. Never put an unescaped "
                            f"double-quote character inside a string value; "
                            f"use single quotes for any phrases quoted from "
                            f"the essays. Output only the JSON object."
                        ),
                    },
                ]
            continue

        return {
            "verdict": verdict.get("verdict", "suspicious"),
            "explanation": verdict.get("explanation", ""),
            "shared_evidence": verdict.get("shared_evidence", []) or [],
        }

    # All attempts produced unparseable output. Flag for a human instead of
    # killing the pipeline; the lexical/semantic scores are still reported.
    print(
        f"     ! Could not obtain a valid JSON verdict for pair {pair_label} "
        f"after {1 + PARSE_FIX_ATTEMPTS} attempts; flagging for manual review."
    )
    return {
        "verdict": "suspicious",
        "explanation": (
            f"Automated verdict could not be parsed after "
            f"{1 + PARSE_FIX_ATTEMPTS} attempts (last error: "
            f"{last_parse_error}) — manual review required. "
            f"Similarity scores are unaffected."
        ),
        "shared_evidence": [],
    }


def _stream_with_retry(
    client: anthropic.Anthropic,
    model: str,
    system: str,
    messages: List[Dict],
    pair_label: str,
    max_retries: int,
) -> str:
    """Streams one completion, retrying transient connection errors."""
    for attempt in range(1, max_retries + 1):
        try:
            with client.messages.stream(
                model=model,
                max_tokens=PAIR_REVIEW_MAX_TOKENS,
                temperature=0,
                system=system,
                messages=messages,
            ) as stream:
                raw_output = "".join(stream.text_stream).strip()
                stop_reason = stream.get_final_message().stop_reason
            if stop_reason == "max_tokens":
                raise ValueError(
                    f"Plagiarism verdict for pair {pair_label} hit the "
                    f"{PAIR_REVIEW_MAX_TOKENS}-token output limit and was cut "
                    f"off. Increase PAIR_REVIEW_MAX_TOKENS in "
                    f"plagiarism_checker.py."
                )
            return raw_output
        except RETRYABLE_EXCEPTIONS as exc:
            if attempt < max_retries:
                wait = 2 ** attempt
                print(
                    f"     ⚠ {type(exc).__name__} (attempt {attempt}/{max_retries}); "
                    f"retrying in {wait}s..."
                )
                time.sleep(wait)
            else:
                raise
    raise AssertionError("unreachable")


# ============================================================
# HELPERS
# ============================================================
def _canonical_pair(essay_a: Dict, essay_b: Dict) -> Tuple[Dict, Dict]:
    """Orders a pair so the lower candidate number always comes first."""
    key_a = _numeric_key(essay_a["candidate_number"])
    key_b = _numeric_key(essay_b["candidate_number"])
    return (essay_a, essay_b) if key_a <= key_b else (essay_b, essay_a)


def _numeric_key(candidate_number: str):
    """Sorts numerically when possible, lexically otherwise."""
    try:
        return (0, int(candidate_number))
    except ValueError:
        return (1, candidate_number)
