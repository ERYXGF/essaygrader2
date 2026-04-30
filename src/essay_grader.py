"""Sends each essay to Claude for grading and returns structured results."""
 
import os
import json
from pathlib import Path
from typing import List, Dict
 
from dotenv import load_dotenv
import anthropic
 
 
# ============================================================
# CONFIG
# ============================================================
load_dotenv()
 
# Current production Haiku model (April 2026). Anthropic retired
# claude-3-haiku-20240307 in February 2026.
DEFAULT_MODEL = "claude-haiku-4-5-20251001"
 
# The grading prompt lives at <project_root>/config/essay_prompt.txt.
# This file is in <project_root>/src, so we go up one level then into config.
BASE_DIR = Path(__file__).resolve().parent
PROMPT_FILE = BASE_DIR.parent / "config" / "essay_prompt.txt"
 
 
# ============================================================
# LAZY INITIALISATION
# ============================================================
def _load_grading_prompt() -> str:
    """Loads the grading prompt at call time (not import time) so the module
    can be imported even when the prompt file is temporarily missing."""
    if not PROMPT_FILE.exists():
        raise FileNotFoundError(
            f"Grading prompt not found: {PROMPT_FILE}. "
            f"Expected at <project_root>/config/essay_prompt.txt"
        )
    return PROMPT_FILE.read_text(encoding="utf-8")
 
 
def _build_client() -> anthropic.Anthropic:
    """Builds an Anthropic client. Raises a clear error if no key is set —
    failing here is much better than failing inside the per-essay loop."""
    api_key = os.getenv("ANTHROPIC_API_KEY")
    if not api_key:
        raise RuntimeError(
            "ANTHROPIC_API_KEY not found. Add it to your .env file at the "
            "project root (or export it in your shell) before running the "
            "pipeline."
        )
    return anthropic.Anthropic(api_key=api_key)
 
 
# ============================================================
# PER-ESSAY GRADING
# ============================================================
def grade_essay(
    essay_text: str,
    candidate_number: str,
    role: str,
    client: anthropic.Anthropic,
    grading_prompt: str,
    model: str = DEFAULT_MODEL,
) -> Dict:
    """Sends a single essay to Claude and returns the parsed result.
 
    Fail-fast policy
    ----------------
    Any API error or invalid JSON response raises and stops the run.
    Empty essays (data condition, not system failure) return a normal
    row flagged with classification='error'.
    """
    if not essay_text or not essay_text.strip():
        return _empty_essay_result(candidate_number, role)
 
    response = client.messages.create(
        model=model,
        max_tokens=1500,
        temperature=0,
        system=grading_prompt,
        messages=[
            {
                "role": "user",
                "content": (
                    f"Candidate number: {candidate_number}\n"
                    f"Role: {role}\n\n"
                    f"Essay:\n{essay_text}"
                ),
            }
        ],
    )
 
    raw_output = response.content[0].text.strip()
    cleaned = _strip_code_fences(raw_output)
 
    try:
        result = json.loads(cleaned)
    except json.JSONDecodeError as exc:
        raise ValueError(
            f"Claude returned invalid JSON for candidate {candidate_number}.\n"
            f"Raw response:\n{raw_output}"
        ) from exc
 
    return _normalise_result(result, candidate_number, role)
 
 
# ============================================================
# BATCH ENTRY POINT
# ============================================================
def grade_essays(essays: List[Dict], model: str = DEFAULT_MODEL) -> List[Dict]:
    """Grades every essay in the list.
 
    Parameters
    ----------
    essays : list of dicts shaped like the output of pdf_loader.load_essays:
             {"candidate_number", "role", "essay_text", "source_file"}
    model  : Anthropic model string (default: current Haiku)
 
    Returns
    -------
    list of result dicts, one per input essay.
    """
    if not essays:
        raise ValueError("No essays provided to grade.")
 
    client = _build_client()
    grading_prompt = _load_grading_prompt()
 
    results: List[Dict] = []
    total = len(essays)
 
    for idx, essay in enumerate(essays, start=1):
        candidate_number = essay["candidate_number"]
        role = essay["role"]
        essay_text = essay["essay_text"]
 
        print(f"  → Grading {idx}/{total} (candidate {candidate_number}, role {role})")
 
        result = grade_essay(
            essay_text=essay_text,
            candidate_number=candidate_number,
            role=role,
            client=client,
            grading_prompt=grading_prompt,
            model=model,
        )
 
        # Carry the source filename through so the report can reference it
        # if anything looks off.
        result["source_file"] = essay.get("source_file", "")
        results.append(result)
 
    return results
 
 
# ============================================================
# RESULT SHAPING
# ============================================================
def _empty_essay_result(candidate_number: str, role: str) -> Dict:
    """Standard result row for empty submissions."""
    return {
        "candidate_number": candidate_number,
        "Role": role,
        "classification": "error",
        "rationale": "Empty essay submission",
        "cross_cutting_assessment": {},
        "strengths": [],
        "weaknesses": [],
        "writing_quality": {},
        "ai_usage_probability": "unknown",
        "ai_usage_indicators": "",
        "question_assessments": [],
    }
 
 
def _normalise_result(result: Dict, candidate_number: str, role: str) -> Dict:
    """Ensures every expected key is present in the result dict.
 
    Note: we always set 'candidate_number' from our own parameter rather than
    trusting whatever Claude returned in the response. The candidate number is
    authoritative on our side; Claude's job is to grade, not to identify.
    """
    return {
        "candidate_number": candidate_number,
        "Role": result.get("Role", role),
        "classification": result.get("classification", "unknown"),
        "rationale": result.get("rationale", ""),
        "cross_cutting_assessment": result.get("cross_cutting_assessment", {}),
        "strengths": result.get("strengths", []),
        "weaknesses": result.get("weaknesses", []),
        "writing_quality": result.get("writing_quality", {}),
        "ai_usage_probability": result.get("ai_usage_probability", "unknown"),
        "ai_usage_indicators": result.get("ai_usage_indicators", ""),
        "question_assessments": result.get("question_assessments", []),
    }
 
 
def _strip_code_fences(text: str) -> str:
    """Removes leading/trailing markdown code fences if present.
 
    Claude often wraps JSON in ```json ... ``` even when instructed otherwise.
    This is a known quirk; rather than fight it via prompting alone, strip
    fences defensively before parsing. Handles all common variants:
 
        ```json\\n{...}\\n```
        ```\\n{...}\\n```
        {...}                    (no fences — passed through unchanged)
    """
    text = text.strip()
 
    if not text.startswith("```"):
        return text
 
    lines = text.split("\n")
 
    # Drop the opening fence line (e.g. "```json" or just "```")
    lines = lines[1:]
 
    # Drop the closing fence line if present
    if lines and lines[-1].strip().startswith("```"):
        lines = lines[:-1]
 
    return "\n".join(lines).strip()
 