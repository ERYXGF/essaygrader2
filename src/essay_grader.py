"""Sends each essay to Claude for grading and returns structured results."""
 
import os
import json
import time
from pathlib import Path
from typing import List, Dict
 
import httpx
from dotenv import load_dotenv
import anthropic
 
 
# ============================================================
# CONFIG
# ============================================================
load_dotenv()
 
# Current production Haiku model (April 2026). Anthropic retired
# claude-3-haiku-20240307 in February 2026.
DEFAULT_MODEL = "claude-haiku-4-5-20251001"
 
# Output token cap. 4000 is enough for thorough "Do Not Interview" verdicts
# with detailed rationale, weaknesses, and per-question assessments.
MAX_TOKENS = 4000
 
# Retry policy for transient connection errors.
MAX_RETRIES = 4
 
# The grading prompt lives at <project_root>/config/essay_prompt.txt.
# This file is in <project_root>/src, so we go up one level then into config.
BASE_DIR = Path(__file__).resolve().parent
PROMPT_FILE = BASE_DIR.parent / "config" / "essay_prompt.txt"
 
 
# Errors we treat as transient and worth retrying.
#
# We catch a deliberately wide net of httpx and anthropic exception types
# because in streaming mode, network errors occurring mid-stream surface as
# raw httpx exceptions rather than the wrapped anthropic ones.
RETRYABLE_EXCEPTIONS = (
    anthropic.APIConnectionError,
    anthropic.APITimeoutError,
    httpx.ConnectError,
    httpx.ReadError,
    httpx.WriteError,
    httpx.RemoteProtocolError,
    httpx.PoolTimeout,
    httpx.ConnectTimeout,
    httpx.ReadTimeout,
    httpx.WriteTimeout,
    ConnectionError,  # OS-level WinError 10054 sometimes surfaces this way
)
 
 
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
    """Builds an Anthropic client with a custom HTTP client tuned for
    network paths that aggressively close idle/long connections.
 
    Why a custom http_client
    ------------------------
    On Windows networks where corporate proxies, antivirus, or aggressive NAT
    sweepers kill long-running TLS connections (WinError 10054), the SDK's
    default httpx settings can struggle. We apply two defensive measures:
 
      - Disable HTTP/2: connection multiplexing is a known soft spot;
        HTTP/1.1 is more forgiving.
      - Longer read timeout: a 4000-token grade can legitimately take
        60+ seconds. Default httpx read timeout is 5s, far too short.
    """
    api_key = os.getenv("ANTHROPIC_API_KEY")
    if not api_key:
        raise RuntimeError(
            "ANTHROPIC_API_KEY not found. Add it to your .env file at the "
            "project root (or export it in your shell) before running the "
            "pipeline."
        )
 
    custom_http_client = httpx.Client(
        http2=False,
        timeout=httpx.Timeout(
            connect=15.0,   # opening the TCP/TLS connection
            read=180.0,     # reading bytes from a stream
            write=30.0,     # uploading the request body
            pool=10.0,      # waiting for a free connection from the pool
        ),
    )
 
    return anthropic.Anthropic(api_key=api_key, http_client=custom_http_client)
 
 
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
    Authentication errors, rate-limit errors, and JSON-parse errors raise
    immediately and stop the run. Connection errors are retried with
    exponential backoff before giving up.
 
    Empty essays (data condition, not system failure) return a normal row
    flagged with classification='error'.
    """
    if not essay_text or not essay_text.strip():
        return _empty_essay_result(candidate_number, role)
 
    raw_output = _call_claude_with_retry(
        client=client,
        model=model,
        system=grading_prompt,
        candidate_number=candidate_number,
        role=role,
        essay_text=essay_text,
    )
 
    try:
        json_text = _extract_first_json_object(raw_output)
        result = json.loads(json_text)
    except (ValueError, json.JSONDecodeError) as exc:
        raise ValueError(
            f"Could not parse JSON from Claude's response for candidate "
            f"{candidate_number}.\nReason: {exc}\nRaw response:\n{raw_output}"
        ) from exc
 
    return _normalise_result(result, candidate_number, role)
 
 
# ============================================================
# BATCH ENTRY POINT
# ============================================================
def grade_essays(essays: List[Dict], model: str = DEFAULT_MODEL) -> List[Dict]:
    """Grades every essay in the list."""
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
 
        result["source_file"] = essay.get("source_file", "")
        results.append(result)
 
    return results
 
 
# ============================================================
# NETWORK CALL WITH RETRY
# ============================================================
def _call_claude_with_retry(
    client: anthropic.Anthropic,
    model: str,
    system: str,
    candidate_number: str,
    role: str,
    essay_text: str,
    max_retries: int = MAX_RETRIES,
) -> str:
    """Calls Claude using streaming mode and retries on transient errors."""
    last_exc: Exception | None = None
 
    for attempt in range(1, max_retries + 1):
        try:
            with client.messages.stream(
                model=model,
                max_tokens=MAX_TOKENS,
                temperature=0,
                system=system,
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
            ) as stream:
                return "".join(stream.text_stream).strip()
 
        except RETRYABLE_EXCEPTIONS as exc:
            last_exc = exc
            if attempt < max_retries:
                wait = 2 ** attempt  # 2s, 4s, 8s, 16s
                err_type = type(exc).__name__
                print(
                    f"     ⚠ {err_type} (attempt {attempt}/{max_retries}); "
                    f"retrying in {wait}s..."
                )
                time.sleep(wait)
            else:
                raise
 
    assert last_exc is not None
    raise last_exc
 
 
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
 
 
def _extract_first_json_object(text: str) -> str:
    """Finds and returns the first balanced JSON object in text.
 
    Why this exists
    ---------------
    Claude sometimes wraps its JSON in markdown code fences. Sometimes it
    adds a preamble before the JSON. Sometimes — for borderline assessments —
    it returns one JSON object, second-guesses itself, then returns a
    *second* JSON object with a corrected verdict, with markdown narration
    between them. A simple ``json.loads`` of the whole response fails on
    any of these variations.
 
    This function locates the first '{' in the response, then tracks brace
    depth (correctly handling braces inside string literals) until it finds
    the matching '}'. The resulting substring is guaranteed to be a single
    balanced JSON object — though not necessarily *valid* JSON; the caller
    still parses it, which catches malformed content.
 
    When Claude returns multiple JSON objects, we deliberately take the
    first one rather than the last. The first is the model's primary
    answer; subsequent ones are revisions, but those revisions also
    sometimes contradict the prompt's required schema. Taking the first
    is more predictable.
 
    Raises
    ------
    ValueError if no '{' is found, or if the first '{' has no matching '}'.
    """
    start = text.find("{")
    if start == -1:
        raise ValueError("No JSON object found in response (no '{' character)")
 
    depth = 0
    in_string = False
    escape_next = False
 
    for i in range(start, len(text)):
        ch = text[i]
 
        # Handle escape sequences inside strings
        if escape_next:
            escape_next = False
            continue
        if ch == "\\" and in_string:
            escape_next = True
            continue
 
        # Track whether we're inside a string literal
        if ch == '"':
            in_string = not in_string
            continue
 
        # Braces inside strings don't count toward depth
        if in_string:
            continue
 
        if ch == "{":
            depth += 1
        elif ch == "}":
            depth -= 1
            if depth == 0:
                return text[start : i + 1]
 
    raise ValueError(
        f"Unterminated JSON object starting at position {start}; "
        f"response may have been truncated."
    )
 