"""The single background grading run, and its live-progress state.

Launched as a subprocess running `python src/main.py --grade-scope ...` — the
exact same entry point the CLI uses — rather than calling run_pipeline()
in-process. That means: a hang or crash in the pipeline can't take the web
server down with it, ANTHROPIC_API_KEY needs no forwarding (the subprocess
loads .env itself, same as the CLI), and the web layer can never behave
differently from `python src/main.py` on the command line, because it IS
that command line. Progress comes free from the pipeline's own print()
statements — no callback needs threading through run_pipeline/grade_essays.

Only one run at a time: the pipeline reads and writes the same
output/grading_cache.json and input/essays/ on every run, so two at once
would corrupt each other's work. JOB is a single process-wide slot, which is
enough for a single-process, single-user app.
"""
from __future__ import annotations

import re
import subprocess
import threading
import time
from typing import List, Optional

from paths import MAIN_PY, PROJECT_ROOT, VENV_PYTHON

# Matches the pipeline's own emoji-prefixed step announcements in main.py.
_STEP_MARKERS = [
    ("📄 Loading essays", "loading_essays"),
    ("📤 Sending essays to Claude", "grading"),
    ("🔍 Screening essay pairs", "plagiarism"),
    ("📋 Checking the re-application embargo", "embargo"),
    ("📝 Writing Excel report", "report"),
    ("✅ Pipeline complete!", "complete"),
]
# Matches essay_grader.py's "  → Grading 12/45 (...)" / "  → Skipping 3/45 (...)".
_PROGRESS_RE = re.compile(r"→\s*(?:Grading|Skipping)\s+(\d+)\s*/\s*(\d+)")

_LOG_TAIL_KEPT = 500  # lines retained server-side; /api/status returns a shorter tail


class _JobState:
    """Guards its own fields with a lock — read via snapshot(), not directly."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._reset()

    def _reset(self) -> None:
        self.state = "idle"  # idle | running | done | error
        self.scope: Optional[str] = None
        self.roles: Optional[str] = None
        self.fy: Optional[str] = None
        self.report_only = False
        self.step: Optional[str] = None
        self.graded = 0
        self.total = 0
        self.log_lines: List[str] = []
        self.started_at: Optional[float] = None
        self.finished_at: Optional[float] = None
        self.exit_code: Optional[int] = None

    def snapshot(self, log_tail: int = 40) -> dict:
        with self._lock:
            return {
                "state": self.state,
                "scope": self.scope,
                "roles": self.roles,
                "fy": self.fy,
                "report_only": self.report_only,
                "step": self.step,
                "graded": self.graded,
                "total": self.total,
                "log_tail": list(self.log_lines[-log_tail:]),
                "started_at": self.started_at,
                "finished_at": self.finished_at,
                "exit_code": self.exit_code,
            }


JOB = _JobState()


def start_run(
    scope: str,
    roles: Optional[str] = None,
    fy: Optional[str] = None,
    report_only: bool = False,
) -> bool:
    """Starts a run in the background. Returns False if one is already running.

    `roles`, if given, is a comma-separated string in the same format as the
    CLI's --roles flag (see main.parse_roles) — scopes a rubric-driven regrade
    to those roles, leaving other roles' cached grades untouched.

    `fy`, if given, is a per-action override of the campaign, same as the
    CLI's --fy — never written to config/campaign.txt.

    `report_only`, if true, skips grading entirely and just rebuilds the
    report from whatever is already cached (same as the CLI's --report-only)
    — `scope` is ignored in that case, since nothing gets graded either way.
    """
    with JOB._lock:
        if JOB.state == "running":
            return False
        JOB._reset()
        JOB.state = "running"
        JOB.scope = scope
        JOB.roles = roles
        JOB.fy = fy
        JOB.report_only = report_only
        JOB.started_at = time.time()

    threading.Thread(
        target=_run, args=(scope, roles, fy, report_only), daemon=True
    ).start()
    return True


def _run(
    scope: str,
    roles: Optional[str] = None,
    fy: Optional[str] = None,
    report_only: bool = False,
) -> None:
    argv = [str(VENV_PYTHON), str(MAIN_PY)]
    argv += ["--report-only"] if report_only else ["--grade-scope", scope]
    if roles:
        argv += ["--roles", roles]
    if fy:
        argv += ["--fy", fy]
    proc = subprocess.Popen(
        argv,
        cwd=str(PROJECT_ROOT),
        stdin=subprocess.DEVNULL,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1,
    )
    assert proc.stdout is not None
    try:
        for raw_line in proc.stdout:
            line = raw_line.rstrip("\n")
            with JOB._lock:
                JOB.log_lines.append(line)
                overflow = len(JOB.log_lines) - _LOG_TAIL_KEPT
                if overflow > 0:
                    del JOB.log_lines[:overflow]
                for marker, step in _STEP_MARKERS:
                    if marker in line:
                        JOB.step = step
                        break
                match = _PROGRESS_RE.search(line)
                if match:
                    JOB.graded, JOB.total = int(match.group(1)), int(match.group(2))
        exit_code = proc.wait()
    except Exception:
        proc.kill()
        proc.wait()
        with JOB._lock:
            JOB.state = "error"
            JOB.finished_at = time.time()
        raise

    with JOB._lock:
        JOB.exit_code = exit_code
        JOB.finished_at = time.time()
        JOB.state = "done" if exit_code == 0 else "error"
