"""Unit tests for the web layer's reading of a run's stdout.

`web/jobs.py` follows a grading run by parsing the lines `python src/main.py`
prints. That coupling has already broken once — the progress bar changed the
per-essay line, the web app's private regex stopped matching, and the browser's
bar would have sat at 0% for a whole run without a single error anywhere.

So these tests do not use hand-typed sample lines. They drive the **real**
grading loop with Claude stubbed out, capture what it actually prints to a
pipe, and feed that through the real `consume_line`. If the printed format and
the parser ever disagree again, this fails.

No API calls, no subprocess, no filesystem writes. Run from the project root:

    venv/bin/python -m unittest discover tests -v
"""

import io
import sys
import unittest
from contextlib import redirect_stdout
from pathlib import Path
from unittest import mock

ROOT = Path(__file__).resolve().parent.parent
# web/app.py puts both of these on sys.path before importing jobs; do the same.
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(ROOT / "web"))

import essay_grader as eg
import jobs


def _essays(count: int) -> list:
    return [
        {
            "candidate_number": str(860000 + i),
            "role": "TRI",
            "essay_text": "an essay",
            "source_file": f"{860000 + i}_TRI_assignment.pdf",
            "format_label": "",
        }
        for i in range(count)
    ]


def _capture_real_grading_output(essays: list) -> list:
    """What grade_essays actually prints to a pipe, with Claude stubbed out.

    redirect_stdout gives a StringIO, whose isatty() is False — so this is the
    non-terminal path, exactly as the web app's subprocess sees it.
    """
    def fake_grade(essay_text, candidate_number, role, client, grading_prompt, model):
        return {
            "candidate_number": candidate_number,
            "role": role,
            "classification": "Priority Interview",
        }

    buffer = io.StringIO()
    with mock.patch.object(eg, "_build_client", lambda: None), \
            mock.patch.object(eg, "_load_grading_prompt", lambda: "prompt"), \
            mock.patch.object(eg, "grade_essay", fake_grade):
        with redirect_stdout(buffer):
            eg.grade_essays(essays)
    return buffer.getvalue().splitlines()


class JobTestCase(unittest.TestCase):
    def setUp(self):
        jobs.JOB._reset()


class TestFollowingARealRun(JobTestCase):
    def test_the_bar_advances_from_zero_to_the_full_count(self):
        lines = _capture_real_grading_output(_essays(12))
        self.assertTrue(lines, "the grading loop printed nothing to a pipe")

        seen = []
        for line in lines:
            jobs.consume_line(line)
            seen.append((jobs.JOB.graded, jobs.JOB.total))

        self.assertEqual(jobs.JOB.graded, 12)
        self.assertEqual(jobs.JOB.total, 12)
        # Monotonic, and it actually moved — a bar stuck at 0 is the bug.
        counts = [g for g, _ in seen]
        self.assertEqual(counts, sorted(counts))
        self.assertEqual(counts[-1], 12)

    def test_it_names_the_candidate(self):
        for line in _capture_real_grading_output(_essays(3)):
            jobs.consume_line(line)
        self.assertEqual(jobs.JOB.current, "860002 TRI")

    def test_an_unreadable_submission_still_advances_the_bar(self):
        essays = _essays(3)
        essays[1]["essay_text"] = ""
        essays[1]["format_reason"] = "'860001_TRI_assignment.docx' is a Word file."
        for line in _capture_real_grading_output(essays):
            jobs.consume_line(line)
        self.assertEqual((jobs.JOB.graded, jobs.JOB.total), (3, 3))


class TestStepMarkers(JobTestCase):
    def test_the_pipeline_steps_are_recognised(self):
        for line, expected in [
            ("📄 Loading essays from /x/input/essays...", "loading_essays"),
            ("📤 Sending essays to Claude...", "grading"),
            ("🔍 Screening essay pairs for plagiarism...", "plagiarism"),
            ("📋 Checking the re-application embargo...", "embargo"),
            ("📝 Writing Excel report...", "report"),
            ("✅ Pipeline complete!", "complete"),
        ]:
            with self.subTest(step=expected):
                jobs.consume_line(line)
                self.assertEqual(jobs.JOB.step, expected)

    def test_the_count_survives_the_steps_after_grading(self):
        """Grading finishes at N/N; the later steps print no progress lines,
        so the bar must stay full rather than resetting or running backwards."""
        for line in _capture_real_grading_output(_essays(4)):
            jobs.consume_line(line)
        for line in [
            "🔍 Screening essay pairs for plagiarism...",
            "  -> Reviewing pair 850263 <-> 850327 (lexical 3%, semantic 75%)",
            "   ✓ 2 pair(s) flagged for review",
            "📝 Writing Excel report...",
        ]:
            jobs.consume_line(line)
        self.assertEqual((jobs.JOB.graded, jobs.JOB.total), (4, 4))
        self.assertEqual(jobs.JOB.step, "report")


class TestLogTail(JobTestCase):
    def test_every_line_is_kept_for_the_browser(self):
        for line in ["🚀 Pipeline starting...", "   ✓ Loaded 12 essay(s)"]:
            jobs.consume_line(line)
        self.assertEqual(jobs.JOB.log_lines[-2:],
                         ["🚀 Pipeline starting...", "   ✓ Loaded 12 essay(s)"])

    def test_the_tail_is_bounded(self):
        for n in range(jobs._LOG_TAIL_KEPT + 50):
            jobs.consume_line(f"line {n}")
        self.assertEqual(len(jobs.JOB.log_lines), jobs._LOG_TAIL_KEPT)
        self.assertEqual(jobs.JOB.log_lines[-1], f"line {jobs._LOG_TAIL_KEPT + 49}")

    def test_the_snapshot_carries_what_the_ui_reads(self):
        for line in _capture_real_grading_output(_essays(2)):
            jobs.consume_line(line)
        snapshot = jobs.JOB.snapshot()
        for key in ("state", "step", "graded", "total", "current", "log_tail",
                    "started_at"):
            self.assertIn(key, snapshot, f"the UI reads {key}")


if __name__ == "__main__":
    unittest.main()
