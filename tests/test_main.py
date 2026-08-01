"""Unit tests for the --dry-run pre-flight.

The property that matters: a dry run must never reach the grader. It is the
command you reach for *because* you don't want to spend, so if it can ever call
the API it is worse than not having it. `grade_essays` is replaced with a
tripwire that raises, so any call fails the test loudly.

No API calls are made. Run from the project root with:

    venv\\Scripts\\python.exe -m unittest discover tests -v
"""

import io
import sys
import tempfile
import unittest
from contextlib import redirect_stdout
from pathlib import Path
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

import main
import grading_cache as gc


def _essay(number, role, text):
    return {
        "candidate_number": number,
        "role": role,
        "essay_text": text,
        "source_file": f"{number}_{role}_assignment.pdf",
    }


class _Tripwire(Exception):
    """Raised if a dry run ever tries to grade."""


class TestDryRun(unittest.TestCase):
    def _run(self, essays, cache_data=None, roles=None):
        """Runs the pipeline in dry-run mode and returns what it printed."""
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            (base / "input" / "essays").mkdir(parents=True)
            (base / "output").mkdir(parents=True)
            if cache_data is not None:
                gc.save_cache(str(base / "output" / "grading_cache.json"), cache_data)

            buffer = io.StringIO()
            with mock.patch.object(main, "load_essays", return_value=essays), \
                 mock.patch.object(main, "__file__", str(base / "src" / "main.py")), \
                 mock.patch.object(
                     main, "grade_essays",
                     side_effect=_Tripwire("dry run must not grade"),
                 ), \
                 mock.patch.object(
                     main, "check_plagiarism",
                     side_effect=_Tripwire("dry run must not screen"),
                 ), \
                 mock.patch.object(
                     main, "write_report",
                     side_effect=_Tripwire("dry run must not write a report"),
                 ):
                with redirect_stdout(buffer):
                    main.run_pipeline(roles=roles, dry_run=True)
            return buffer.getvalue()

    def test_dry_run_never_grades(self):
        """The whole point: no API call, no report, no cost."""
        output = self._run([_essay("1", "LTC", "aaa"), _essay("2", "TRI", "bbb")])
        self.assertIn("Dry run", output)
        self.assertIn("2 API call(s) if run for real", output)

    def test_empty_cache_reports_everything_as_new(self):
        output = self._run([_essay("1", "LTC", "aaa")])
        self.assertIn("new submission", output)
        self.assertIn("Would grade 1", output)

    def test_changed_text_is_named_not_just_counted(self):
        """Changed text is the trap this command exists to surface."""
        cache = gc._empty_cache()
        original = [_essay("1", "LTC", "aaa"), _essay("2", "TRI", "bbb")]
        graded = [(e, {"candidate_number": e["candidate_number"]}) for e in original]
        gc.merge_and_update(cache, original, graded, "HASH", "v1.0")

        edited = [_essay("1", "LTC", "aaa REEXTRACTED"), _essay("2", "TRI", "bbb")]
        with mock.patch.object(gc, "fingerprint", return_value="HASH"), \
             mock.patch.object(main, "fingerprint", return_value="HASH"):
            output = self._run(edited, cache_data=cache)

        self.assertIn("text changed since last grade", output)
        self.assertIn("1|LTC", output)
        self.assertIn("ignores --roles", output)

    def test_scope_is_reported(self):
        output = self._run([_essay("1", "TRI", "bbb")], roles={"TRI"})
        self.assertIn("Regrade scoped to: TRI", output)


class TestArgParsing(unittest.TestCase):
    def test_dry_run_defaults_off(self):
        self.assertFalse(main._parse_args([]).dry_run)

    def test_dry_run_flag(self):
        self.assertTrue(main._parse_args(["--dry-run"]).dry_run)

    def test_roles_and_dry_run_combine(self):
        args = main._parse_args(["--roles", "TRI", "--dry-run"])
        self.assertEqual(args.roles, "TRI")
        self.assertTrue(args.dry_run)


if __name__ == "__main__":
    unittest.main()
