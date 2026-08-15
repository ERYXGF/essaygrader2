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


def _essay(number, role, text, file_hash=""):
    return {
        "candidate_number": number,
        "role": role,
        "essay_text": text,
        "file_sha256": file_hash,
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

    def test_resubmission_is_named_not_just_counted(self):
        """A candidate sending a different file bypasses --roles, so name them."""
        cache = gc._empty_cache()
        original = [
            _essay("1", "LTC", "aaa", file_hash="FILE-A"),
            _essay("2", "TRI", "bbb", file_hash="FILE-B"),
        ]
        graded = [(e, {"candidate_number": e["candidate_number"]}) for e in original]
        gc.merge_and_update(cache, original, graded, "HASH", "v1.0")

        resubmitted = [
            _essay("1", "LTC", "different essay", file_hash="FILE-A-V2"),
            _essay("2", "TRI", "bbb", file_hash="FILE-B"),
        ]
        with mock.patch.object(main, "fingerprint", return_value="HASH"):
            output = self._run(resubmitted, cache_data=cache)

        self.assertIn("resubmitted", output)
        self.assertIn("1|LTC", output)
        self.assertIn("ignores --roles", output)

    def test_extraction_drift_warns_but_costs_nothing(self):
        """Same file, changed extraction, no version bump: reuse but say so."""
        cache = gc._empty_cache()
        original = [_essay("1", "LTC", "aaa", file_hash="FILE-A")]
        graded = [(original[0], {"candidate_number": "1"})]
        gc.merge_and_update(cache, original, graded, "HASH", "v1.0")

        # Same file, but our extractor now reads it differently.
        redread = [_essay("1", "LTC", "aaa read differently", file_hash="FILE-A")]
        with mock.patch.object(main, "fingerprint", return_value="HASH"):
            output = self._run(redread, cache_data=cache)

        self.assertIn("Would grade 0", output)          # costs nothing
        self.assertIn("will NOT refresh", output)       # but is not silent
        self.assertIn("1|LTC", output)

    def test_scope_is_reported(self):
        output = self._run([_essay("1", "TRI", "bbb")], roles={"TRI"})
        self.assertIn("Regrade scoped to: TRI", output)


class TestReportOnly(unittest.TestCase):
    """--report-only must never grade; that is the entire reason it exists."""

    def _run(self, essays, cache_data=None, **kwargs):
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
                     side_effect=_Tripwire("report-only must not grade"),
                 ), \
                 mock.patch.object(main, "check_plagiarism", return_value=[]), \
                 mock.patch.object(main, "apply_plagiarism_overrides"), \
                 mock.patch.object(main, "write_report"):
                with redirect_stdout(buffer):
                    main.run_pipeline(report_only=True, **kwargs)
            return buffer.getvalue()

    def _seeded(self):
        """A cache holding one FY26 grade, and the matching essay list."""
        cache = gc._empty_cache()
        essays = [_essay("1", "LTC", "aaa", file_hash="F1")]
        gc.merge_and_update(
            cache, essays, [(essays[0], {"candidate_number": "1"})],
            "HASH", "v1.0", "FY26",
        )
        return cache, essays

    def test_rebuilds_without_grading(self):
        cache, essays = self._seeded()
        output = self._run(essays, cache_data=cache, fy="FY26")
        self.assertIn("Report-only", output)
        self.assertIn("Campaign: FY26", output)

    def test_campaign_banner_names_its_source(self):
        cache, essays = self._seeded()
        output = self._run(essays, cache_data=cache, fy="FY26")
        self.assertIn("--fy", output)

    def test_ungraded_essays_are_reported_as_left_out(self):
        """A new PDF cannot appear in a report built from the cache."""
        cache, essays = self._seeded()
        with_new = essays + [_essay("2", "TRI", "brand new", file_hash="F2")]
        output = self._run(with_new, cache_data=cache, fy="FY26")
        self.assertIn("have no usable grade and are left out", output)
        # Named individually, so the reviewer knows which rows are missing
        # rather than only how many.
        self.assertIn("2|TRI", output)


class TestEmbargoWiring(unittest.TestCase):
    """The embargo annotates results; it never blocks or skips a candidate."""

    def _results(self):
        return [
            {"candidate_number": "872524", "Role": "TRI"},
            {"candidate_number": "860775", "Role": "LTC"},
        ]

    def _list(self, rows):
        handle = tempfile.NamedTemporaryFile(
            "w", suffix=".csv", delete=False, encoding="utf-8-sig", newline=""
        )
        handle.write("Created,Staff Number,Position applied for,OUTCOME\n")
        for created, staff, role in rows:
            handle.write(f"{created},{staff},{role},\n")
        handle.close()
        return handle.name

    def test_a_missing_list_marks_every_row_rather_than_leaving_it_blank(self):
        """A blank cell reads as 'checked and clear'. Not checking is not clear."""
        results = self._results()
        buffer = io.StringIO()
        with redirect_stdout(buffer):
            main._apply_embargoes(
                results, "FY27", main._load_applications("/nonexistent/list.csv")
            )
        self.assertTrue(all(r["embargo"] == main.NOT_CHECKED for r in results))
        self.assertIn("NOT checked", buffer.getvalue())

    def test_flags_a_reapplicant_and_clears_the_others(self):
        path = self._list([
            ("25/07/2026 09:12", "872524", "TRI"),   # FY26
            ("12/10/2026 08:30", "872524", "LTC"),   # FY27, 79 days later
            ("15/11/2026 09:00", "860775", "TRI"),   # FY27 only
        ])
        results = self._results()
        with redirect_stdout(io.StringIO()):
            main._apply_embargoes(
                results, "FY27", main._load_applications(path)
            )
        self.assertTrue(results[0]["embargo"].startswith("⚠"))
        self.assertEqual(results[1]["embargo"], "")

    def test_a_candidate_absent_from_the_list_is_marked_unknown(self):
        path = self._list([("12/10/2026 08:30", "872524", "LTC")])
        results = self._results()
        with redirect_stdout(io.StringIO()):
            main._apply_embargoes(
                results, "FY27", main._load_applications(path)
            )
        self.assertEqual(results[1]["embargo"], main.NOT_LISTED)

    def test_nothing_is_removed_from_the_results(self):
        """Flag only: an embargoed candidate is still graded and still reported."""
        path = self._list([
            ("25/07/2026 09:12", "872524", "TRI"),
            ("12/10/2026 08:30", "872524", "LTC"),
        ])
        results = self._results()
        with redirect_stdout(io.StringIO()):
            main._apply_embargoes(
                results, "FY27", main._load_applications(path)
            )
        self.assertEqual(len(results), 2)


class TestArgParsing(unittest.TestCase):
    def test_dry_run_defaults_off(self):
        self.assertFalse(main._parse_args([]).dry_run)

    def test_dry_run_flag(self):
        self.assertTrue(main._parse_args(["--dry-run"]).dry_run)

    def test_roles_and_dry_run_combine(self):
        args = main._parse_args(["--roles", "TRI", "--dry-run"])
        self.assertEqual(args.roles, "TRI")
        self.assertTrue(args.dry_run)

    def test_report_only_and_fy_default_off(self):
        args = main._parse_args([])
        self.assertFalse(args.report_only)
        self.assertIsNone(args.fy)

    def test_report_only_and_fy_parse(self):
        args = main._parse_args(["--report-only", "--fy", "FY26"])
        self.assertTrue(args.report_only)
        self.assertEqual(args.fy, "FY26")

    def test_recruitment_list_defaults_to_none_and_parses(self):
        self.assertIsNone(main._parse_args([]).recruitment_list)
        args = main._parse_args(["--recruitment-list", "/tmp/list.csv"])
        self.assertEqual(args.recruitment_list, "/tmp/list.csv")


if __name__ == "__main__":
    unittest.main()
