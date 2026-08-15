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


class TestConfirmGrading(unittest.TestCase):
    """Nothing is graded until this says so. A rubric change silently makes
    every cached grade stale, so an ordinary run can become a full regrade
    nobody chose to pay for — this is where that choice gets made."""

    def _classified(self, new=0, stale=0):
        rows = [(_essay(f"n{i}", "TRI", "x"), gc.REASON_NEW) for i in range(new)]
        rows += [
            (_essay(f"s{i}", "LTC", "x"), gc.REASON_STALE_RUBRIC) for i in range(stale)
        ]
        return rows

    def _answer(self, classified, replies, **kwargs):
        """Runs the prompt against scripted keystrokes; returns (chosen, output)."""
        buffer = io.StringIO()
        with mock.patch.object(main.sys.stdin, "isatty", return_value=True), \
             mock.patch("builtins.input", side_effect=replies):
            with redirect_stdout(buffer):
                chosen = main._confirm_grading(classified, **kwargs)
        return chosen, buffer.getvalue()

    def test_yes_grades_everything(self):
        chosen, _ = self._answer(self._classified(new=4, stale=154), ["y"])
        self.assertEqual(len(chosen), 158)

    def test_only_new_skips_the_rubric_regrade(self):
        """The whole point: incremental without needing --roles."""
        chosen, output = self._answer(self._classified(new=4, stale=154), ["o"])
        self.assertEqual(len(chosen), 4)
        self.assertTrue(all(e["candidate_number"].startswith("n") for e in chosen))
        self.assertIn("[o]", output)

    def test_no_cancels(self):
        chosen, _ = self._answer(self._classified(new=4, stale=154), ["n"])
        self.assertEqual(chosen, [])

    def test_eof_cancels_rather_than_proceeding(self):
        chosen, _ = self._answer(self._classified(new=1, stale=1), EOFError())
        self.assertEqual(chosen, [])

    def test_unrecognised_input_reasks(self):
        """A typo must not be read as consent, nor as a refusal."""
        chosen, output = self._answer(self._classified(new=1, stale=1), ["what", "y"])
        self.assertEqual(len(chosen), 2)
        self.assertIn("Please answer", output)

    def test_all_new_work_is_not_offered_a_pointless_choice(self):
        chosen, output = self._answer(self._classified(new=3), ["y"])
        self.assertEqual(len(chosen), 3)
        self.assertNotIn("[o]", output)

    def test_the_reasons_are_shown_not_just_the_total(self):
        """158 alone does not tell you it is 154 regrades and 4 new essays."""
        _, output = self._answer(self._classified(new=4, stale=154), ["y"])
        self.assertIn("About to grade 158", output)
        self.assertIn(gc.REASON_LABELS[gc.REASON_NEW], output)
        self.assertIn(gc.REASON_LABELS[gc.REASON_STALE_RUBRIC], output)

    def test_assume_yes_never_prompts(self):
        classified = self._classified(new=4, stale=154)
        buffer = io.StringIO()
        with mock.patch("builtins.input", side_effect=AssertionError("prompted!")):
            with redirect_stdout(buffer):
                chosen = main._confirm_grading(classified, assume_yes=True)
        self.assertEqual(len(chosen), 158)

    def test_a_non_interactive_run_proceeds_as_it_always_did(self):
        """Blocking a scripted run on a prompt nobody can answer would be worse
        than the problem this solves."""
        buffer = io.StringIO()
        with mock.patch.object(main.sys.stdin, "isatty", return_value=False), \
             mock.patch("builtins.input", side_effect=AssertionError("prompted!")):
            with redirect_stdout(buffer):
                chosen = main._confirm_grading(self._classified(new=2, stale=2))
        self.assertEqual(len(chosen), 4)
        self.assertIn("not a terminal", buffer.getvalue())

    def test_nothing_to_grade_asks_nothing(self):
        with mock.patch("builtins.input", side_effect=AssertionError("prompted!")):
            self.assertEqual(main._confirm_grading([]), [])


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
        self.assertTrue(all(r["embargo"] == main.EMBARGO_UNKNOWN for r in results))
        self.assertTrue(all(r["embargo_detail"] == main.NOT_CHECKED for r in results))
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
        self.assertEqual(results[0]["embargo"], main.EMBARGO_YES)
        self.assertTrue(results[0]["embargo_detail"].startswith("⚠"))
        # A clear candidate says so outright rather than by an empty cell.
        self.assertEqual(results[1]["embargo"], main.EMBARGO_NO)
        self.assertEqual(results[1]["embargo_detail"], "")

    def test_a_candidate_absent_from_the_list_is_marked_unknown(self):
        path = self._list([("12/10/2026 08:30", "872524", "LTC")])
        results = self._results()
        with redirect_stdout(io.StringIO()):
            main._apply_embargoes(
                results, "FY27", main._load_applications(path)
            )
        self.assertEqual(results[1]["embargo"], main.EMBARGO_UNKNOWN)
        self.assertEqual(results[1]["embargo_detail"], main.NOT_LISTED)

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


class TestCampaignMembershipGuard(unittest.TestCase):
    """Stops a run grading another campaign's essays under this campaign's name.

    The folder is not campaign-aware; the export is. Without this, running
    --fy FY26 with FY27 PDFs still in input/essays/ grades all of them at full
    price and files them as FY26 candidates.
    """

    def _app(self, staff, year, date="2026-07-28"):
        import datetime as dt
        import recruitment_list as rl
        return rl.Application(
            staff, dt.date.fromisoformat(date), "TRI", "", "", year
        )

    def test_essays_from_another_campaign_are_excluded(self):
        essays = [_essay("1", "TRI", "aaa"), _essay("2", "TRI", "bbb")]
        apps = [self._app("1", "FY26"), self._app("2", "FY27")]
        kept, excluded = main._check_campaign_membership(essays, "FY26", apps)
        self.assertEqual([e["candidate_number"] for e in kept], ["1"])
        self.assertEqual(excluded[0][0]["candidate_number"], "2")
        self.assertEqual(excluded[0][1], ["FY27"])

    def test_a_candidate_absent_from_the_export_is_kept(self):
        """Absence is not evidence — we cannot prove they are misfiled."""
        essays = [_essay("9", "TRI", "aaa")]
        kept, excluded = main._check_campaign_membership(
            essays, "FY26", [self._app("1", "FY26")]
        )
        self.assertEqual(len(kept), 1)
        self.assertEqual(excluded, [])

    def test_a_candidate_in_both_campaigns_is_kept(self):
        essays = [_essay("1", "TRI", "aaa")]
        apps = [self._app("1", "FY26"), self._app("1", "FY27")]
        kept, _ = main._check_campaign_membership(essays, "FY27", apps)
        self.assertEqual(len(kept), 1)

    def test_no_export_means_no_guard(self):
        essays = [_essay("1", "TRI", "aaa")]
        for applications in (None, []):
            kept, excluded = main._check_campaign_membership(
                essays, "FY26", applications
            )
            self.assertEqual(len(kept), 1)
            self.assertEqual(excluded, [])

    def test_the_exclusion_is_named_not_just_counted(self):
        buffer = io.StringIO()
        with redirect_stdout(buffer):
            main._report_excluded([(_essay("2", "TRI", "b"), ["FY27"])], "FY26")
        output = buffer.getvalue()
        self.assertIn("2|TRI", output)
        self.assertIn("FY27", output)


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

    def test_yes_defaults_off_and_parses(self):
        self.assertFalse(main._parse_args([]).yes)
        self.assertTrue(main._parse_args(["--yes"]).yes)
        self.assertTrue(main._parse_args(["-y"]).yes)

    def test_recruitment_list_defaults_to_none_and_parses(self):
        self.assertIsNone(main._parse_args([]).recruitment_list)
        args = main._parse_args(["--recruitment-list", "/tmp/list.csv"])
        self.assertEqual(args.recruitment_list, "/tmp/list.csv")


if __name__ == "__main__":
    unittest.main()
