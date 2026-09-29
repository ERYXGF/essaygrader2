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

    def test_job_numbers_are_resolved_before_the_per_job_flags(self):
        """Regression: the embargo ran before _apply_job_numbers, so a row
        whose job number came from the List lookup had none yet and fell back
        to the whole person — flagging the rejected job's own row."""
        cache, essays = self._seeded()
        order = mock.Mock()
        order._apply_job_numbers.return_value = set()
        with mock.patch.object(main, "_apply_job_numbers", order._apply_job_numbers), \
             mock.patch.object(main, "_apply_embargoes", order._apply_embargoes), \
             mock.patch.object(
                 main, "_apply_double_applications", order._apply_double_applications
             ):
            self._run(essays, cache_data=cache, fy="FY26")
        called = [c[0] for c in order.mock_calls if not c[0].startswith("_apply_job_numbers.")]
        self.assertEqual(
            called[:3],
            ["_apply_job_numbers", "_apply_embargoes", "_apply_double_applications"],
        )

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

    def test_scope_all_grades_everything_without_prompting(self):
        """The web UI's confirm step already asked; this must not ask again."""
        with mock.patch("builtins.input", side_effect=AssertionError("prompted!")):
            chosen = main._confirm_grading(
                self._classified(new=4, stale=154), scope="all"
            )
        self.assertEqual(len(chosen), 158)

    def test_scope_new_narrows_to_incremental_without_prompting(self):
        with mock.patch("builtins.input", side_effect=AssertionError("prompted!")):
            chosen = main._confirm_grading(
                self._classified(new=4, stale=154), scope="new"
            )
        self.assertEqual(len(chosen), 4)
        self.assertTrue(all(e["candidate_number"].startswith("n") for e in chosen))

    def test_scope_new_falls_back_to_all_when_nothing_to_narrow(self):
        """Mirrors the CLI's [o] option: offered only when there's something to skip."""
        with mock.patch("builtins.input", side_effect=AssertionError("prompted!")):
            chosen = main._confirm_grading(self._classified(new=3), scope="new")
        self.assertEqual(len(chosen), 3)

    def test_scope_ignored_when_nothing_to_grade(self):
        with mock.patch("builtins.input", side_effect=AssertionError("prompted!")):
            self.assertEqual(main._confirm_grading([], scope="all"), [])


class TestPreview(unittest.TestCase):
    """Structured, JSON-shaped counterpart to --dry-run for a non-terminal caller.

    Built on the same classify() the CLI dry-run and the real run both use, so
    it cannot promise a web caller something a real run would not do.
    """

    def _run(self, essays, cache_data=None, roles=None, fy="FY26"):
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            (base / "input" / "essays").mkdir(parents=True)
            (base / "output").mkdir(parents=True)
            if cache_data is not None:
                gc.save_cache(str(base / "output" / "grading_cache.json"), cache_data)

            with mock.patch.object(main, "load_essays", return_value=essays), \
                 mock.patch.object(main, "__file__", str(base / "src" / "main.py")), \
                 mock.patch.object(
                     main, "find_export",
                     side_effect=FileNotFoundError("no export"),
                 ):
                return main.preview(roles=roles, fy=fy)

    def test_matches_what_dry_run_would_report(self):
        data = self._run([_essay("1", "LTC", "aaa"), _essay("2", "TRI", "bbb")])
        self.assertEqual(data["campaign"], "FY26")
        self.assertEqual(data["would_grade_total"], 2)
        self.assertEqual(data["incremental_total"], 2)
        self.assertEqual({c["reason"] for c in data["counts"]}, {gc.REASON_NEW})
        self.assertEqual(data["counts"][0]["label"], gc.REASON_LABELS[gc.REASON_NEW])

    def test_can_narrow_matches_the_cli_s_offer_condition(self):
        """can_narrow is true exactly when the CLI's [o] option would appear."""
        cache = gc._empty_cache()
        seeded = [_essay("1", "LTC", "aaa", file_hash="F1")]
        gc.merge_and_update(
            cache, seeded, [(seeded[0], {"candidate_number": "1"})], "OLD", "v1.0",
        )
        essays = seeded + [_essay("2", "TRI", "new one", file_hash="F2")]
        data = self._run(essays, cache_data=cache)
        self.assertTrue(data["can_narrow"])
        self.assertEqual(data["incremental_total"], 1)
        self.assertEqual(data["would_grade_total"], 2)

    def test_no_essays_to_narrow_reports_false(self):
        data = self._run([_essay("1", "LTC", "aaa")])
        self.assertFalse(data["can_narrow"])

    def test_missing_recruitment_list_is_reported_not_raised(self):
        data = self._run([_essay("1", "LTC", "aaa")])
        self.assertFalse(data["recruitment_list"]["found"])

    def test_raises_the_same_error_load_essays_does(self):
        """A missing/malformed input/essays/ must fail the same way for both callers."""
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            (base / "output").mkdir(parents=True)
            with mock.patch.object(main, "__file__", str(base / "src" / "main.py")):
                with self.assertRaises(FileNotFoundError):
                    main.preview(fy="FY26")


class TestEmbargoWiring(unittest.TestCase):
    """The embargo annotates results; it never blocks or skips a candidate."""

    def _results(self):
        return [
            {"candidate_number": "872524", "Role": "TRI"},
            {"candidate_number": "860775", "Role": "LTC"},
        ]

    def _list(self, rows):
        """rows are (created, staff, role) or (created, staff, role, decision)
        — decision defaults to '' when omitted, matching a candidate whose
        interview decision isn't recorded."""
        handle = tempfile.NamedTemporaryFile(
            "w", suffix=".csv", delete=False, encoding="utf-8-sig", newline=""
        )
        handle.write("Created,Staff Number,Position applied for,DECISION\n")
        for row in rows:
            created, staff, role = row[:3]
            decision = row[3] if len(row) > 3 else ""
            handle.write(f"{created},{staff},{role},{decision}\n")
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
            ("25/07/2026 09:12", "872524", "TRI", "NO"),  # FY26, rejected
            ("12/10/2026 08:30", "872524", "LTC"),        # FY27, 79 days later
            ("15/11/2026 09:00", "860775", "TRI"),        # FY27 only
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

    def test_only_the_job_applied_for_after_the_rejection_is_flagged(self):
        path = tempfile.NamedTemporaryFile(
            "w", suffix=".csv", delete=False, encoding="utf-8-sig", newline=""
        )
        path.write(
            "Created,Staff Number,Position applied for,DECISION,"
            "JOB NUMBER APPLIED FOR,ACTUALINTERVIEWDATE\n"
            "04/06/2026 09:00,850263,LTC,NO,1,24/07/2026\n"
            "25/07/2026 09:00,850263,LTC,,17073,\n"
        )
        path.close()
        results = [
            {"candidate_number": "850263", "Role": "LTC", "job_number": "1"},
            {"candidate_number": "850263", "Role": "LTC", "job_number": "17073"},
        ]
        with redirect_stdout(io.StringIO()):
            main._apply_embargoes(results, "FY26", main._load_applications(path.name))
        self.assertEqual(results[0]["embargo"], main.EMBARGO_NO)
        self.assertEqual(results[1]["embargo"], main.EMBARGO_YES)
        self.assertIn("interview on 24 Jul 2026", results[1]["embargo_detail"])

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


class TestDoubleApplicationWiring(unittest.TestCase):
    """Two *open* applications, in this or a neighbouring season, could
    mean two interviews for one person."""

    def _list(self, rows):
        """rows are (created, staff, role, job, decision, approval), optionally
        followed by (invited_to_interview, actual_interview_date)."""
        handle = tempfile.NamedTemporaryFile(
            "w", suffix=".csv", delete=False, encoding="utf-8-sig", newline=""
        )
        handle.write(
            "Created,Staff Number,Position applied for,DECISION,FINALAPPROVAL,"
            "JOB NUMBER APPLIED FOR,INTERVIEW,ACTUALINTERVIEWDATE\n"
        )
        for row in rows:
            created, staff, role, job, decision, approval = row[:6]
            invited, interviewed = (tuple(row[6:]) + ("", ""))[:2]
            handle.write(
                f"{created},{staff},{role},{decision},{approval},{job},"
                f"{invited},{interviewed}\n"
            )
        handle.close()
        return main._load_applications(handle.name)

    def _apply(self, rows, results, campaign="FY26"):
        with redirect_stdout(io.StringIO()):
            main._apply_double_applications(results, campaign, self._list(rows))
        return results

    def test_two_open_applications_flag_each_other(self):
        results = self._apply(
            [("25/07/2026 09:00", "7872", "LTC", "17074", "PENDING", "PENDING"),
             ("28/07/2026 09:00", "7872", "TRI", "17092", "YES", "PENDING")],
            [{"candidate_number": "7872", "job_number": "17074"},
             {"candidate_number": "7872", "job_number": "17092"}],
        )
        self.assertEqual([r["double_application"] for r in results], ["YES", "YES"])
        self.assertIn("TRI job 17092", results[0]["double_application_detail"])
        self.assertIn("interview: YES", results[0]["double_application_detail"])

    def test_either_side_of_the_season_turnover_is_caught(self):
        """The case the List is needed for: the other application is in the
        next season, so it is not a row of this report at all."""
        results = self._apply(
            [("25/09/2026 09:00", "100", "LTC", "1", "", ""),
             ("03/10/2026 09:00", "100", "TRI", "2", "", "")],
            [{"candidate_number": "100", "job_number": "1"}],
        )
        self.assertEqual(results[0]["double_application"], "YES")
        self.assertIn("FY27", results[0]["double_application_detail"])

    def test_two_seasons_apart_is_not_a_double_application(self):
        results = self._apply(
            [("25/09/2026 09:00", "100", "LTC", "1", "", ""),
             ("03/10/2027 09:00", "100", "TRI", "2", "", "")],
            [{"candidate_number": "100", "job_number": "1"}],
        )
        self.assertEqual(results[0]["double_application"], "NO")

    def test_the_flag_is_kept_once_the_other_application_closes(self):
        """History, not just live state: the two were open together, so the
        flag stays whatever became of the other one."""
        cases = {
            ("NO", "REJECTED"): "rejected at interview on 01 Sep 2026",
            ("YES", "APPROVED"): "approved",
            ("YES", "HOLD"): "on hold",
        }
        for (decision, approval), outcome in cases.items():
            results = self._apply(
                [("25/07/2026 09:00", "100", "LTC", "1", "", ""),
                 ("28/07/2026 09:00", "100", "TRI", "2", decision, approval,
                  "YES", "01/09/2026")],
                [{"candidate_number": "100", "job_number": "1"}],
            )
            self.assertEqual(results[0]["double_application"], "YES", outcome)
            self.assertIn(
                f"Also applied: TRI job 2 (FY26, submitted 28 Jul 2026) — {outcome}",
                results[0]["double_application_detail"],
            )

    def test_the_real_872524_case_flags_both_jobs(self):
        """LTC 17074 was not taken to interview; TRI 17092 was rejected at
        interview on 1 Sep. Both were live together on 28 Jul."""
        results = self._apply(
            [("25/07/2026 09:00", "872524", "LTC", "17074", "PENDING", "PENDING",
              "NO", ""),
             ("28/07/2026 09:00", "872524", "TRI", "17092", "NO", "REJECTED",
              "YES", "01/09/2026")],
            [{"candidate_number": "872524", "job_number": "17074"},
             {"candidate_number": "872524", "job_number": "17092"}],
        )
        self.assertEqual([r["double_application"] for r in results], ["YES", "YES"])
        self.assertIn("not taken to interview", results[1]["double_application_detail"])

    def test_an_application_closed_before_the_other_was_made_is_not_a_double(self):
        """Interviewed and rejected on 26 Jul; the 28 Jul one is a
        re-application, not a double."""
        results = self._apply(
            [("25/07/2026 09:00", "100", "LTC", "1", "NO", "REJECTED",
              "YES", "26/07/2026"),
             ("28/07/2026 09:00", "100", "TRI", "2", "", "")],
            [{"candidate_number": "100", "job_number": "1"},
             {"candidate_number": "100", "job_number": "2"}],
        )
        self.assertEqual([r["double_application"] for r in results], ["NO", "NO"])

    def test_a_row_without_a_job_number_falls_back_to_the_person(self):
        results = self._apply(
            [("25/07/2026 09:00", "100", "LTC", "1", "", ""),
             ("28/07/2026 09:00", "100", "TRI", "2", "", "")],
            [{"candidate_number": "100", "job_number": ""}],
        )
        self.assertEqual(results[0]["double_application"], "YES")

    def test_rows_the_list_cannot_judge_are_left_for_the_report_to_count(self):
        results = [{"candidate_number": "999", "job_number": "1"}]
        self._apply([("25/07/2026 09:00", "100", "LTC", "1", "", "")], results)
        self.assertNotIn("double_application", results[0])
        with redirect_stdout(io.StringIO()):
            main._apply_double_applications(results, "FY26", None)
        self.assertNotIn("double_application", results[0])


class TestJobNumberWiring(unittest.TestCase):
    """_apply_job_numbers resolves Job Number + Match Status for every row.

    A job number already on the result (parsed from a new-style filename)
    is cross-checked against the List, not trusted blindly. Every scenario
    ends in a result row that still carries its grading data — the join
    never drops a row, only leaves Job Number blank with a Match Status
    naming why.
    """

    def _list(self, rows):
        """rows are (created, staff, role, job_number)."""
        handle = tempfile.NamedTemporaryFile(
            "w", suffix=".csv", delete=False, encoding="utf-8-sig", newline=""
        )
        handle.write("Created,Staff Number,Position applied for,JOB NUMBER applied for\n")
        for created, staff, role, job_number in rows:
            handle.write(f"{created},{staff},{role},{job_number}\n")
        handle.close()
        return handle.name

    def _result(self, number="860775", role="LTC", job_number="", source_file=None):
        return {
            "candidate_number": number,
            "Role": role,
            "job_number": job_number,
            "source_file": source_file or f"{number}_{role}_assignment.pdf",
        }

    def test_one_matching_row_resolves_cleanly(self):
        path = self._list([("04/06/2026 09:00", "860775", "LTC", "1")])
        results = [self._result()]
        main._apply_job_numbers(results, "FY26", main._load_applications(path))
        self.assertEqual(results[0]["job_number"], "1")
        self.assertEqual(results[0]["match_status"], main.MATCH_STATUS_MATCHED)

    def test_a_job_number_already_on_the_result_is_verified_and_kept(self):
        """Parsed straight from a new-style filename — cross-checked, not
        blindly trusted, but kept when the List agrees."""
        path = self._list([("04/06/2026 09:00", "860775", "LTC", "17073")])
        results = [self._result(job_number="17073")]
        main._apply_job_numbers(results, "FY26", main._load_applications(path))
        self.assertEqual(results[0]["job_number"], "17073")
        self.assertEqual(results[0]["match_status"], main.MATCH_STATUS_MATCHED)

    def test_two_role_matching_rows_with_no_filename_job_number_are_fy_ambiguous(self):
        """Same role applied for twice, one physical file with no job
        number in its name — genuinely can't tell which attempt it is."""
        path = self._list([
            ("04/06/2026 09:00", "860775", "LTC", "1"),
            ("15/07/2026 09:00", "860775", "LTC", "17073"),
        ])
        results = [self._result()]
        buffer = io.StringIO()
        with redirect_stdout(buffer):
            main._apply_job_numbers(results, "FY26", main._load_applications(path))
        self.assertEqual(results[0]["job_number"], "")
        self.assertEqual(results[0]["match_status"], main.MATCH_FY_AMBIGUOUS)
        self.assertIn("NO JOB NUMBER IN FILENAME AND FY AMBIGUOUS", buffer.getvalue())

    def test_staff_number_absent_from_the_list_entirely(self):
        path = self._list([("04/06/2026 09:00", "999999", "LTC", "1")])
        results = [self._result(number="860775")]
        main._apply_job_numbers(results, "FY26", main._load_applications(path))
        self.assertEqual(results[0]["job_number"], "")
        self.assertEqual(results[0]["match_status"], main.MATCH_STAFF_NOT_IN_LIST)

    def test_no_list_available_reads_as_staff_not_in_list(self):
        results = [self._result()]
        main._apply_job_numbers(results, "FY26", None)
        self.assertEqual(results[0]["job_number"], "")
        self.assertEqual(results[0]["match_status"], main.MATCH_STAFF_NOT_IN_LIST)

    def test_staff_in_list_but_not_for_this_role_with_no_filename_job_number(self):
        """Candidate is on the List, just not for the role this filename
        claims — no job number to cross-check against, so it's a role
        mismatch, not a "not in list"."""
        path = self._list([("04/06/2026 09:00", "860775", "TRI", "1")])
        results = [self._result(role="LTC")]
        main._apply_job_numbers(results, "FY26", main._load_applications(path))
        self.assertEqual(results[0]["job_number"], "")
        self.assertEqual(results[0]["match_status"], main.MATCH_ROLE_MISMATCH)

    def test_filename_job_number_not_found_anywhere_for_this_staff(self):
        path = self._list([("04/06/2026 09:00", "860775", "LTC", "1")])
        results = [self._result(job_number="99999")]
        main._apply_job_numbers(results, "FY26", main._load_applications(path))
        self.assertEqual(results[0]["job_number"], "")
        self.assertEqual(results[0]["match_status"], main.MATCH_NO_MATCHING_ROW)

    def test_filename_job_number_found_under_a_different_role(self):
        """The job number is real, but the List has it down for a
        different role than the filename claims — a mistyped-role case."""
        path = self._list([("04/06/2026 09:00", "860775", "TRI", "17073")])
        results = [self._result(role="LTC", job_number="17073")]
        main._apply_job_numbers(results, "FY26", main._load_applications(path))
        self.assertEqual(results[0]["job_number"], "")
        self.assertEqual(results[0]["match_status"], main.MATCH_ROLE_MISMATCH)

    def test_filename_job_number_shared_by_two_list_rows(self):
        """A CSV data problem — two rows claim the same job number for
        this staff — rather than something to guess between."""
        path = self._list([
            ("04/06/2026 09:00", "860775", "LTC", "17073"),
            ("15/07/2026 09:00", "860775", "TRI", "17073"),
        ])
        results = [self._result(job_number="17073")]
        main._apply_job_numbers(results, "FY26", main._load_applications(path))
        self.assertEqual(results[0]["job_number"], "")
        self.assertEqual(results[0]["match_status"], main.MATCH_MULTIPLE_MATCHING_ROWS)

    def test_unmatched_rows_are_never_dropped_from_results(self):
        """The whole point: a failed join still leaves the row (and its
        grading data) in the list that gets written to the report."""
        results = [self._result(number="000000")]
        results[0]["classification"] = "Priority Interview"
        main._apply_job_numbers(results, "FY26", None)
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0]["classification"], "Priority Interview")
        self.assertEqual(results[0]["match_status"], main.MATCH_STAFF_NOT_IN_LIST)

    def test_summary_reports_totals_and_unmatched_filenames(self):
        path = self._list([("04/06/2026 09:00", "860775", "LTC", "1")])
        results = [
            self._result(number="860775", source_file="860775_LTC_assignment.pdf"),
            self._result(
                number="999999", role="TRI", source_file="999999_TRI_assignment.pdf"
            ),
        ]
        buffer = io.StringIO()
        with redirect_stdout(buffer):
            main._apply_job_numbers(results, "FY26", main._load_applications(path))
        output = buffer.getvalue()
        self.assertIn("1 of 2 matched", output)
        self.assertIn("STAFF NUMBER NOT IN LIST: 1", output)
        self.assertIn("999999_TRI_assignment.pdf: STAFF NUMBER NOT IN LIST", output)


class TestUnclaimedApplications(unittest.TestCase):
    """_unclaimed_applications finds List rows no essay file ever
    considered — the reverse of an unmatched file's Match Status.

    A row an essay file *considered* but couldn't cleanly resolve (ROLE
    MISMATCH, MULTIPLE MATCHING ROWS, FY AMBIGUOUS) is not "no assignment
    file" — some file plausibly belongs to it — so those must stay out.
    """

    def _app(self, staff, role, job_number, date="2026-06-04", campaign="FY26"):
        import datetime as dt
        import recruitment_list as rl
        return rl.Application(
            staff, dt.date.fromisoformat(date), role, "", "", campaign,
            "", "", job_number,
        )

    def _result(self, number, role, job_number=""):
        return {
            "candidate_number": number,
            "Role": role,
            "job_number": job_number,
            "source_file": f"{number}_{role}_assignment.pdf",
        }

    def test_the_850263_shape_one_job_number_claimed_the_other_is_not(self):
        """Two List rows for one role; only one has a written assignment."""
        apps = [
            self._app("850263", "LTC", "1", date="2026-06-04"),      # no file
            self._app("850263", "LTC", "17073", date="2026-07-25"),  # has one
        ]
        results = [self._result("850263", "LTC", job_number="17073")]
        referenced = main._apply_job_numbers(results, "FY26", apps)
        unclaimed = main._unclaimed_applications(apps, "FY26", referenced)
        self.assertEqual(len(unclaimed), 1)
        self.assertEqual(unclaimed[0]["job_number"], "1")
        self.assertEqual(unclaimed[0]["candidate_number"], "850263")

    def test_a_candidate_with_no_essay_file_at_all_is_unclaimed(self):
        apps = [self._app("999999", "LTC", "5")]
        referenced = main._apply_job_numbers([], "FY26", apps)
        unclaimed = main._unclaimed_applications(apps, "FY26", referenced)
        self.assertEqual(len(unclaimed), 1)
        self.assertEqual(unclaimed[0]["candidate_number"], "999999")

    def test_role_mismatch_via_job_number_keeps_the_row_claimed(self):
        """The job number is real, just filed under a different role in the
        filename — a file clearly exists for it, so it must not also show
        up as having no assignment on record."""
        apps = [self._app("1", "TRI", "17073")]
        results = [self._result("1", "LTC", job_number="17073")]  # wrong role
        referenced = main._apply_job_numbers(results, "FY26", apps)
        self.assertEqual(results[0]["match_status"], main.MATCH_ROLE_MISMATCH)
        unclaimed = main._unclaimed_applications(apps, "FY26", referenced)
        self.assertEqual(unclaimed, [])

    def test_multiple_matching_rows_keeps_both_rows_claimed(self):
        apps = [
            self._app("1", "LTC", "17073", date="2026-06-04"),
            self._app("1", "TRI", "17073", date="2026-07-25"),
        ]
        results = [self._result("1", "LTC", job_number="17073")]
        referenced = main._apply_job_numbers(results, "FY26", apps)
        self.assertEqual(results[0]["match_status"], main.MATCH_MULTIPLE_MATCHING_ROWS)
        unclaimed = main._unclaimed_applications(apps, "FY26", referenced)
        self.assertEqual(unclaimed, [])

    def test_fy_ambiguous_keeps_both_role_matches_claimed(self):
        """A file exists (no job number in its name) that could be either
        of two same-role rows — neither should read as unclaimed."""
        apps = [
            self._app("1", "LTC", "1", date="2026-06-04"),
            self._app("1", "LTC", "17073", date="2026-07-25"),
        ]
        results = [self._result("1", "LTC")]  # no job number in filename
        referenced = main._apply_job_numbers(results, "FY26", apps)
        self.assertEqual(results[0]["match_status"], main.MATCH_FY_AMBIGUOUS)
        unclaimed = main._unclaimed_applications(apps, "FY26", referenced)
        self.assertEqual(unclaimed, [])

    def test_no_applications_returns_empty(self):
        unclaimed = main._unclaimed_applications(None, "FY26", set())
        self.assertEqual(unclaimed, [])

    def test_a_different_campaigns_row_is_not_reported(self):
        apps = [self._app("1", "LTC", "1", campaign="FY25")]
        referenced = main._apply_job_numbers([], "FY26", apps)
        unclaimed = main._unclaimed_applications(apps, "FY26", referenced)
        self.assertEqual(unclaimed, [])


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

    def test_grade_scope_defaults_to_none_and_parses(self):
        self.assertIsNone(main._parse_args([]).grade_scope)
        self.assertEqual(
            main._parse_args(["--grade-scope", "new"]).grade_scope, "new"
        )
        self.assertEqual(
            main._parse_args(["--grade-scope", "all"]).grade_scope, "all"
        )

    def test_grade_scope_rejects_unknown_values(self):
        with self.assertRaises(SystemExit):
            main._parse_args(["--grade-scope", "bogus"])


class TestParseRoles(unittest.TestCase):
    """Shared by the CLI's --roles flag and the web layer's roles query param."""

    def test_none_and_empty_string_are_unscoped(self):
        self.assertIsNone(main.parse_roles(None))
        self.assertIsNone(main.parse_roles(""))

    def test_single_role(self):
        self.assertEqual(main.parse_roles("tri"), {"TRI"})

    def test_comma_separated_roles(self):
        self.assertEqual(main.parse_roles("TRI,TFO"), {"TRI", "TFO"})

    def test_a_role_containing_a_space_is_preserved(self):
        """'TFO TRI' is one role name, not two roles split on the space."""
        self.assertEqual(main.parse_roles("TFO TRI"), {"TFO TRI"})
        self.assertEqual(main.parse_roles("TRI,TFO TRI"), {"TRI", "TFO TRI"})

    def test_surrounding_whitespace_and_blank_entries_are_ignored(self):
        self.assertEqual(main.parse_roles(" TRI , , TFO "), {"TRI", "TFO"})


if __name__ == "__main__":
    unittest.main()
