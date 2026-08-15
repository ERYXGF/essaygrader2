"""Unit tests for the Detailed sheet's per-question summary columns.

Candidates answer the written task in whatever order they like, so the Q1/Q2/Q3
columns are filled by canonical question_number rather than by position in the
question_assessments list. These tests pin that behaviour, plus the fallbacks
for legacy results and error rows.

No API calls are made. Run from the project root with:

    venv\\Scripts\\python.exe -m unittest discover tests -v
"""

import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from openpyxl import load_workbook

import report_writer as rw
import essay_grader as eg


# ------------------------------------------------------------
# Fixtures
# ------------------------------------------------------------
def _assessment(number, summary, key=""):
    return {
        "question_number": number,
        "question_key": key,
        "question_text": "",
        "summary": summary,
        "quality": "Good",
        "relevance": "High",
        "competencies_demonstrated": [],
    }


def _result(number, role, assessments):
    return {
        "candidate_number": number,
        "Role": role,
        "classification": "Priority Interview",
        "rationale": "",
        "cross_cutting_assessment": {},
        "strengths": [],
        "weaknesses": [],
        "writing_quality": {},
        "ai_usage_probability": "Low",
        "ai_usage_indicators": "",
        "question_assessments": assessments,
        "source_file": f"{number}_{role}_assignment.pdf",
    }


# ------------------------------------------------------------
# Column mapping
# ------------------------------------------------------------
class TestQuestionSummaries(unittest.TestCase):
    def test_out_of_sequence_answers_land_in_canonical_columns(self):
        """A TRI candidate who wrote 3, 1, 2 still lines up with everyone else."""
        summaries = rw._question_summaries([
            _assessment(3, "most challenging", "tri-most-challenging"),
            _assessment(1, "slats guidance", "tri-slats"),
            _assessment(2, "sbt design", "tri-sbt-behaviours"),
        ])
        self.assertEqual(summaries[1], "slats guidance")
        self.assertEqual(summaries[2], "sbt design")
        self.assertEqual(summaries[3], "most challenging")

    def test_two_question_role_leaves_q3_empty(self):
        """TFO has only two questions."""
        summaries = rw._question_summaries([
            _assessment(1, "ground school", "tfo-ground-school"),
            _assessment(2, "feedback", "tfo-feedback"),
        ])
        self.assertEqual(summaries.get(3, ""), "")

    def test_unnumbered_assessments_fall_back_to_position(self):
        """Legacy cached results carry no question_number."""
        summaries = rw._question_summaries([
            {"summary": "first"},
            {"summary": "second"},
            {"summary": "third"},
        ])
        self.assertEqual(summaries[1], "first")
        self.assertEqual(summaries[2], "second")
        self.assertEqual(summaries[3], "third")

    def test_string_question_numbers_are_accepted(self):
        summaries = rw._question_summaries([_assessment("2", "sbt design")])
        self.assertEqual(summaries[2], "sbt design")
        self.assertNotIn(1, summaries)

    def test_positional_fallback_never_overwrites_a_numbered_answer(self):
        summaries = rw._question_summaries([
            _assessment(1, "numbered slats"),
            {"summary": "unnumbered stray"},
        ])
        self.assertEqual(summaries[1], "numbered slats")

    def test_empty_and_malformed_input(self):
        self.assertEqual(rw._question_summaries([]), {})
        self.assertEqual(rw._question_summaries(["not a dict"]), {})

    def test_out_of_range_number_is_dropped_not_guessed_into_q1(self):
        """Better a blank cell than the wrong answer under the Q1 header."""
        self.assertEqual(rw._question_summaries([_assessment(9, "out of range")]), {})


# ------------------------------------------------------------
# End-to-end through the workbook
# ------------------------------------------------------------
class TestDetailedSheet(unittest.TestCase):
    def _write_and_read(self, results):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "report.xlsx"
            rw.write_report(results, str(path))
            ws = load_workbook(path)["Detailed"]
            return [[c.value for c in row] for row in ws.iter_rows()]

    def test_headers_and_row_values(self):
        rows = self._write_and_read([
            _result("100001", "TRI", [
                _assessment(3, "most challenging"),
                _assessment(1, "slats guidance"),
                _assessment(2, "sbt design"),
            ]),
        ])
        headers = rows[0]
        self.assertEqual(headers[-3:], ["Q1 Summary", "Q2 Summary", "Q3 Summary"])
        self.assertEqual(rows[1][-3:], ["slats guidance", "sbt design", "most challenging"])

    def test_error_row_writes_blanks_rather_than_raising(self):
        error_row = eg._error_result("100002", "TRI", "Empty essay submission")
        rows = self._write_and_read([error_row])
        # openpyxl reads an empty-string cell back as None.
        self.assertEqual([v or "" for v in rows[1][-3:]], ["", "", ""])


# ------------------------------------------------------------
# Double Application column
# ------------------------------------------------------------
class TestDoubleApplication(unittest.TestCase):
    def test_same_number_under_two_roles_is_flagged(self):
        numbers = rw._double_application_numbers([
            {"candidate_number": "872524", "Role": "TRI"},
            {"candidate_number": "872524", "Role": "TFO TRI"},
            {"candidate_number": "111111", "Role": "LTC"},
        ])
        self.assertEqual(numbers, {"872524"})

    def test_single_role_candidate_is_not_flagged(self):
        self.assertEqual(
            rw._double_application_numbers([{"candidate_number": "1", "Role": "LTC"}]),
            set(),
        )

    def test_three_roles_still_flags_once(self):
        numbers = rw._double_application_numbers([
            {"candidate_number": "7", "Role": "LTC"},
            {"candidate_number": "7", "Role": "TRI"},
            {"candidate_number": "7", "Role": "TFO TRI"},
        ])
        self.assertEqual(numbers, {"7"})

    def test_rows_without_a_candidate_number_are_ignored(self):
        """Blank numbers must not group into a phantom double applicant."""
        numbers = rw._double_application_numbers([
            {"Role": "LTC"}, {"Role": "TRI"}, {"candidate_number": "", "Role": "TFO"},
        ])
        self.assertEqual(numbers, set())


class TestSummarySheet(unittest.TestCase):
    def _summary(self, results):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "report.xlsx"
            rw.write_report(results, str(path))
            ws = load_workbook(path)["Summary"]
            headers = [c.value for c in ws[1]]
            rows = [[c.value for c in row] for row in ws.iter_rows(min_row=2)]
            fills = {
                h: [ws.cell(row=r, column=headers.index(h) + 1).fill
                    for r in range(2, ws.max_row + 1)]
                for h in headers
            }
            return headers, rows, fills

    def test_column_reports_yes_and_no(self):
        headers, rows, _ = self._summary([
            _result("872524", "TRI", []),
            _result("872524", "TFO TRI", []),
            _result("111111", "LTC", []),
        ])
        col = headers.index("Double Application")
        self.assertEqual([r[col] for r in rows], ["Yes", "Yes", "No"])

    def test_classification_band_still_lands_on_the_right_cell(self):
        """Regression: the colour band used to assume Classification was 3rd.

        Inserting Double Application ahead of it would silently colour the
        wrong column.
        """
        headers, rows, fills = self._summary([_result("1", "LTC", [])])
        self.assertGreater(
            headers.index("Classification"), 2,
            "Classification is no longer 3rd — this test guards that move",
        )
        green = "00C6EFCE"
        self.assertEqual(fills["Classification"][0].start_color.rgb, green)
        # And the column it displaced must NOT have been coloured.
        self.assertNotEqual(
            fills["Double Application"][0].start_color.rgb, green
        )


# ------------------------------------------------------------
# Grader-side normalisation
# ------------------------------------------------------------
class TestNormaliseQuestionAssessments(unittest.TestCase):
    def test_sorts_into_canonical_order_and_coerces_numbers(self):
        normalised = eg._normalise_question_assessments([
            _assessment("3", "third"),
            _assessment(1.0, "first"),
            _assessment(2, "second"),
        ])
        self.assertEqual([a["question_number"] for a in normalised], [1, 2, 3])
        self.assertEqual([a["summary"] for a in normalised], ["first", "second", "third"])

    def test_unnumbered_entries_sort_last_in_original_order(self):
        normalised = eg._normalise_question_assessments([
            {"summary": "stray a"},
            _assessment(1, "numbered"),
            {"summary": "stray b"},
        ])
        self.assertEqual(
            [a["summary"] for a in normalised],
            ["numbered", "stray a", "stray b"],
        )
        self.assertIsNone(normalised[1]["question_number"])

    def test_non_list_and_non_dict_input_is_dropped(self):
        self.assertEqual(eg._normalise_question_assessments(None), [])
        self.assertEqual(eg._normalise_question_assessments("nope"), [])
        self.assertEqual(eg._normalise_question_assessments(["nope"]), [])

    def test_normalise_result_applies_it(self):
        result = eg._normalise_result(
            {"question_assessments": [_assessment(2, "second"), _assessment(1, "first")]},
            "100003",
            "TRI",
        )
        self.assertEqual(
            [a["question_number"] for a in result["question_assessments"]], [1, 2]
        )

    def test_does_not_mutate_the_caller_s_dicts(self):
        original = [{"question_number": "1", "summary": "first"}]
        eg._normalise_question_assessments(original)
        self.assertEqual(original[0]["question_number"], "1")


if __name__ == "__main__":
    unittest.main()
