"""Unit tests for recruitment-campaign (financial year) resolution.

The campaign decides what lands in the report, so the failure that matters most
is a malformed setting resolving to something no cached grade matches — that
would produce a silently empty report. It must raise instead.

No files outside a temp directory are touched. Run from the project root with:

    venv\\Scripts\\python.exe -m unittest discover tests -v
"""

import datetime as dt
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

import campaign as cp


def _file(tmp: Path, contents: str) -> Path:
    path = tmp / "campaign.txt"
    path.write_text(contents, encoding="utf-8")
    return path


# ------------------------------------------------------------
# The 1 October boundary
# ------------------------------------------------------------
class TestFyForDate(unittest.TestCase):
    def test_boundary_either_side_of_1_october(self):
        for date, expected in [
            (dt.date(2026, 9, 30), "FY26"),   # last day of FY26
            (dt.date(2026, 10, 1), "FY27"),   # first day of FY27
        ]:
            with self.subTest(date=date):
                self.assertEqual(cp.fy_for_date(date), expected)

    def test_a_financial_year_is_named_for_the_year_it_ends_in(self):
        self.assertEqual(cp.fy_for_date(dt.date(2025, 10, 1)), "FY26")
        self.assertEqual(cp.fy_for_date(dt.date(2026, 8, 15)), "FY26")
        self.assertEqual(cp.fy_for_date(dt.date(2026, 9, 30)), "FY26")

    def test_spans_the_calendar_year_change(self):
        self.assertEqual(cp.fy_for_date(dt.date(2026, 12, 31)), "FY27")
        self.assertEqual(cp.fy_for_date(dt.date(2027, 1, 1)), "FY27")

    def test_defaults_to_today(self):
        self.assertEqual(cp.fy_for_date(), cp.fy_for_date(dt.date.today()))


# ------------------------------------------------------------
# Reading the setting
# ------------------------------------------------------------
class TestActiveCampaign(unittest.TestCase):
    def test_reads_the_value(self):
        with tempfile.TemporaryDirectory() as tmp:
            self.assertEqual(
                cp.active_campaign(_file(Path(tmp), "FY26\n")), "FY26"
            )

    def test_ignores_comments_and_blank_lines(self):
        """The real file leads with an explanation of the rollover."""
        with tempfile.TemporaryDirectory() as tmp:
            contents = "# which campaign we are on\n\n   \nFY27\n"
            self.assertEqual(
                cp.active_campaign(_file(Path(tmp), contents)), "FY27"
            )

    def test_normalises_case_and_whitespace(self):
        with tempfile.TemporaryDirectory() as tmp:
            self.assertEqual(
                cp.active_campaign(_file(Path(tmp), "  fy27  \n")), "FY27"
            )

    def test_missing_file_falls_back_to_the_date(self):
        """A fresh checkout must still run."""
        missing = Path(tempfile.gettempdir()) / "definitely_not_here_12345.txt"
        self.assertEqual(cp.active_campaign(missing), cp.fy_for_date())

    def test_empty_file_falls_back_to_the_date(self):
        with tempfile.TemporaryDirectory() as tmp:
            self.assertEqual(
                cp.active_campaign(_file(Path(tmp), "# only a comment\n")),
                cp.fy_for_date(),
            )

    def test_malformed_value_raises_rather_than_emptying_the_report(self):
        """A campaign nothing matches would silently produce an empty report."""
        for bad in ["F26", "FY2026", "2026", "FY", "FY6", "next year"]:
            with self.subTest(value=bad), tempfile.TemporaryDirectory() as tmp:
                with self.assertRaises(ValueError) as ctx:
                    cp.active_campaign(_file(Path(tmp), bad + "\n"))
                self.assertIn("not a valid", str(ctx.exception))


# ------------------------------------------------------------
# The staleness warning
# ------------------------------------------------------------
class TestLooksStale(unittest.TestCase):
    def test_setting_behind_the_date_is_stale(self):
        self.assertTrue(cp.looks_stale("FY26", dt.date(2026, 10, 1)))

    def test_setting_matching_the_date_is_not(self):
        self.assertFalse(cp.looks_stale("FY26", dt.date(2026, 8, 15)))

    def test_setting_ahead_of_the_date_is_not_stale(self):
        """Deliberately ahead is the whole reason the setting is manual —
        FY27 applications arrive during September, while FY26 is still live."""
        self.assertFalse(cp.looks_stale("FY27", dt.date(2026, 9, 15)))


if __name__ == "__main__":
    unittest.main()
