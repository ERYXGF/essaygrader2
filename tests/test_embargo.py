"""Unit tests for the recruitment list loader and the re-application embargo.

The failure that matters most is a **false clear**: a candidate who re-applied
inside the six-month window but comes back unflagged. Most of these tests exist
to pin the edges where that could happen — date parsing, campaign boundaries,
and the window boundary itself.

No API calls are made. Run from the project root with:

    venv/bin/python -m unittest discover tests -v
"""

import datetime as dt
import os
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

import embargo as eb
import recruitment_list as rl


# ------------------------------------------------------------
# Fixtures
# ------------------------------------------------------------
HEADERS = (
    "Created,Created By,Email,Staff Number,Name,BASE,Position applied for,"
    "\U0001f7e3 APPLICATION SUCCESSFUL \U0001f7e3,OUTCOME"
)


def _csv(rows, headers=HEADERS):
    """Writes a List export to a temp file and returns its path."""
    handle = tempfile.NamedTemporaryFile(
        "w", suffix=".csv", delete=False, encoding="utf-8-sig", newline=""
    )
    handle.write(headers + "\n")
    for row in rows:
        handle.write(row + "\n")
    handle.close()
    return Path(handle.name)


def _row(created, staff, role="TRI", successful="", outcome=""):
    return f"{created},someone,a@b.com,{staff},A Name,LGW,{role},{successful},{outcome}"


def _app(staff, date, role="TRI", outcome=""):
    return rl.Application(staff, date, role, outcome, "")


# The OData export straight from SharePoint: internal header names with
# _xHHHH_ escapes, ISO 8601 dates, and choice fields wrapped in JSON.
ODATA_HEADERS = (
    "@odata.etag,ID,Title,Created,Staff_x0020_Number,Name,"
    "Position_x0020_applied_x0020_for,Position_x0020_applied_x0020_for#Id,"
    "Have_x0020_you_x0020_applied_x00,Have_x0020_you_x0020_applied_x00#Id,"
    "APPLICATIONSUCCESSFUL,FINALAPPROVAL"
)


def _choice(value, ident=0):
    inner = (
        '{""@odata.type"":""#Microsoft.Azure.Connectors.SharePoint.'
        'SPListExpandedReference"",""Id"":%d,""Value"":""%s""}' % (ident, value)
    )
    return f'"{inner}"'


def _odata_row(created, staff, role="TRI"):
    return ",".join([
        "etag", "1", "Title", created, staff, "A Name",
        _choice(role), "2", _choice("NO"), "1",
        _choice("YES"), _choice("PENDING"),
    ])


def _date(text):
    return dt.datetime.strptime(text, "%Y-%m-%d").date()


# ------------------------------------------------------------
# Loading the export
# ------------------------------------------------------------
class TestLoadApplications(unittest.TestCase):
    def test_reads_rows_and_parses_uk_dates(self):
        path = _csv([_row("01/04/2026 11:56", "872524", "LTC")])
        apps = rl.load_applications(path)
        self.assertEqual(len(apps), 1)
        self.assertEqual(apps[0].staff_number, "872524")
        self.assertEqual(apps[0].submitted_at, _date("2026-04-01"))
        self.assertEqual(apps[0].role, "LTC")

    def test_day_comes_first_never_the_month(self):
        """01/04/2026 is 1 April. Read the American way it would be 4 January —
        a nine-month error, easily enough to clear a six-month embargo."""
        path = _csv([_row("01/04/2026 11:56", "1")])
        self.assertEqual(rl.load_applications(path)[0].submitted_at.month, 4)

    def test_headers_are_matched_through_their_emoji(self):
        apps = rl.load_applications(
            _csv([_row("01/04/2026 11:56", "1", successful="YES")])
        )
        self.assertEqual(apps[0].successful, "YES")

    def test_date_without_a_time_is_accepted(self):
        path = _csv([_row("28/07/2026", "1")])
        self.assertEqual(rl.load_applications(path)[0].submitted_at, _date("2026-07-28"))

    def test_rows_without_a_staff_number_or_date_are_skipped_and_counted(self):
        path = _csv([
            _row("01/04/2026 11:56", "1"),
            _row("01/04/2026 11:56", ""),      # no staff number
            _row("not a date", "2"),            # unreadable date
        ])
        apps, skipped = rl.load_report(path)
        self.assertEqual([a.staff_number for a in apps], ["1"])
        self.assertEqual(skipped, 2)

    def test_blank_trailing_lines_are_not_counted_as_skipped(self):
        path = _csv([_row("01/04/2026 11:56", "1"), ",,,,,,,,"])
        _, skipped = rl.load_report(path)
        self.assertEqual(skipped, 0)

    def test_missing_required_column_raises(self):
        """A silently empty list would read as 'nobody has applied before',
        which clears every candidate — the one wrong answer worth crashing on."""
        path = _csv([], headers="Created,Name,OUTCOME")
        with self.assertRaises(ValueError) as caught:
            rl.load_applications(path)
        self.assertIn("staff number", str(caught.exception).lower())

    def test_missing_file_raises_file_not_found(self):
        with self.assertRaises(FileNotFoundError):
            rl.load_applications(Path("/nonexistent/list.csv"))

    def test_reads_the_odata_export_shape(self):
        """SharePoint's own export: escaped headers, ISO dates, JSON values."""
        path = _csv(
            [_odata_row("2026-06-04T13:26:07Z", "860775", "LTC")],
            headers=ODATA_HEADERS,
        )
        apps = rl.load_applications(path)
        self.assertEqual(len(apps), 1)
        self.assertEqual(apps[0].staff_number, "860775")
        self.assertEqual(apps[0].submitted_at, _date("2026-06-04"))
        self.assertEqual(apps[0].role, "LTC")        # unwrapped from JSON
        self.assertEqual(apps[0].outcome, "PENDING")  # FINALAPPROVAL

    def test_a_companion_id_column_does_not_shadow_its_real_column(self):
        """'Position applied for#Id' shares a prefix with the column we want."""
        path = _csv(
            [_odata_row("2026-06-04T13:26:07Z", "1", "TRI")], headers=ODATA_HEADERS
        )
        self.assertEqual(rl.load_applications(path)[0].role, "TRI")

    def test_created_is_not_confused_with_created_by(self):
        """Both start with 'created'; only an exact match may win."""
        path = _csv(
            [_row("28/07/2026 09:00", "1")],
            headers="Created,Created By,Email,Staff Number,Name,BASE,"
                    "Position applied for,OUTCOME,X",
        )
        self.assertEqual(rl.load_applications(path)[0].submitted_at, _date("2026-07-28"))

    def test_unwrap_leaves_plain_values_alone(self):
        self.assertEqual(rl.unwrap("LTC"), "LTC")
        self.assertEqual(rl.unwrap(""), "")
        self.assertEqual(rl.unwrap("{not json"), "{not json")

    def test_applications_come_back_oldest_first(self):
        path = _csv([
            _row("28/07/2026 09:00", "3"),
            _row("05/02/2026 09:00", "1"),
            _row("18/05/2026 09:00", "2"),
        ])
        apps = rl.load_applications(path)
        self.assertEqual([a.staff_number for a in apps], ["1", "2", "3"])


# ------------------------------------------------------------
# Month arithmetic
# ------------------------------------------------------------
class TestSubtractMonths(unittest.TestCase):
    def test_plain_case(self):
        self.assertEqual(eb.subtract_months(_date("2026-10-15"), 6), _date("2026-04-15"))

    def test_crosses_the_year(self):
        self.assertEqual(eb.subtract_months(_date("2027-02-10"), 6), _date("2026-08-10"))

    def test_clamps_to_a_real_day(self):
        """31 August minus 6 months is 28 February, not an error."""
        self.assertEqual(eb.subtract_months(_date("2026-08-31"), 6), _date("2026-02-28"))


# ------------------------------------------------------------
# The embargo rule
# ------------------------------------------------------------
class TestFindEmbargoes(unittest.TestCase):
    def test_reapplying_inside_the_window_is_flagged(self):
        apps = [
            _app("100", _date("2026-07-28"), "TRI"),   # FY26
            _app("100", _date("2026-10-15"), "LTC"),   # FY27, 79 days later
        ]
        found = eb.find_embargoes(apps, "FY27")
        self.assertIn("100", found)
        self.assertEqual(found["100"].days_apart, 79)
        self.assertEqual(found["100"].prior_campaign, "FY26")

    def test_reapplying_outside_the_window_is_clear(self):
        apps = [
            _app("100", _date("2026-02-05")),          # FY26
            _app("100", _date("2026-10-15")),          # FY27, 252 days later
        ]
        self.assertEqual(eb.find_embargoes(apps, "FY27"), {})

    def test_a_different_role_still_counts(self):
        """The embargo attaches to the person, not the post."""
        apps = [
            _app("100", _date("2026-07-28"), "TRI"),
            _app("100", _date("2026-10-01"), "TFO"),
        ]
        self.assertIn("100", eb.find_embargoes(apps, "FY27"))

    def test_two_roles_in_the_same_campaign_do_not_flag_each_other(self):
        """The six real double applicants submit 0-4 days apart. Flagging them
        would make the column useless noise."""
        apps = [
            _app("872524", _date("2026-07-25"), "TRI"),
            _app("872524", _date("2026-07-28"), "TFO TRI"),
        ]
        self.assertEqual(eb.find_embargoes(apps, "FY26"), {})

    def test_the_window_boundary_is_inclusive(self):
        apps = [
            _app("100", _date("2026-04-15")),
            _app("100", _date("2026-10-15")),  # exactly six months
        ]
        self.assertIn("100", eb.find_embargoes(apps, "FY27"))

    def test_one_day_beyond_the_window_is_clear(self):
        apps = [
            _app("100", _date("2026-04-14")),
            _app("100", _date("2026-10-15")),
        ]
        self.assertEqual(eb.find_embargoes(apps, "FY27"), {})

    def test_the_shortest_gap_is_the_one_reported(self):
        apps = [
            _app("100", _date("2026-05-20")),
            _app("100", _date("2026-07-28")),
            _app("100", _date("2026-10-15")),
        ]
        self.assertEqual(eb.find_embargoes(apps, "FY27")["100"].days_apart, 79)

    def test_candidates_not_in_this_campaign_are_ignored(self):
        apps = [_app("100", _date("2026-07-28")), _app("200", _date("2026-02-05"))]
        self.assertEqual(eb.find_embargoes(apps, "FY27"), {})

    def test_a_first_time_applicant_is_clear(self):
        self.assertEqual(
            eb.find_embargoes([_app("100", _date("2026-10-15"))], "FY27"), {}
        )

    def test_a_later_campaign_cannot_trigger_an_embargo(self):
        """Only history counts. A future application is not a prior one."""
        apps = [
            _app("100", _date("2026-10-15")),  # FY27
            _app("100", _date("2026-11-20")),  # FY27 as well
        ]
        self.assertEqual(eb.find_embargoes(apps, "FY27"), {})

    def test_the_campaign_boundary_itself(self):
        """30 Sep is FY26, 1 Oct is FY27 — one day apart, still a
        re-application, and well inside the window."""
        apps = [
            _app("100", _date("2026-09-30")),
            _app("100", _date("2026-10-01")),
        ]
        found = eb.find_embargoes(apps, "FY27")
        self.assertEqual(found["100"].days_apart, 1)


class TestFinancialYear(unittest.TestCase):
    """The List declares the campaign; that beats deriving it from the date."""

    def test_the_exported_value_is_quoted(self):
        """The raw cell is literally `'2026'`, apostrophes and all."""
        self.assertEqual(rl.parse_financial_year("'2026'"), "FY26")

    def test_other_spellings(self):
        for raw in ("2026", " 2026 ", "FY26", "FY2026", "'2027'"):
            expected = "FY27" if "27" in raw else "FY26"
            self.assertEqual(rl.parse_financial_year(raw), expected, raw)

    def test_unreadable_values_yield_nothing_rather_than_a_guess(self):
        for raw in ("", None, "N/A", "financial year", "20267"):
            self.assertEqual(rl.parse_financial_year(raw), "")

    def test_the_declared_year_wins_over_the_date(self):
        """The case this exists for: a campaign can open before 1 October, so a
        September application can legitimately belong to the next FY."""
        september = rl.Application(
            "100", _date("2026-09-20"), "TRI", "", "", "FY27"
        )
        self.assertEqual(rl.campaign_of_application(september), "FY27")
        # What the date alone would have said, and why it is not enough:
        from campaign import fy_for_date
        self.assertEqual(fy_for_date(september.submitted_at), "FY26")

    def test_the_date_is_used_when_no_year_is_declared(self):
        """Exports predating the FINANCIALYEAR column still work."""
        legacy = rl.Application("100", _date("2026-07-28"), "TRI", "", "", "")
        self.assertEqual(rl.campaign_of_application(legacy), "FY26")

    def test_an_early_next_year_application_can_still_be_embargoed(self):
        """Read by date, the September row would be FY26 and could never be
        flagged against the July FY26 one."""
        apps = [
            rl.Application("100", _date("2026-07-28"), "TRI", "", "", "FY26"),
            rl.Application("100", _date("2026-09-20"), "LTC", "", "", "FY27"),
        ]
        found = eb.find_embargoes(apps, "FY27")
        self.assertIn("100", found)
        self.assertEqual(found["100"].days_apart, 54)


class TestFindExport(unittest.TestCase):
    """Locating the nightly export, which is named for the day it was taken."""

    def _dir(self, names):
        folder = Path(tempfile.mkdtemp())
        for name in names:
            (folder / name).write_text("Created,Staff Number\n", encoding="utf-8")
        return folder

    def test_the_newest_filename_date_wins(self):
        folder = self._dir([
            "Recruitment_Export_2026-08-06.csv",
            "Recruitment_Export_2026-07-14.csv",
            "Recruitment_Export_2026-08-15.csv",
        ])
        self.assertEqual(
            rl.find_export(folder).name, "Recruitment_Export_2026-08-15.csv"
        )

    def test_mtime_is_ignored_in_favour_of_the_filename(self):
        """Copying a file resets its mtime — the same trap that made submission
        dates undetectable in the first place. The name carries the real date."""
        folder = self._dir([
            "Recruitment_Export_2026-08-15.csv",
            "Recruitment_Export_2026-01-02.csv",
        ])
        # Make the OLD export look freshly modified.
        os.utime(folder / "Recruitment_Export_2026-01-02.csv", (2 ** 31 - 1,) * 2)
        self.assertEqual(
            rl.find_export(folder).name, "Recruitment_Export_2026-08-15.csv"
        )

    def test_lowercase_names_are_found(self):
        folder = self._dir(["recruitment_export_2026-08-06.csv"])
        self.assertEqual(
            rl.find_export(folder).name, "recruitment_export_2026-08-06.csv"
        )

    def test_an_undated_export_is_used_when_it_is_all_there_is(self):
        folder = self._dir(["Recruitment_Export_final.csv"])
        self.assertEqual(rl.find_export(folder).name, "Recruitment_Export_final.csv")

    def test_a_dated_export_beats_an_undated_one(self):
        folder = self._dir([
            "Recruitment_Export_final.csv",
            "Recruitment_Export_2026-08-06.csv",
        ])
        self.assertEqual(
            rl.find_export(folder).name, "Recruitment_Export_2026-08-06.csv"
        )

    def test_the_hand_renamed_fallback_still_works(self):
        folder = self._dir(["recruitment_list.csv"])
        self.assertEqual(rl.find_export(folder).name, "recruitment_list.csv")

    def test_nothing_found_raises_and_says_where_it_looked(self):
        """A missing export must never be mistaken for 'nobody applied'."""
        folder = self._dir([])
        with self.assertRaises(FileNotFoundError) as caught:
            rl.find_export(folder)
        message = str(caught.exception)
        self.assertIn(str(folder), message)
        self.assertIn("Recruitment_Export", message)


class TestSubmittedDates(unittest.TestCase):
    """Resolving the date one graded submission actually arrived."""

    def test_maps_candidate_campaign_and_role_to_a_date(self):
        dates = rl.submitted_dates([_app("100", _date("2026-07-28"), "TRI")])
        self.assertEqual(dates[("100", "FY26", "TRI")], _date("2026-07-28"))

    def test_the_later_of_two_same_role_applications_wins(self):
        """Real case: 860775 has FY26 LTC rows on 4 Jun and 15 Jul, but only
        one graded essay — and the essay on file is the later submission."""
        dates = rl.submitted_dates([
            _app("860775", _date("2026-06-04"), "LTC"),
            _app("860775", _date("2026-07-15"), "LTC"),
        ])
        self.assertEqual(dates[("860775", "FY26", "LTC")], _date("2026-07-15"))

    def test_the_same_role_in_two_campaigns_stays_separate(self):
        dates = rl.submitted_dates([
            _app("100", _date("2026-07-28"), "LTC"),
            _app("100", _date("2026-10-15"), "LTC"),
        ])
        self.assertEqual(dates[("100", "FY26", "LTC")], _date("2026-07-28"))
        self.assertEqual(dates[("100", "FY27", "LTC")], _date("2026-10-15"))

    def test_lookup_falls_back_to_the_campaign_when_the_role_differs(self):
        dates = rl.submitted_dates([_app("100", _date("2026-07-28"), "LTC")])
        self.assertEqual(
            rl.submitted_for(dates, "100", "FY26", "TFO TRI"), _date("2026-07-28")
        )

    def test_an_unknown_candidate_returns_nothing_rather_than_a_guess(self):
        dates = rl.submitted_dates([_app("100", _date("2026-07-28"), "LTC")])
        self.assertIsNone(rl.submitted_for(dates, "999", "FY26", "LTC"))

    def test_the_wrong_campaign_returns_nothing(self):
        dates = rl.submitted_dates([_app("100", _date("2026-07-28"), "LTC")])
        self.assertIsNone(rl.submitted_for(dates, "100", "FY27", "LTC"))


class TestDescribe(unittest.TestCase):
    def test_names_the_campaign_gap_and_role(self):
        found = eb.find_embargoes([
            _app("100", _date("2026-07-28"), "LTC"),
            _app("100", _date("2026-10-15"), "TRI"),
        ], "FY27")
        text = eb.describe(found["100"])
        self.assertTrue(text.startswith("⚠"))
        for expected in ("79d", "FY26", "28 Jul 2026", "LTC"):
            self.assertIn(expected, text)


if __name__ == "__main__":
    unittest.main()
