"""Unit tests for filename parsing and role recognition.

The case that matters most: "TFO TRI" contains a space, and the pattern used to
accept only letters. An unmatched filename raises out of load_essays(), which
aborts the whole run — so a single new-role submission would have stopped the
pipeline before anything was graded.

No files are read. Run from the project root with:

    venv\\Scripts\\python.exe -m unittest discover tests -v
"""

import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

import pdf_loader


class TestParseFilename(unittest.TestCase):
    def test_existing_roles_are_unaffected(self):
        for name, expected in [
            ("12345_LTC_assignment.pdf", ("12345", "LTC")),
            ("12345_TRI_assignment.pdf", ("12345", "TRI")),
            ("12345_TFO_assignment.pdf", ("12345", "TFO")),
        ]:
            with self.subTest(name=name):
                self.assertEqual(pdf_loader._parse_filename(name), expected)

    def test_tfo_tri_with_a_space_parses(self):
        """The real-world naming: 872524_TFO TRI_assignment.pdf."""
        self.assertEqual(
            pdf_loader._parse_filename("872524_TFO TRI_assignment.pdf"),
            ("872524", "TFO TRI"),
        )

    def test_underscored_spelling_normalises_to_the_same_role(self):
        """A filename typo must not create a second, parallel role."""
        self.assertEqual(
            pdf_loader._parse_filename("872524_TFO_TRI_assignment.pdf"),
            ("872524", "TFO TRI"),
        )

    def test_non_pdf_extension_still_parses(self):
        """Wrong container is flagged later, not rejected here."""
        self.assertEqual(
            pdf_loader._parse_filename("999_TFO TRI_assignment.docx"),
            ("999", "TFO TRI"),
        )

    def test_unrecognised_role_still_raises_helpfully(self):
        with self.assertRaises(ValueError) as ctx:
            pdf_loader._parse_filename("12345_XX_assignment.pdf")
        self.assertIn("unrecognised role", str(ctx.exception))

    def test_role_like_gibberish_is_rejected(self):
        """Allowing spaces must not turn the role into a free-text field."""
        for name in [
            "12345_TFO TRI TRI_assignment.pdf",
            "12345_TFO LTC_assignment.pdf",
        ]:
            with self.subTest(name=name):
                with self.assertRaises(ValueError):
                    pdf_loader._parse_filename(name)

    def test_malformed_names_still_raise(self):
        for name in [
            "no_number_LTC_assignment.pdf",
            "12345_LTC.pdf",
            "12345_LTC_assignment",
            "12345LTC_assignment.pdf",
        ]:
            with self.subTest(name=name):
                with self.assertRaises(ValueError):
                    pdf_loader._parse_filename(name)


class TestValidRoles(unittest.TestCase):
    def test_all_four_roles_are_accepted(self):
        self.assertEqual(
            pdf_loader.VALID_ROLES, {"LTC", "TFO", "TRI", "TFO TRI"}
        )

    def test_aliases_resolve_to_a_valid_role(self):
        for alias, canonical in pdf_loader.ROLE_ALIASES.items():
            with self.subTest(alias=alias):
                self.assertIn(canonical, pdf_loader.VALID_ROLES)
                self.assertNotIn(alias, pdf_loader.VALID_ROLES)

    def test_tri_and_tfo_tri_are_distinct_submissions(self):
        """One candidate may hold both; they must not collide in the cache."""
        tri = pdf_loader._parse_filename("872524_TRI_assignment.pdf")
        tfo_tri = pdf_loader._parse_filename("872524_TFO TRI_assignment.pdf")
        self.assertEqual(tri[0], tfo_tri[0])   # same candidate
        self.assertNotEqual(tri[1], tfo_tri[1])  # different role


if __name__ == "__main__":
    unittest.main()
