"""Unit tests for PDF text-quality detection and the OCR fallback decision.

These cover the choice logic only — no PDFs are rendered and no OCR runs, so
the suite stays fast and passes on machines without the macOS Vision stack.

Run from the project root with:

    venv\\Scripts\\python.exe -m unittest discover tests -v
"""

import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

import ocr
import text_extractors as tx


# ------------------------------------------------------------
# Damaged-glyph detection
# ------------------------------------------------------------
class TestBrokenGlyphRatio(unittest.TestCase):
    def test_clean_text_scores_zero(self):
        self.assertEqual(tx.broken_glyph_ratio("perfectly ordinary prose"), 0.0)

    def test_empty_text_is_not_a_division_error(self):
        self.assertEqual(tx.broken_glyph_ratio(""), 0.0)

    def test_unmapped_ligature_glyphs_are_counted(self):
        # 'practical' as it arrives when the font declares no ToUnicode entry
        # for its 'ti' ligature.
        self.assertGreater(tx.broken_glyph_ratio("prac\x10cal"), 0)

    def test_ordinary_whitespace_is_not_damage(self):
        self.assertEqual(tx.broken_glyph_ratio("line one\nline two\ttabbed\r\n"), 0.0)

    def test_real_world_corruption_clears_the_threshold(self):
        # 850237 carried 71 damaged characters in 6,846 — about 1%.
        text = ("a" * 6775) + ("\x10" * 71)
        self.assertGreater(tx.broken_glyph_ratio(text), tx.BROKEN_GLYPH_RATIO)


# ------------------------------------------------------------
# Choosing between the embedded text layer and OCR
# ------------------------------------------------------------
class TestBetterText(unittest.TestCase):
    def test_sound_embedded_text_is_never_traded_for_ocr(self):
        """The candidate's literal words beat a transcription of them."""
        embedded = "The candidate writes clearly about slat faults. " * 10
        recognised = "OCR misreading of the same page. " * 15
        self.assertEqual(tx._better_text(embedded, recognised), embedded)

    def test_damaged_embedded_text_is_replaced_by_ocr(self):
        embedded = ("prac\x10cal guidance " * 40).strip()
        recognised = ("practical guidance " * 40).strip()
        self.assertEqual(tx._better_text(embedded, recognised), recognised)

    def test_ocr_is_rejected_when_it_lost_most_of_the_page(self):
        """Losing whole answers is worse than a scatter of broken words."""
        embedded = ("prac\x10cal guidance " * 40).strip()
        recognised = "practical"
        self.assertEqual(tx._better_text(embedded, recognised), embedded)

    def test_empty_embedded_text_always_takes_ocr(self):
        self.assertEqual(tx._better_text("", "recovered by OCR"), "recovered by OCR")


# ------------------------------------------------------------
# OCR backend availability
# ------------------------------------------------------------
class TestOcrAvailability(unittest.TestCase):
    def test_is_available_is_a_bool_and_never_raises(self):
        self.assertIsInstance(ocr.is_available(), bool)

    def test_ocr_pdf_returns_none_when_backend_missing(self):
        """Absent OCR must degrade to the embedded text, not kill the run."""
        original = ocr._BACKEND
        ocr._BACKEND = None
        try:
            self.assertFalse(ocr.is_available())
            self.assertIsNone(ocr.ocr_pdf(Path("does_not_matter.pdf")))
        finally:
            ocr._BACKEND = original

    def test_unopenable_file_returns_none_rather_than_raising(self):
        if not ocr.is_available():
            self.skipTest("OCR backend unavailable on this machine")
        self.assertIsNone(ocr.ocr_pdf(Path("/nonexistent/nope.pdf")))


if __name__ == "__main__":
    unittest.main()
