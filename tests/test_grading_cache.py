"""Unit tests for the incremental grading cache.

No API calls are made. Run from the project root with:

    venv\\Scripts\\python.exe -m unittest discover tests -v
"""

import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

import grading_cache as gc


# ------------------------------------------------------------
# Fixtures
# ------------------------------------------------------------
def _essay(number, role, text):
    return {
        "candidate_number": number,
        "role": role,
        "essay_text": text,
        "source_file": f"{number}_{role}_assignment.pdf",
    }


def _result(number, role, classification="Priority Interview"):
    return {"candidate_number": number, "Role": role, "classification": classification}


PROMPT_A = "Grade essays with rubric A."
PROMPT_B = "Grade essays with rubric B (changed)."


# ------------------------------------------------------------
# Hashing
# ------------------------------------------------------------
class TestHashing(unittest.TestCase):
    def test_prompt_hash_changes_with_prompt(self):
        self.assertNotEqual(gc.prompt_hash(PROMPT_A), gc.prompt_hash(PROMPT_B))

    def test_essay_hash_ignores_trailing_whitespace(self):
        self.assertEqual(
            gc.essay_hash("hello world"),
            gc.essay_hash("hello world   \n"),
        )

    def test_essay_hash_differs_on_real_edit(self):
        self.assertNotEqual(gc.essay_hash("hello world"), gc.essay_hash("hello there"))


# ------------------------------------------------------------
# Load / save round-trip
# ------------------------------------------------------------
class TestLoadSave(unittest.TestCase):
    def test_missing_file_returns_empty_skeleton(self):
        with tempfile.TemporaryDirectory() as d:
            cache = gc.load_cache(str(Path(d) / "nope.json"))
            self.assertEqual(cache, {"prompt_sha256": "", "candidates": {}})

    def test_corrupt_file_returns_empty_skeleton(self):
        with tempfile.TemporaryDirectory() as d:
            path = Path(d) / "cache.json"
            path.write_text("{ this is not valid json", encoding="utf-8")
            self.assertEqual(
                gc.load_cache(str(path)), {"prompt_sha256": "", "candidates": {}}
            )

    def test_save_then_load_round_trips(self):
        with tempfile.TemporaryDirectory() as d:
            path = str(Path(d) / "cache.json")
            cache = {"prompt_sha256": "abc", "candidates": {"1|LTC": {"x": 1}}}
            gc.save_cache(path, cache)
            self.assertEqual(gc.load_cache(path), cache)

    def test_save_is_atomic_no_tmp_left_behind(self):
        with tempfile.TemporaryDirectory() as d:
            path = Path(d) / "cache.json"
            gc.save_cache(str(path), gc._empty_cache())
            leftovers = [p.name for p in path.parent.iterdir()]
            self.assertEqual(leftovers, ["cache.json"])


# ------------------------------------------------------------
# Partition
# ------------------------------------------------------------
class TestPartition(unittest.TestCase):
    def _seed_cache(self, essays, prompt):
        """Build a cache as if `essays` had been graded under `prompt`."""
        cache = gc._empty_cache()
        graded = [(e, _result(e["candidate_number"], e["role"])) for e in essays]
        gc.merge_and_update(cache, essays, graded, gc.prompt_hash(prompt))
        return cache

    def test_stale_prompt_regrades_everything(self):
        essays = [_essay("1", "LTC", "aaa"), _essay("2", "TRI", "bbb")]
        cache = self._seed_cache(essays, PROMPT_A)
        to_grade, reused = gc.partition(essays, cache, gc.prompt_hash(PROMPT_B))
        self.assertEqual(len(to_grade), 2)
        self.assertEqual(reused, [])

    def test_unchanged_essay_is_reused(self):
        essays = [_essay("1", "LTC", "aaa")]
        cache = self._seed_cache(essays, PROMPT_A)
        to_grade, reused = gc.partition(essays, cache, gc.prompt_hash(PROMPT_A))
        self.assertEqual(to_grade, [])
        self.assertEqual(len(reused), 1)
        self.assertEqual(reused[0]["candidate_number"], "1")

    def test_edited_essay_is_regraded(self):
        cache = self._seed_cache([_essay("1", "LTC", "aaa")], PROMPT_A)
        edited = [_essay("1", "LTC", "aaa CHANGED")]
        to_grade, reused = gc.partition(edited, cache, gc.prompt_hash(PROMPT_A))
        self.assertEqual(len(to_grade), 1)
        self.assertEqual(reused, [])

    def test_new_candidate_is_graded_others_reused(self):
        cache = self._seed_cache([_essay("1", "LTC", "aaa")], PROMPT_A)
        essays = [_essay("1", "LTC", "aaa"), _essay("2", "TRI", "bbb")]
        to_grade, reused = gc.partition(essays, cache, gc.prompt_hash(PROMPT_A))
        self.assertEqual([e["candidate_number"] for e in to_grade], ["2"])
        self.assertEqual(len(reused), 1)

    def test_same_candidate_two_roles_are_independent(self):
        # A candidate applying for two roles: caching one must not shadow the other.
        cache = self._seed_cache([_essay("1", "LTC", "aaa")], PROMPT_A)
        essays = [_essay("1", "LTC", "aaa"), _essay("1", "TRI", "ccc")]
        to_grade, reused = gc.partition(essays, cache, gc.prompt_hash(PROMPT_A))
        self.assertEqual([e["role"] for e in to_grade], ["TRI"])
        self.assertEqual(len(reused), 1)


# ------------------------------------------------------------
# Merge / update
# ------------------------------------------------------------
class TestMergeAndUpdate(unittest.TestCase):
    def test_new_results_written_and_returned_in_folder_order(self):
        cache = gc._empty_cache()
        essays = [_essay("2", "TRI", "bbb"), _essay("1", "LTC", "aaa")]
        graded = [(e, _result(e["candidate_number"], e["role"])) for e in essays]
        results, plag = gc.merge_and_update(cache, essays, graded, gc.prompt_hash(PROMPT_A))
        self.assertEqual([r["candidate_number"] for r in results], ["2", "1"])
        self.assertEqual([p["candidate_number"] for p in plag], ["2", "1"])
        self.assertEqual(cache["prompt_sha256"], gc.prompt_hash(PROMPT_A))

    def test_removed_pdf_still_appears_via_cache(self):
        # Grade two, then run with only one PDF in the folder — the removed
        # candidate must survive in results and in the plagiarism corpus.
        cache = gc._empty_cache()
        both = [_essay("1", "LTC", "aaa"), _essay("2", "TRI", "bbb")]
        graded = [(e, _result(e["candidate_number"], e["role"])) for e in both]
        gc.merge_and_update(cache, both, graded, gc.prompt_hash(PROMPT_A))

        folder_now = [_essay("1", "LTC", "aaa")]
        results, plag = gc.merge_and_update(
            cache, folder_now, [], gc.prompt_hash(PROMPT_A)
        )
        numbers = sorted(r["candidate_number"] for r in results)
        self.assertEqual(numbers, ["1", "2"])
        # The cached-only essay retains its text for plagiarism comparison.
        removed = next(p for p in plag if p["candidate_number"] == "2")
        self.assertEqual(removed["essay_text"], "bbb")

    def test_reused_essay_keeps_its_cached_result(self):
        cache = gc._empty_cache()
        essays = [_essay("1", "LTC", "aaa")]
        graded = [(essays[0], _result("1", "LTC", "Do Not Interview"))]
        gc.merge_and_update(cache, essays, graded, gc.prompt_hash(PROMPT_A))

        # Second run: nothing graded, essay unchanged.
        results, _ = gc.merge_and_update(cache, essays, [], gc.prompt_hash(PROMPT_A))
        self.assertEqual(results[0]["classification"], "Do Not Interview")


if __name__ == "__main__":
    unittest.main()
