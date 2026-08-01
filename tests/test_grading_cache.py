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
            cache = {
                "prompt_sha256": "abc",
                "candidates": {"1|LTC": {"x": 1, "prompt_sha256": "abc"}},
            }
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
        """Unscoped run: a rubric change makes every cached grade stale."""
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
# Role-scoped regrading
#
# The rule under test throughout: `roles` scopes REGRADING, never new work.
# ------------------------------------------------------------
class TestRoleScopedPartition(unittest.TestCase):
    def _seed_cache(self, essays, prompt):
        cache = gc._empty_cache()
        graded = [(e, _result(e["candidate_number"], e["role"])) for e in essays]
        gc.merge_and_update(cache, essays, graded, gc.prompt_hash(prompt), "v1.0")
        return cache

    def test_only_scoped_roles_are_regraded_when_rubric_changes(self):
        essays = [_essay("1", "LTC", "aaa"), _essay("2", "TRI", "bbb")]
        cache = self._seed_cache(essays, PROMPT_A)
        to_grade, reused = gc.partition(
            essays, cache, gc.prompt_hash(PROMPT_B), roles={"TRI"}
        )
        self.assertEqual([e["role"] for e in to_grade], ["TRI"])
        self.assertEqual(len(reused), 1)  # the stale LTC grade is kept as-is

    def test_new_essay_is_graded_even_when_its_role_is_out_of_scope(self):
        """The incremental path must never be suppressed by the role filter.

        A brand-new LTC submission has no grade at all; skipping it during a
        TRI-scoped run would silently drop it from the report.
        """
        cache = self._seed_cache([_essay("1", "TRI", "bbb")], PROMPT_A)
        essays = [_essay("1", "TRI", "bbb"), _essay("2", "LTC", "brand new")]
        to_grade, _ = gc.partition(
            essays, cache, gc.prompt_hash(PROMPT_A), roles={"TRI"}
        )
        self.assertEqual([e["candidate_number"] for e in to_grade], ["2"])

    def test_edited_essay_is_regraded_even_when_out_of_scope(self):
        cache = self._seed_cache([_essay("1", "LTC", "aaa")], PROMPT_A)
        edited = [_essay("1", "LTC", "aaa CHANGED")]
        to_grade, _ = gc.partition(
            edited, cache, gc.prompt_hash(PROMPT_A), roles={"TRI"}
        )
        self.assertEqual(len(to_grade), 1)

    def test_unchanged_prompt_grades_nothing_whatever_the_scope(self):
        essays = [_essay("1", "LTC", "aaa"), _essay("2", "TRI", "bbb")]
        cache = self._seed_cache(essays, PROMPT_A)
        to_grade, reused = gc.partition(
            essays, cache, gc.prompt_hash(PROMPT_A), roles={"TRI"}
        )
        self.assertEqual(to_grade, [])
        self.assertEqual(len(reused), 2)

    def test_out_of_scope_entries_stay_stale_after_merge(self):
        """A scoped run must not promote untouched rows to the new rubric."""
        essays = [_essay("1", "LTC", "aaa"), _essay("2", "TRI", "bbb")]
        cache = self._seed_cache(essays, PROMPT_A)
        new_hash = gc.prompt_hash(PROMPT_B)

        tri = _essay("2", "TRI", "bbb")
        gc.merge_and_update(
            cache, essays, [(tri, _result("2", "TRI"))], new_hash, "v2.0"
        )

        self.assertEqual(cache["candidates"]["2|TRI"]["prompt_sha256"], new_hash)
        self.assertEqual(cache["candidates"]["2|TRI"]["rubric_version"], "v2.0")
        # The LTC row keeps the rubric it was actually graded under.
        self.assertEqual(
            cache["candidates"]["1|LTC"]["prompt_sha256"], gc.prompt_hash(PROMPT_A)
        )
        self.assertEqual(cache["candidates"]["1|LTC"]["rubric_version"], "v1.0")
        self.assertEqual(gc.stale_keys(cache, new_hash), ["1|LTC"])

    def test_results_carry_rubric_version_and_currency(self):
        essays = [_essay("1", "LTC", "aaa"), _essay("2", "TRI", "bbb")]
        cache = self._seed_cache(essays, PROMPT_A)
        new_hash = gc.prompt_hash(PROMPT_B)
        tri = _essay("2", "TRI", "bbb")
        results, _ = gc.merge_and_update(
            cache, essays, [(tri, _result("2", "TRI"))], new_hash, "v2.0"
        )
        by_number = {r["candidate_number"]: r for r in results}
        self.assertTrue(by_number["2"]["rubric_is_current"])
        self.assertEqual(by_number["2"]["rubric_version"], "v2.0")
        self.assertFalse(by_number["1"]["rubric_is_current"])
        self.assertEqual(by_number["1"]["rubric_version"], "v1.0")


# ------------------------------------------------------------
# Rubric version parsing / legacy cache migration
# ------------------------------------------------------------
class TestRubricVersion(unittest.TestCase):
    def test_reads_version_token(self):
        prompt = "# Title\n\n## Version\nv9.3 — August 2026 (Adds: things)\n\n---\n"
        self.assertEqual(gc.rubric_version(prompt), "v9.3")

    def test_skips_blank_line_after_heading(self):
        prompt = "## Version\n\n\nv10.0 — later\n"
        self.assertEqual(gc.rubric_version(prompt), "v10.0")

    def test_missing_version_section_is_not_fatal(self):
        self.assertEqual(gc.rubric_version("no version here"), "")
        self.assertEqual(gc.rubric_version("## Version\n"), "")


class TestLegacyCacheMigration(unittest.TestCase):
    def test_entries_without_hashes_inherit_the_global_one(self):
        """Pre-migration caches must stay reusable, not force a full regrade."""
        import json

        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "cache.json"
            legacy = {
                "prompt_sha256": gc.prompt_hash(PROMPT_A),
                "candidates": {
                    "1|LTC": {
                        "candidate_number": "1",
                        "role": "LTC",
                        "source_file": "1_LTC_assignment.pdf",
                        "essay_sha256": gc.essay_hash("aaa"),
                        "essay_text": "aaa",
                        "result": _result("1", "LTC"),
                    }
                },
            }
            path.write_text(json.dumps(legacy), encoding="utf-8")

            cache = gc.load_cache(str(path))
            self.assertEqual(
                cache["candidates"]["1|LTC"]["prompt_sha256"], gc.prompt_hash(PROMPT_A)
            )
            to_grade, reused = gc.partition(
                [_essay("1", "LTC", "aaa")], cache, gc.prompt_hash(PROMPT_A)
            )
            self.assertEqual(to_grade, [])
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
