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


def _key(number, role, campaign=""):
    """The cache key, built the way the module builds it.

    Tests ask for keys through this rather than spelling the format out, so a
    change to the key layout shows up as a real failure rather than as dozens
    of string literals to hand-edit. An empty campaign resolves to FY26, which
    is what the production default does.
    """
    return gc._cache_key(campaign, number, role)


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
                "candidates": {_key("1", "LTC"): {"x": 1, "prompt_sha256": "abc"}},
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

        self.assertEqual(cache["candidates"][_key("2", "TRI")]["prompt_sha256"], new_hash)
        self.assertEqual(cache["candidates"][_key("2", "TRI")]["rubric_version"], "v2.0")
        # The LTC row keeps the rubric it was actually graded under.
        self.assertEqual(
            cache["candidates"][_key("1", "LTC")]["prompt_sha256"],
            gc.prompt_hash(PROMPT_A),
        )
        self.assertEqual(cache["candidates"][_key("1", "LTC")]["rubric_version"], "v1.0")
        self.assertEqual(gc.stale_keys(cache, new_hash), [_key("1", "LTC")])

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
# classify() — the reasons behind partition()'s split
# ------------------------------------------------------------
class TestClassify(unittest.TestCase):
    def _seed_cache(self, essays, prompt):
        cache = gc._empty_cache()
        graded = [(e, _result(e["candidate_number"], e["role"])) for e in essays]
        gc.merge_and_update(cache, essays, graded, gc.prompt_hash(prompt), "v1.0")
        return cache

    def _reasons(self, essays, cache, prompt, roles=None):
        return [r for _, r in gc.classify(essays, cache, gc.prompt_hash(prompt), roles)]

    def test_each_reason_is_produced(self):
        seeded = [_essay("1", "LTC", "aaa"), _essay("2", "TRI", "bbb")]
        cache = self._seed_cache(seeded, PROMPT_A)
        essays = [
            _essay("1", "LTC", "aaa"),           # unchanged, stale rubric, out of scope
            _essay("2", "TRI", "bbb"),           # unchanged, stale rubric, in scope
            _essay("3", "TRI", "brand new"),     # never seen
            _essay("1", "TRI", "different role"),  # new: role makes a distinct key
        ]
        self.assertEqual(
            self._reasons(essays, cache, PROMPT_B, roles={"TRI"}),
            [
                gc.REASON_OUT_OF_SCOPE,
                gc.REASON_STALE_RUBRIC,
                gc.REASON_NEW,
                gc.REASON_NEW,
            ],
        )

    def test_changed_text_is_distinguished_from_new(self):
        cache = self._seed_cache([_essay("1", "LTC", "aaa")], PROMPT_A)
        essays = [_essay("1", "LTC", "aaa EDITED"), _essay("9", "LTC", "unseen")]
        self.assertEqual(
            self._reasons(essays, cache, PROMPT_A),
            [gc.REASON_CHANGED, gc.REASON_NEW],
        )

    def test_changed_text_ignores_role_scope(self):
        """An extractor change must not be hidden by --roles."""
        cache = self._seed_cache([_essay("1", "LTC", "aaa")], PROMPT_A)
        essays = [_essay("1", "LTC", "aaa REEXTRACTED")]
        self.assertEqual(
            self._reasons(essays, cache, PROMPT_A, roles={"TRI"}),
            [gc.REASON_CHANGED],
        )

    def test_current_grades_are_reported_as_current(self):
        cache = self._seed_cache([_essay("1", "LTC", "aaa")], PROMPT_A)
        self.assertEqual(
            self._reasons([_essay("1", "LTC", "aaa")], cache, PROMPT_A),
            [gc.REASON_CURRENT],
        )

    def test_grade_reasons_cover_exactly_the_costly_ones(self):
        self.assertEqual(
            set(gc.GRADE_REASONS),
            {
                gc.REASON_NEW,
                gc.REASON_CHANGED,
                gc.REASON_REEXTRACTED,
                gc.REASON_STALE_RUBRIC,
            },
        )

    def test_every_reason_has_a_label(self):
        for reason in (
            gc.REASON_NEW, gc.REASON_CHANGED, gc.REASON_REEXTRACTED,
            gc.REASON_STALE_RUBRIC, gc.REASON_CURRENT, gc.REASON_OUT_OF_SCOPE,
        ):
            self.assertIn(reason, gc.REASON_LABELS)

    def test_agrees_with_partition_across_every_case(self):
        """The dry run is worthless if it can disagree with a real run."""
        cache = self._seed_cache(
            [_essay("1", "LTC", "aaa"), _essay("2", "TRI", "bbb")], PROMPT_A
        )
        essays = [
            _essay("1", "LTC", "aaa"),
            _essay("2", "TRI", "bbb EDITED"),
            _essay("3", "TRI", "new one"),
        ]
        for prompt in (PROMPT_A, PROMPT_B):
            for roles in (None, {"TRI"}, {"LTC"}, {"TRI", "LTC"}):
                with self.subTest(prompt=prompt, roles=roles):
                    h = gc.prompt_hash(prompt)
                    to_grade, reused = gc.partition(essays, cache, h, roles)
                    classified = gc.classify(essays, cache, h, roles)
                    expected = [e for e, r in classified if r in gc.GRADE_REASONS]
                    self.assertEqual(to_grade, expected)
                    self.assertEqual(len(to_grade) + len(reused), len(essays))


# ------------------------------------------------------------
# File identity: an extractor change must not look like a resubmission
# ------------------------------------------------------------
def _file_essay(number, role, text, file_hash):
    essay = _essay(number, role, text)
    essay["file_sha256"] = file_hash
    return essay


class TestFileIdentity(unittest.TestCase):
    """The submission is identified by its bytes, not by what we read out."""

    def _seed(self, essays, prompt=None):
        cache = gc._empty_cache()
        graded = [(e, _result(e["candidate_number"], e["role"])) for e in essays]
        gc.merge_and_update(
            cache, essays, graded, gc.prompt_hash(prompt or PROMPT_A), "v1.0"
        )
        return cache

    def _reason(self, essay, cache, prompt=None):
        classified = gc.classify(
            [essay], cache, gc.prompt_hash(prompt or PROMPT_A)
        )
        return classified[0][1]

    def test_same_file_read_differently_is_not_a_regrade(self):
        """The whole fix: an extractor change costs nothing."""
        cache = self._seed([_file_essay("1", "LTC", "prac\x10cal", "FILE-A")])
        reread = _file_essay("1", "LTC", "practical", "FILE-A")  # same file
        self.assertEqual(self._reason(reread, cache), gc.REASON_CURRENT)

    def test_different_file_is_a_resubmission(self):
        cache = self._seed([_file_essay("1", "LTC", "aaa", "FILE-A")])
        resubmitted = _file_essay("1", "LTC", "aaa", "FILE-A-V2")
        self.assertEqual(self._reason(resubmitted, cache), gc.REASON_CHANGED)

    def test_identical_file_and_text_is_current(self):
        cache = self._seed([_file_essay("1", "LTC", "aaa", "FILE-A")])
        self.assertEqual(
            self._reason(_file_essay("1", "LTC", "aaa", "FILE-A"), cache),
            gc.REASON_CURRENT,
        )

    def test_bumping_the_extractor_version_forces_a_reread(self):
        """The deliberate key still works."""
        cache = self._seed([_file_essay("1", "LTC", "aaa", "FILE-A")])
        cache["candidates"][_key("1", "LTC")]["extractor_version"] = "0"  # graded by an older one
        self.assertEqual(
            self._reason(_file_essay("1", "LTC", "aaa", "FILE-A"), cache),
            gc.REASON_REEXTRACTED,
        )

    def test_resubmission_beats_a_version_bump(self):
        """A changed file is reported as a resubmission, not a re-extraction."""
        cache = self._seed([_file_essay("1", "LTC", "aaa", "FILE-A")])
        cache["candidates"][_key("1", "LTC")]["extractor_version"] = "0"
        self.assertEqual(
            self._reason(_file_essay("1", "LTC", "aaa", "FILE-B"), cache),
            gc.REASON_CHANGED,
        )

    def test_legacy_entry_without_a_file_hash_falls_back_to_text(self):
        """Upgrading must not invalidate a single existing grade."""
        cache = self._seed([_essay("1", "LTC", "aaa")])  # no file_sha256
        cache["candidates"][_key("1", "LTC")].pop("file_sha256", None)

        unchanged = _essay("1", "LTC", "aaa")
        self.assertEqual(self._reason(unchanged, cache), gc.REASON_CURRENT)
        edited = _essay("1", "LTC", "aaa EDITED")
        self.assertEqual(self._reason(edited, cache), gc.REASON_CHANGED)

    def test_unreadable_file_hash_falls_back_to_text(self):
        cache = self._seed([_file_essay("1", "LTC", "aaa", "FILE-A")])
        unreadable = _file_essay("1", "LTC", "aaa", "")  # hashing failed
        self.assertEqual(self._reason(unreadable, cache), gc.REASON_CURRENT)

    def test_entry_records_both_new_fields(self):
        cache = self._seed([_file_essay("1", "LTC", "aaa", "FILE-A")])
        entry = cache["candidates"][_key("1", "LTC")]
        self.assertEqual(entry["file_sha256"], "FILE-A")
        self.assertEqual(entry["extractor_version"], gc.EXTRACTOR_VERSION)


class TestStaleExtractions(unittest.TestCase):
    """Reusing a grade built from different text must not be silent."""

    def _seed(self, essays):
        cache = gc._empty_cache()
        graded = [(e, _result(e["candidate_number"], e["role"])) for e in essays]
        gc.merge_and_update(cache, essays, graded, gc.prompt_hash(PROMPT_A), "v1.0")
        return cache

    def test_drift_without_a_version_bump_is_reported(self):
        cache = self._seed([_file_essay("1", "LTC", "aaa", "FILE-A")])
        reread = [_file_essay("1", "LTC", "aaa read differently", "FILE-A")]
        drifted = gc.stale_extractions(reread, cache)
        self.assertEqual([e["candidate_number"] for e in drifted], ["1"])

    def test_no_drift_when_text_is_identical(self):
        cache = self._seed([_file_essay("1", "LTC", "aaa", "FILE-A")])
        same = [_file_essay("1", "LTC", "aaa", "FILE-A")]
        self.assertEqual(gc.stale_extractions(same, cache), [])

    def test_resubmissions_are_not_reported_as_drift(self):
        """Already regrading — warning about it too would be noise."""
        cache = self._seed([_file_essay("1", "LTC", "aaa", "FILE-A")])
        resubmitted = [_file_essay("1", "LTC", "totally new", "FILE-B")]
        self.assertEqual(gc.stale_extractions(resubmitted, cache), [])

    def test_deliberate_reextraction_is_not_reported_as_drift(self):
        cache = self._seed([_file_essay("1", "LTC", "aaa", "FILE-A")])
        cache["candidates"][_key("1", "LTC")]["extractor_version"] = "0"
        reread = [_file_essay("1", "LTC", "aaa better", "FILE-A")]
        self.assertEqual(gc.stale_extractions(reread, cache), [])

    def test_unknown_candidate_is_ignored(self):
        self.assertEqual(
            gc.stale_extractions([_file_essay("9", "TRI", "x", "F")], gc._empty_cache()),
            [],
        )


# ------------------------------------------------------------
# Campaign scoping: each recruitment year starts from scratch
# ------------------------------------------------------------
class TestCampaignScoping(unittest.TestCase):
    def _graded(self, essays, campaign):
        cache = gc._empty_cache()
        graded = [(e, _result(e["candidate_number"], e["role"])) for e in essays]
        gc.merge_and_update(
            cache, essays, graded, gc.prompt_hash(PROMPT_A), "v1.0", campaign
        )
        return cache

    def test_a_grade_records_the_campaign_it_was_produced_in(self):
        cache = self._graded([_essay("1", "LTC", "aaa")], "FY27")
        self.assertEqual(cache["candidates"][_key("1", "LTC", "FY27")]["campaign"], "FY27")
        self.assertTrue(cache["candidates"][_key("1", "LTC", "FY27")]["graded_at"])

    def test_a_returning_candidate_does_not_overwrite_last_years_grade(self):
        """The regression this key layout exists to prevent.

        A candidate who applied in FY26 and re-applies in FY27 for the SAME
        role used to land on the same cache key, destroying the FY26 grade —
        and they are precisely the candidates the embargo flags, so last year's
        grade is the one a reviewer most wants to see.
        """
        fy26 = [_essay("1", "LTC", "last year")]
        cache = self._graded(fy26, "FY26")

        fy27 = [_essay("1", "LTC", "this year")]
        gc.merge_and_update(
            cache, fy27, [(fy27[0], _result("1", "LTC", "Maybe"))],
            gc.prompt_hash(PROMPT_B), "v2.0", "FY27",
        )

        self.assertEqual(len(cache["candidates"]), 2)
        old = cache["candidates"][_key("1", "LTC", "FY26")]
        new = cache["candidates"][_key("1", "LTC", "FY27")]
        self.assertEqual(old["essay_text"], "last year")
        self.assertEqual(new["essay_text"], "this year")
        self.assertEqual(old["rubric_version"], "v1.0")
        self.assertEqual(old["result"]["classification"], "Priority Interview")
        self.assertEqual(new["result"]["classification"], "Maybe")

    def test_last_years_report_still_rebuilds_after_a_returning_candidate(self):
        """--fy FY26 must still reproduce the FY26 row, not the FY27 one."""
        fy26 = [_essay("1", "LTC", "last year")]
        cache = self._graded(fy26, "FY26")
        fy27 = [_essay("1", "LTC", "this year")]
        gc.merge_and_update(
            cache, fy27, [(fy27[0], _result("1", "LTC", "Maybe"))],
            gc.prompt_hash(PROMPT_B), "v2.0", "FY27",
        )

        results, _ = gc.merge_and_update(
            cache, [], [], gc.prompt_hash(PROMPT_A), "v1.0", "FY26"
        )
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0]["classification"], "Priority Interview")
        self.assertEqual(results[0]["rubric_version"], "v1.0")

    def test_a_returning_candidate_is_new_work_not_a_resubmission(self):
        """No FY27 entry exists yet, so it is NEW. It grades either way — the
        point is that last year's grade is not this year's starting point."""
        cache = self._graded([_essay("1", "LTC", "last year")], "FY26")
        classified = gc.classify(
            [_essay("1", "LTC", "this year")], cache,
            gc.prompt_hash(PROMPT_A), None, "FY27",
        )
        self.assertEqual([r for _, r in classified], [gc.REASON_NEW])

    def test_re_applying_for_a_different_role_also_stays_separate(self):
        cache = self._graded([_essay("1", "LTC", "aaa")], "FY26")
        fy27 = [_essay("1", "TRI", "bbb")]
        gc.merge_and_update(
            cache, fy27, [(fy27[0], _result("1", "TRI"))],
            gc.prompt_hash(PROMPT_A), "v1.0", "FY27",
        )
        self.assertEqual(
            sorted(cache["candidates"]),
            sorted([_key("1", "LTC", "FY26"), _key("1", "TRI", "FY27")]),
        )

    def test_stale_keys_can_be_scoped_to_one_campaign(self):
        """Last year's grades are out of scope by design, not work left undone."""
        cache = self._graded([_essay("1", "LTC", "aaa")], "FY26")
        fy27 = [_essay("2", "TRI", "bbb")]
        gc.merge_and_update(
            cache, fy27, [(fy27[0], _result("2", "TRI"))],
            gc.prompt_hash(PROMPT_A), "v1.0", "FY27",
        )
        new_hash = gc.prompt_hash(PROMPT_B)
        self.assertEqual(len(gc.stale_keys(cache, new_hash)), 2)
        self.assertEqual(gc.stale_keys(cache, new_hash, "FY27"),
                         [_key("2", "TRI", "FY27")])

    def test_results_carry_their_campaign(self):
        cache = self._graded([_essay("1", "LTC", "aaa")], "FY26")
        results, _ = gc.merge_and_update(
            cache, [], [], gc.prompt_hash(PROMPT_A), "v1.0", "FY26"
        )
        self.assertEqual(results[0]["campaign"], "FY26")

    def test_entries_without_a_campaign_backfill_to_the_legacy_one(self):
        """Grades predating the field were all produced during FY26."""
        self.assertEqual(gc.campaign_of({}), gc.LEGACY_CAMPAIGN)
        self.assertEqual(gc.campaign_of({"campaign": ""}), gc.LEGACY_CAMPAIGN)
        self.assertEqual(gc.campaign_of({"campaign": "FY27"}), "FY27")

    def test_report_contains_only_the_active_campaign(self):
        cache = self._graded([_essay("1", "LTC", "aaa")], "FY26")
        # A new year, a new candidate, the old one's PDF removed.
        new = [_essay("2", "TRI", "bbb")]
        results, plag = gc.merge_and_update(
            cache, new, [(new[0], _result("2", "TRI"))],
            gc.prompt_hash(PROMPT_A), "v1.0", "FY27",
        )
        self.assertEqual([r["candidate_number"] for r in results], ["2"])
        # The plagiarism corpus is scoped too — last year's essays are not
        # compared against this year's.
        self.assertEqual([p["candidate_number"] for p in plag], ["2"])

    def test_excluded_campaigns_are_kept_in_the_cache(self):
        """Nothing is deleted — a past year's report can still be rebuilt."""
        cache = self._graded([_essay("1", "LTC", "aaa")], "FY26")
        new = [_essay("2", "TRI", "bbb")]
        gc.merge_and_update(
            cache, new, [(new[0], _result("2", "TRI"))],
            gc.prompt_hash(PROMPT_A), "v1.0", "FY27",
        )
        self.assertIn(_key("1", "LTC", "FY26"), cache["candidates"])
        self.assertEqual(
            cache["candidates"][_key("1", "LTC", "FY26")]["campaign"], "FY26"
        )

    def test_an_earlier_campaign_can_be_reported_again(self):
        cache = self._graded([_essay("1", "LTC", "aaa")], "FY26")
        results, _ = gc.merge_and_update(
            cache, [], [], gc.prompt_hash(PROMPT_A), "v1.0", "FY26"
        )
        self.assertEqual([r["candidate_number"] for r in results], ["1"])

    def test_reused_entry_keeps_its_original_campaign(self):
        """A later run must not silently absorb last year's grades."""
        essays = [_essay("1", "LTC", "aaa")]
        cache = self._graded(essays, "FY26")
        gc.merge_and_update(
            cache, essays, [], gc.prompt_hash(PROMPT_A), "v1.0", "FY27"
        )
        self.assertEqual(
            cache["candidates"][_key("1", "LTC", "FY26")]["campaign"], "FY26"
        )

    def test_no_campaign_means_no_filtering(self):
        cache = self._graded([_essay("1", "LTC", "aaa")], "FY26")
        results, _ = gc.merge_and_update(
            cache, [], [], gc.prompt_hash(PROMPT_A), "v1.0"
        )
        self.assertEqual(len(results), 1)


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
                cache["candidates"][_key("1", "LTC")]["prompt_sha256"], gc.prompt_hash(PROMPT_A)
            )
            to_grade, reused = gc.partition(
                [_essay("1", "LTC", "aaa")], cache, gc.prompt_hash(PROMPT_A)
            )
            self.assertEqual(to_grade, [])
            self.assertEqual(len(reused), 1)

    def test_pre_campaign_keys_gain_the_campaign_exactly_once(self):
        """Loading twice must not produce 'FY26|FY26|1|LTC'."""
        legacy = {"1|LTC": {"campaign": "FY26", "result": {}}}
        once = gc._migrate_keys(legacy)
        self.assertEqual(list(once), [_key("1", "LTC", "FY26")])
        self.assertEqual(gc._migrate_keys(once), once)

    def test_a_key_is_migrated_using_its_own_campaign(self):
        migrated = gc._migrate_keys({"1|LTC": {"campaign": "FY27", "result": {}}})
        self.assertEqual(list(migrated), [_key("1", "LTC", "FY27")])

    def test_a_role_containing_a_space_migrates_intact(self):
        migrated = gc._migrate_keys({"872524|TFO TRI": {"result": {}}})
        self.assertEqual(list(migrated), [_key("872524", "TFO TRI", "FY26")])


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
