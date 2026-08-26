"""Unit tests for the grading progress bar.

The bar is presentation only, so the failures worth guarding are the ones that
damage something else on its way past:

  - control characters leaking into a redirected run's log file
  - a glyph the console cannot encode crashing the run it reports on
  - a wrapped line turning the bar into a stuttering mess on a narrow terminal
  - `log()` from deep in the call stack blowing up when no bar is active

Nothing here touches the network or the filesystem. Run from the project root:

    venv/bin/python -m unittest discover tests -v
"""

import io
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

import progress


class FakeStream(io.StringIO):
    """A StringIO that can claim to be a terminal, with a chosen encoding.

    `encoding` is a read-only slot on the C-level StringIO, so it is shadowed
    with a property rather than assigned.
    """

    def __init__(self, tty: bool = True, encoding: str = "utf-8"):
        super().__init__()
        self._tty = tty
        self._encoding = encoding

    @property
    def encoding(self) -> str:
        return self._encoding

    def isatty(self) -> bool:
        return self._tty


def _bar(**kwargs) -> progress.Bar:
    """A bar on a fake terminal, wide enough not to drop anything."""
    stream = kwargs.pop("stream", None) or FakeStream()
    return progress.Bar(kwargs.pop("total", 10), stream=stream, width=100, **kwargs)


class TestRegistryIsLeftClean(unittest.TestCase):
    """Every test below assumes it starts with no bar active."""

    def setUp(self):
        progress._stack.clear()

    def tearDown(self):
        self.assertEqual(progress._stack, [], "a test leaked an active bar")


# ------------------------------------------------------------
# Not a terminal: today's behaviour, and no escape characters
# ------------------------------------------------------------
class TestNonTty(TestRegistryIsLeftClean):
    def test_no_control_characters_reach_a_redirected_stream(self):
        stream = FakeStream(tty=False)
        with progress.Bar(3, stream=stream, width=100) as bar:
            bar.set_current("860775 TRI")
            bar.advance("✓ 860775 TRI")
            bar.log("     ! ConnectionError; retrying")
            bar.advance("✓ 850237 TRI")

        written = stream.getvalue()
        for forbidden in ("\r", "\x1b"):
            self.assertNotIn(forbidden, written)

    def test_one_line_per_item_carrying_the_counter(self):
        stream = FakeStream(tty=False)
        with progress.Bar(2, stream=stream, width=100) as bar:
            bar.advance("✓ 860775 TRI  Priority Interview")
            bar.advance("✓ 850237 TRI  Maybe")

        lines = stream.getvalue().splitlines()
        self.assertEqual(len(lines), 2)
        # Off a terminal there is no bar to read the count from, so the line
        # carries it instead.
        self.assertIn("1/2", lines[0])
        self.assertIn("860775 TRI", lines[0])
        self.assertIn("2/2", lines[1])

    def test_set_current_draws_nothing_at_all(self):
        stream = FakeStream(tty=False)
        with progress.Bar(2, stream=stream, width=100) as bar:
            bar.set_current("860775 TRI")
        self.assertEqual(stream.getvalue(), "")


# ------------------------------------------------------------
# A terminal: redraw in place, with other output above
# ------------------------------------------------------------
class TestLiveBar(TestRegistryIsLeftClean):
    def test_the_bar_is_the_last_thing_on_the_stream_after_a_log(self):
        stream = FakeStream()
        with progress.Bar(10, stream=stream, width=100) as bar:
            bar.advance()
            bar.log("     ! Invalid JSON; asking Claude to correct it...")
            written = stream.getvalue()

        # The message is printed, and something is redrawn after it.
        message_at = written.index("Invalid JSON")
        self.assertIn("1/10", written[message_at:])
        self.assertTrue(written[message_at:].rstrip().endswith("%")
                        or "elapsed" in written[message_at:])

    def test_each_redraw_returns_to_the_start_of_the_line_and_clears_it(self):
        stream = FakeStream()
        with progress.Bar(10, stream=stream, width=100) as bar:
            bar.advance()
            bar.advance()
        # \r\x1b[K is what stops two redraws piling up on one line.
        self.assertGreaterEqual(stream.getvalue().count("\r\x1b[K"), 3)

    def test_a_logged_line_sits_above_the_bar_not_inside_it(self):
        stream = FakeStream()
        with progress.Bar(10, stream=stream, width=100) as bar:
            bar.log("hello")
        # The message ends its own line before the bar is drawn again.
        self.assertIn("hello\n", stream.getvalue())

    def test_the_in_flight_label_appears_while_the_counts_stand_still(self):
        stream = FakeStream()
        with progress.Bar(10, stream=stream, width=100) as bar:
            bar.set_current("860775 TRI")
            drawn = stream.getvalue()
        self.assertIn("860775 TRI", drawn)
        self.assertIn("0/10", drawn)

    def test_the_finished_bar_stays_in_the_scrollback(self):
        stream = FakeStream()
        bar = progress.Bar(2, stream=stream, width=100)
        bar.__enter__()
        bar.advance()
        bar.advance()
        bar.close()
        self.assertTrue(stream.getvalue().endswith("\n"))
        self.assertIn("2/2", stream.getvalue().splitlines()[-1])

    def test_the_completed_line_carries_no_stale_in_flight_label(self):
        stream = FakeStream()
        with progress.Bar(1, stream=stream, width=100) as bar:
            bar.set_current("860775 TRI")
            bar.advance()
        self.assertNotIn("860775 TRI", stream.getvalue().splitlines()[-1])


# ------------------------------------------------------------
# The ETA
# ------------------------------------------------------------
class TestEta(TestRegistryIsLeftClean):
    def test_absent_until_there_is_something_to_average(self):
        """Durations are set directly: a real advance() takes microseconds,
        which the sub-second guard would hide for its own reasons."""
        bar = _bar(total=10)
        try:
            for done in range(1, progress.MIN_SAMPLES_FOR_ETA):
                bar.done, bar._per_item = done, [30.0] * done
                self.assertNotIn("left", bar._line(), f"ETA shown after {done}")
            bar.done = progress.MIN_SAMPLES_FOR_ETA
            bar._per_item = [30.0] * progress.MIN_SAMPLES_FOR_ETA
            self.assertIn("left", bar._line())
        finally:
            bar.close()

    def test_absent_once_everything_is_done(self):
        bar = _bar(total=3)
        try:
            for _ in range(3):
                bar.advance()
            self.assertNotIn("left", bar._line())
        finally:
            bar.close()

    def test_projects_from_the_mean_item_so_far(self):
        bar = _bar(total=10)
        try:
            bar._per_item = [60.0, 60.0, 60.0]
            bar.done = 3
            # 7 remaining at 60s each is 7 minutes.
            self.assertIn("~7m left", bar._line())
        finally:
            bar.close()

    def test_durations_read_at_human_scale(self):
        for seconds, expected in [
            (0, "0s"), (45, "45s"), (90, "1m"), (3600, "1h00m"), (7530, "2h05m"),
        ]:
            with self.subTest(seconds=seconds):
                self.assertEqual(progress._fmt_duration(seconds), expected)


# ------------------------------------------------------------
# Encodings the console cannot handle
# ------------------------------------------------------------
class TestGlyphFallback(TestRegistryIsLeftClean):
    def test_a_cp1252_console_gets_ascii_and_the_line_survives_encoding(self):
        stream = FakeStream(encoding="cp1252")
        bar = progress.Bar(10, stream=stream, width=100)
        try:
            self.assertTrue(bar.ascii_only)
            bar.done = 5  # a bar with nothing filled has no fill glyph to check
            line = bar._line()
            self.assertNotIn(progress._FILL, line)
            self.assertNotIn(progress._SEP, line)
            self.assertIn(progress._ASCII_FILL, line)
            # The failure the fallback exists to prevent.
            line.encode("cp1252")
        finally:
            bar.close()

    def test_a_utf8_console_keeps_the_blocks(self):
        bar = _bar()
        try:
            self.assertFalse(bar.ascii_only)
            bar.done = 5
            self.assertIn(progress._FILL, bar._line())
        finally:
            bar.close()

    def test_a_stream_with_no_encoding_is_assumed_capable(self):
        stream = io.StringIO()  # no .encoding attribute
        bar = progress.Bar(10, stream=stream)
        try:
            self.assertFalse(bar.ascii_only)
        finally:
            bar.close()


# ------------------------------------------------------------
# Fitting the terminal
# ------------------------------------------------------------
class TestFitting(TestRegistryIsLeftClean):
    def test_a_narrow_terminal_still_gets_exactly_one_line(self):
        for width in (40, 30, 20):
            with self.subTest(width=width):
                bar = progress.Bar(154, stream=FakeStream(), width=width)
                try:
                    bar.done = 47
                    bar._per_item = [30.0] * 5
                    bar.set_current("860775 TRI")
                    line = bar._line()
                    self.assertNotIn("\n", line)
                    self.assertLessEqual(len(line), width)
                finally:
                    bar.close()

    def test_the_counts_are_what_a_narrow_terminal_keeps(self):
        bar = progress.Bar(154, stream=FakeStream(), width=30)
        try:
            bar.done = 47
            bar._per_item = [30.0] * 5
            bar.set_current("860775 TRI")
            line = bar._line()
            self.assertIn("47/154", line)
            # The label is the first thing dropped, the ETA the second.
            self.assertNotIn("860775", line)
        finally:
            bar.close()

    def test_a_wide_terminal_shows_everything(self):
        bar = progress.Bar(154, stream=FakeStream(), width=120)
        try:
            bar.done = 47
            bar._per_item = [30.0] * 5
            bar.set_current("860775 TRI")
            line = bar._line()
            for expected in ("47/154", "31%", "elapsed", "left", "860775 TRI"):
                self.assertIn(expected, line)
        finally:
            bar.close()

    def test_the_bar_keeps_its_length_when_a_label_comes_and_goes(self):
        """A bar that resizes every essay cannot be read as progress."""
        bar = progress.Bar(154, stream=FakeStream(), width=100)
        try:
            def cells(line):
                return line.index("]") - line.index("[") - 1

            bar.done = 47
            plain = cells(bar._line())
            bar.set_current("860775 TRI")
            self.assertEqual(cells(bar._line()), plain)
            bar.set_current("")
            self.assertEqual(cells(bar._line()), plain)
        finally:
            bar.close()

    def test_the_bar_keeps_its_length_when_the_eta_appears(self):
        """The ETA is absent for the first few items; the bar must not jump
        when it arrives. Checked at a narrow width, where it used to."""
        for width in (60, 80, 100):
            with self.subTest(width=width):
                bar = progress.Bar(154, stream=FakeStream(), width=width)
                try:
                    def cells(line):
                        return line.index("]") - line.index("[") - 1

                    bar.done = 1
                    before = cells(bar._line())
                    bar.done, bar._per_item = 47, [30.0] * 47
                    self.assertIn("left", bar._line())
                    self.assertEqual(cells(bar._line()), before)
                finally:
                    bar.close()

    def test_no_eta_is_shown_for_a_sub_second_remainder(self):
        bar = progress.Bar(10, stream=FakeStream(), width=100)
        try:
            bar._per_item = [0.01] * 5
            bar.done = 9
            self.assertNotIn("left", bar._line())
        finally:
            bar.close()

    def test_the_bar_fills_in_proportion(self):
        bar = progress.Bar(10, stream=FakeStream(), width=100)
        try:
            bar.done = 5
            line = bar._line()
            inside = line[line.index("[") + 1:line.index("]")]
            self.assertEqual(inside.count(progress._FILL), len(inside) // 2)
        finally:
            bar.close()


# ------------------------------------------------------------
# The module-level log(), used from deep in the call stack
# ------------------------------------------------------------
class TestModuleLog(TestRegistryIsLeftClean):
    def test_with_no_bar_active_it_is_plain_print(self):
        buffer = io.StringIO()
        stdout, sys.stdout = sys.stdout, buffer
        try:
            progress.log("     ! ConnectionError; retrying in 2s...")
        finally:
            sys.stdout = stdout
        self.assertEqual(buffer.getvalue(), "     ! ConnectionError; retrying in 2s...\n")

    def test_it_finds_the_active_bar(self):
        stream = FakeStream()
        with progress.Bar(10, stream=stream, width=100):
            progress.log("     ! retrying")
        self.assertIn("     ! retrying", stream.getvalue())

    def test_it_finds_the_innermost_of_nested_bars(self):
        outer, inner = FakeStream(), FakeStream()
        with progress.Bar(10, stream=outer, width=100):
            with progress.Bar(2, stream=inner, width=100):
                progress.log("inner message")
            progress.log("outer message")
        self.assertIn("inner message", inner.getvalue())
        self.assertNotIn("inner message", outer.getvalue())
        self.assertIn("outer message", outer.getvalue())


# ------------------------------------------------------------
# The contract with web/jobs.py
# ------------------------------------------------------------
class TestParseProgress(TestRegistryIsLeftClean):
    """The web app follows a run by parsing the non-TTY lines this module
    prints. These tests are what stop the two drifting apart: the web app has
    no regex of its own any more, and this is the round trip that proves the
    printed line and the parser still agree."""

    def _emitted(self, message: str, total: int = 154) -> str:
        """One real line, printed by a real non-TTY Bar."""
        stream = FakeStream(tty=False)
        with progress.Bar(total, stream=stream, width=100) as bar:
            bar.advance(message)
        return stream.getvalue().splitlines()[0]

    def test_a_completed_grade_round_trips(self):
        line = self._emitted("✓ 860775 TRI   Priority Interview                      31s")
        self.assertEqual(progress.parse_progress(line), (1, 154, "860775 TRI"))

    def test_a_skipped_submission_round_trips(self):
        line = self._emitted("! 860003 TRI   skipped — '860003_x.docx' could not be read")
        self.assertEqual(progress.parse_progress(line), (1, 154, "860003 TRI"))

    def test_it_tracks_a_whole_batch(self):
        stream = FakeStream(tty=False)
        with progress.Bar(3, stream=stream, width=100) as bar:
            for n in range(3):
                bar.advance(f"✓ 86000{n} TRI   Maybe   1s")
        parsed = [progress.parse_progress(l) for l in stream.getvalue().splitlines()]
        self.assertEqual([p[:2] for p in parsed], [(1, 3), (2, 3), (3, 3)])

    def test_everything_else_the_pipeline_prints_is_not_progress(self):
        for line in [
            "🚀 Pipeline starting...",
            "   ✓ Loaded 154 essay(s)",
            "   ✓ 2 pair(s) flagged for review",
            "      154  older rubric",
            "  -> Reviewing pair 850263 <-> 850327 (lexical 3%, semantic 75%)",
            "     ! ConnectionError (attempt 1/4); retrying in 2s...",
            "     ! Could not obtain valid JSON for grading response after 3 attempts.",
            "📤 Sending essays to Claude...",
            "",
        ]:
            with self.subTest(line=line):
                self.assertIsNone(progress.parse_progress(line))

    def test_a_drawn_bar_is_not_mistaken_for_progress(self):
        """A live bar's line also contains "47/154", but it never reaches a
        pipe. If one ever did, it must not be read as an item completing."""
        bar = progress.Bar(154, stream=FakeStream(), width=100)
        try:
            bar.done = 47
            self.assertIsNone(progress.parse_progress(bar._line()))
        finally:
            bar.close()


# ------------------------------------------------------------
# Cleanup
# ------------------------------------------------------------
class TestCleanup(TestRegistryIsLeftClean):
    def test_an_interrupt_mid_run_leaves_the_cursor_on_a_fresh_line(self):
        stream = FakeStream()
        with self.assertRaises(KeyboardInterrupt):
            with progress.Bar(10, stream=stream, width=100) as bar:
                bar.advance()
                raise KeyboardInterrupt
        # The shell prompt must not land on top of the bar.
        self.assertTrue(stream.getvalue().endswith("\n"))
        self.assertEqual(progress._stack, [])

    def test_close_is_idempotent(self):
        bar = _bar()
        bar.__enter__()
        bar.close()
        after_first = bar.stream.getvalue()
        bar.close()
        self.assertEqual(bar.stream.getvalue(), after_first)

    def test_an_empty_batch_draws_a_bar_without_dividing_by_zero(self):
        stream = FakeStream()
        with progress.Bar(0, stream=stream, width=100) as bar:
            self.assertIn("0/0", bar._line())


if __name__ == "__main__":
    unittest.main()
