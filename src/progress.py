"""A terminal progress bar for the long Claude loops.

A cold grading run sends 154 essays to Claude one at a time, tens of seconds
each. Without a bar the terminal sits silent through every call, and a run that
has hours left looks exactly like a run that has hung — so the only honest
question, "is this still working?", has no answer on screen.

The bar owns the last line of the terminal and redraws it in place. Everything
else — finished essays, retry warnings, JSON corrections — scrolls above it, so
the per-candidate record is still there in scrollback afterwards.

Two things shape the design more than the drawing does:

  - **A pipe is not a terminal.** Redirected to a file, `\\r` and `\\x1b[K`
    would be garbage in the log rather than a bar in it. When the stream is not
    a TTY the bar emits no control characters at all and prints one plain line
    per item, which is what this pipeline did before there was a bar.
  - **The warnings come from three frames down.** `_stream_with_retry` and
    `_json_with_corrective_retries` in essay_grader print from deep inside a
    single essay's call, and they cannot be handed a bar without threading one
    through signatures shared with the plagiarism checker. So the bar registers
    itself here and the module-level `log()` finds it. With no bar active
    `log()` is plain `print`, which is what makes those helpers safe to call
    outside a batch.

Presentation only. Nothing here decides anything the cache or the report reads.
"""

import re
import shutil
import sys
import time
from typing import List, Optional, TextIO, Tuple

# The bar glyphs, and their ASCII stand-ins. plagiarism_checker already records
# that Windows consoles running cp1252 choke on non-ASCII glyphs; a bar drawn
# in characters the console cannot encode would crash the run it is reporting
# on, which is a poor trade for a nicer block.
_FILL, _EMPTY, _SEP = "█", "░", "·"
# The separator is "|" rather than "-": "-" is already the empty bar cell, and
# the two next to each other read as one run of dashes.
_ASCII_FILL, _ASCII_EMPTY, _ASCII_SEP = "#", "-", "|"

# The bar shrinks to fit a narrow terminal, but below this it stops being a bar
# and becomes noise; the counts are what survive instead.
_MIN_CELLS = 6
_MAX_CELLS = 30

# An estimate off one sample is a guess wearing a number's clothes. Three is
# still rough, but it is an average of something.
MIN_SAMPLES_FOR_ETA = 3

# Bars register themselves so `log()` can find the innermost one. A stack
# rather than a single slot: grading and plagiarism never overlap today, but a
# bar that quietly unregisters someone else's is a bad way to find that out.
_stack: List["Bar"] = []


# ============================================================
# THE NON-TERMINAL LINE, AND ITS PARSER
# ============================================================
# When stdout is not a terminal there is no bar, and `advance()` prints one
# line per item carrying the counter that would have been in it:
#
#     "  47/154  ✓ 860775 TRI   Priority Interview                     31s"
#     "  48/154  ! 860003 TRI   skipped — '860003_x.docx' could not be read"
#
# The web app (web/jobs.py) runs the pipeline as a subprocess — a pipe, so
# never a terminal — and reads progress out of exactly these lines. It imports
# the parser below rather than writing its own regex, because a scraped format
# with two definitions in two files is a format that breaks silently: the
# printed line changes, nothing errors, and the browser's bar simply stops
# moving. One definition, and a test that round-trips it against real output,
# is what stops that happening twice.
PROGRESS_RE = re.compile(r"^\s*(\d+)/(\d+)\s+[✓!]\s+(\S+(?: \S+)?)")


def parse_progress(line: str) -> Optional[Tuple[int, int, str]]:
    """`(done, total, label)` from a non-terminal progress line, else None.

    Every other line the pipeline prints — step announcements, retry warnings,
    the plagiarism screen's pair lines — returns None, so a caller can feed it
    the whole stream.
    """
    match = PROGRESS_RE.match(line)
    if match is None:
        return None
    return int(match.group(1)), int(match.group(2)), match.group(3)


def active() -> Optional["Bar"]:
    """The innermost bar currently drawing, or None."""
    return _stack[-1] if _stack else None


def log(message: str = "") -> None:
    """Print a line without leaving a half-erased bar behind it.

    The whole point of the module-level registry: call sites far below the loop
    can report a retry without knowing a bar exists. With none active this is
    exactly `print`.
    """
    bar = active()
    if bar is None:
        print(message)
    else:
        bar.log(message)


def _fmt_duration(seconds: float) -> str:
    """Human-scale duration: `45s`, `14m`, `2h05m`.

    Deliberately coarse above a minute. A run measured in hours does not become
    more predictable for being told it has 1847 seconds left, and a
    to-the-second ETA invites a precision the estimate does not have.
    """
    total = int(round(max(seconds, 0)))
    if total < 60:
        return f"{total}s"
    minutes, _ = divmod(total, 60)
    if minutes < 60:
        return f"{minutes}m"
    hours, minutes = divmod(minutes, 60)
    return f"{hours}h{minutes:02d}m"


def _is_tty(stream: TextIO) -> bool:
    """True only for a real terminal. Anything unsure counts as not one."""
    try:
        return bool(stream.isatty())
    except (AttributeError, ValueError):
        # ValueError: stream already closed, which is not a terminal either.
        return False


def _encodable(stream: TextIO, text: str) -> bool:
    """Whether `text` survives this stream's encoding."""
    encoding = getattr(stream, "encoding", None)
    if not encoding:
        return True  # StringIO and friends hold str; nothing to encode.
    try:
        text.encode(encoding)
        return True
    except (LookupError, UnicodeEncodeError):
        return False


class Bar:
    """A single-line progress bar that other output can be printed around.

    Used as a context manager, so an interrupted run still leaves the cursor on
    a line of its own rather than on top of the bar::

        with Bar(len(items)) as bar:
            for item in items:
                bar.set_current(label(item))
                result = do_work(item)
                bar.advance(f"✓ {label(item)}  {result}")
    """

    def __init__(
        self,
        total: int,
        stream: Optional[TextIO] = None,
        width: Optional[int] = None,
    ) -> None:
        self.total = max(int(total), 0)
        self.stream = stream if stream is not None else sys.stdout
        # An explicit width is for tests; live bars re-read the terminal on
        # every redraw so a resize mid-run does not wrap the line.
        self.width = width
        self.done = 0
        self.current = ""
        self.live = _is_tty(self.stream)
        self.ascii_only = not _encodable(self.stream, _FILL + _EMPTY + _SEP)
        self._start = time.monotonic()
        self._item_start = self._start
        self._per_item: List[float] = []
        self._drawn = False
        self._closed = False
        _stack.append(self)

    # -- context manager ------------------------------------------------
    def __enter__(self) -> "Bar":
        if self.live:
            self._draw()
        return self

    def __exit__(self, exc_type, exc, tb) -> bool:
        self.close()
        return False  # never swallow; Ctrl-C must still reach the caller

    # -- public ---------------------------------------------------------
    def log(self, message: str = "") -> None:
        """Print a line above the bar, then redraw the bar beneath it."""
        if not self.live:
            print(message, file=self.stream)
            return
        self._erase()
        print(message, file=self.stream)
        self._draw()

    def set_current(self, label: str = "") -> None:
        """Name the item now in flight, and start timing it.

        The label is what turns a static bar into a sign of life during a
        40-second call: the counts have not moved, but the terminal is showing
        which candidate it is waiting on.
        """
        self.current = label
        self._item_start = time.monotonic()
        if self.live:
            self._draw()

    def advance(self, message: Optional[str] = None, count: int = 1) -> None:
        """Record one finished item, optionally logging a line about it.

        The message is given unindented; indenting is this method's business,
        because off a terminal the bar cannot be seen and the counter that
        would have been in it is prefixed to the line instead — a redirected
        run still reads as progress rather than as an undated list.

        That non-terminal line is a contract, not just formatting: see
        `PROGRESS_RE` above, and `parse_progress`, which the web app uses to
        follow a run. Change the shape of it and change them together.
        """
        now = time.monotonic()
        self._per_item.append(now - self._item_start)
        self._item_start = now
        self.done = min(self.done + count, self.total) if self.total else self.done + count
        self.current = ""

        if message is None:
            if self.live:
                self._draw()
            return
        if self.live:
            self.log(f"  {message}")
        else:
            self.log(f"  {self.done}/{self.total}  {message}")

    def close(self) -> None:
        """Stop drawing, leaving the finished bar in the scrollback.

        Idempotent, because `__exit__` and an explicit `close()` in the same
        function is an easy thing to end up with.
        """
        if self._closed:
            return
        self._closed = True
        if self in _stack:
            _stack.remove(self)
        if self.live and self._drawn:
            self._erase()
            self.current = ""
            print(self._line(), file=self.stream)

    # -- drawing --------------------------------------------------------
    def _erase(self) -> None:
        self.stream.write("\r\x1b[K")
        self.stream.flush()

    def _draw(self) -> None:
        self.stream.write("\r\x1b[K" + self._line())
        self.stream.flush()
        self._drawn = True

    def _eta(self) -> Optional[float]:
        """Seconds remaining, from the mean item so far, or None if too early."""
        remaining = self.total - self.done
        if remaining <= 0 or len(self._per_item) < MIN_SAMPLES_FOR_ETA:
            return None
        return (sum(self._per_item) / len(self._per_item)) * remaining

    def _line(self) -> str:
        """The bar as one line, fitted to the terminal.

        Fitting drops information from the least useful end: the in-flight
        label goes first, then the ETA, then the elapsed time. The counts and
        the percentage are what a narrow terminal keeps, because they are the
        two things the bar itself is only a picture of.
        """
        width = self.width or shutil.get_terminal_size((80, 24)).columns
        sep = _ASCII_SEP if self.ascii_only else _SEP
        ratio = (self.done / self.total) if self.total else 0.0

        parts = [f"{self.done}/{self.total}", f"{ratio * 100:3.0f}%"]
        parts.append(f"{_fmt_duration(time.monotonic() - self._start)} elapsed")
        eta = self._eta()
        # Below a second, "~0s left" is a countdown that says nothing.
        if eta is not None and eta >= 1:
            parts.append(f"~{_fmt_duration(eta)} left")

        # Size the bar against the longest this suffix could ever get, not
        # against what it happens to say now. The ETA is absent for the first
        # few items and both times grow as the run goes on, so measuring the
        # current text would resize the bar mid-run — and a bar that changes
        # length cannot be read as progress, which is its whole job.
        widest = f"  {self.total}/{self.total} {sep} 100% {sep} 00h00m elapsed {sep} ~00h00m left"
        cells = max(_MIN_CELLS, min(width - len(widest) - 4, _MAX_CELLS))

        # On a terminal too narrow even for that, drop parts from the least
        # useful end until the line fits: the ETA first, then the elapsed time.
        while len(parts) > 2 and cells + len("  " + f" {sep} ".join(parts)) + 4 > width:
            parts.pop()

        suffix = "  " + f" {sep} ".join(parts)
        fill = _ASCII_FILL if self.ascii_only else _FILL
        empty = _ASCII_EMPTY if self.ascii_only else _EMPTY
        filled = int(ratio * cells)
        line = f"  [{fill * filled}{empty * (cells - filled)}]{suffix}"

        # The label is appended only if it fits, and never costs the bar any
        # cells. Sizing the bar around it instead would make it grow and shrink
        # on every essay — a bar that changes length is hard to read as
        # progress, which is the one job it has.
        if self.current:
            labelled = f"{line} {sep} {self.current}"
            if len(labelled) <= width:
                line = labelled

        # Guaranteed single line: a wrapped bar redraws as a stuttering mess.
        return line[:width]
