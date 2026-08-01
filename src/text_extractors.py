"""Turns a submitted file into essay text, whatever container it arrived in.

Candidates are asked for PDFs but sometimes submit Word or Pages documents —
occasionally renamed to .pdf, so the extension cannot be trusted. Format is
detected from the file's contents.

We read those alternative formats rather than rejecting them: the candidate's
answers are perfectly gradeable, and it is only the container that is wrong.
The caller still flags the submission so the reviewer knows the interviewers
cannot open it and the candidate must resubmit as a PDF.

PDFs whose text layer is missing (graphic or scanned submissions) or damaged
(unmapped ligature glyphs) fall back to OCR — see ocr.py. The embedded text
is preferred wherever it is sound, because it is the candidate's exact words.
"""

import re
import struct
import zipfile
from html import unescape
from pathlib import Path
from typing import List, Tuple

import pdfplumber

import ocr


# ============================================================
# CONFIG
# ============================================================
# Heuristic for "this PDF is probably scanned/image-based".
# A PDF over this size that yields under this many characters is suspicious.
SCANNED_PDF_SIZE_THRESHOLD = 50_000  # bytes
SCANNED_PDF_TEXT_THRESHOLD = 100     # characters

# Control characters that are not legitimate whitespace. Their presence means
# the PDF's font never declared a ToUnicode mapping for some glyphs — usually
# ligatures, so 'practical' arrives as 'prac\x10cal'. The text renders fine in
# a viewer and is unreadable to us, which is why this is worth OCR.
_BROKEN_GLYPH = re.compile(r"[\x00-\x08\x0b\x0c\x0e-\x1f]")

# Above this share of damaged characters, the embedded text is not worth
# grading on its own and OCR is attempted. Set low deliberately: even 1% of
# glyphs missing scatters unreadable words through every paragraph.
BROKEN_GLYPH_RATIO = 0.001

# Word/Pages extraction is heuristic. Anything shorter than this is treated
# as a failed extraction rather than graded as if it were a real answer.
MIN_EXTRACTED_CHARS = 200

# Format identifiers returned by sniff_format().
FORMAT_PDF = "pdf"
FORMAT_DOCX = "docx"
FORMAT_PAGES = "pages"
FORMAT_UNKNOWN = "unknown"

# Human-readable names for the report.
FORMAT_NAMES = {
    FORMAT_PDF: "PDF",
    FORMAT_DOCX: "Word",
    FORMAT_PAGES: "Pages",
    FORMAT_UNKNOWN: "unrecognised",
}

# The Pages entry holding the document body. Other Index/*.iwa files hold
# stylesheets and metadata whose bytes decode into convincing-looking noise.
_DOCUMENT_IWA = re.compile(r"^Index/Document(-\d+)?\.iwa$")

# Lines of extracted Pages text that are document metadata, not answers.
_PAGES_NOISE = (
    re.compile(r"^\s*Application/"),
    re.compile(r"^\s*\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}"),
)


class UnreadableSubmission(Exception):
    """Raised when a file cannot be turned into essay text.

    Carries a human-readable reason for the report; callers convert this
    into a flagged row rather than letting it abort the run.
    """


# ============================================================
# PUBLIC API
# ============================================================
def sniff_format(path: Path) -> str:
    """Identifies a file's real format from its contents.

    The extension is deliberately ignored: candidates have submitted Pages
    documents named '.pdf', which is exactly the case this must catch.
    """
    try:
        with open(path, "rb") as fh:
            header = fh.read(8)
    except OSError as exc:
        raise UnreadableSubmission(f"'{path.name}' could not be read ({exc}).") from exc

    if header.startswith(b"%PDF"):
        return FORMAT_PDF

    # Word and Pages documents are both zip archives; the entries tell
    # them apart.
    if header.startswith(b"PK\x03\x04"):
        try:
            with zipfile.ZipFile(path) as archive:
                names = archive.namelist()
        except zipfile.BadZipFile:
            return FORMAT_UNKNOWN
        if any(n == "word/document.xml" for n in names):
            return FORMAT_DOCX
        if any(n.startswith("Index/") and n.endswith(".iwa") for n in names):
            return FORMAT_PAGES

    return FORMAT_UNKNOWN


def extract_text(path: Path) -> Tuple[str, str]:
    """Extracts essay text from a submission.

    Returns (text, format) where format is one of the FORMAT_* constants.

    Raises
    ------
    UnreadableSubmission : if the format is unsupported, or the file yields
                           too little text to be a real submission.
    """
    fmt = sniff_format(path)

    if fmt == FORMAT_PDF:
        return _extract_pdf(path), fmt
    if fmt == FORMAT_DOCX:
        return _guard(_extract_docx(path), path, fmt), fmt
    if fmt == FORMAT_PAGES:
        return _guard(_extract_pages(path), path, fmt), fmt

    raise UnreadableSubmission(
        f"'{path.name}' is not a PDF, Word or Pages document and its text "
        f"could not be extracted."
    )


# ============================================================
# PER-FORMAT EXTRACTION
# ============================================================
def broken_glyph_ratio(text: str) -> float:
    """Share of characters that are unmapped font glyphs rather than text."""
    if not text:
        return 0.0
    return len(_BROKEN_GLYPH.findall(text)) / len(text)


def _extract_pdf(pdf_path: Path) -> str:
    """Extracts all text from a PDF, falling back to OCR when needed.

    The embedded text layer is preferred: it is the candidate's exact words,
    with no recognition error. OCR is attempted only when that layer is
    missing (a graphic or scanned submission) or damaged (unmapped ligature
    glyphs), and the result is used only when it is genuinely cleaner — see
    _better_text().
    """
    try:
        with pdfplumber.open(pdf_path) as pdf:
            pages = [page.extract_text() or "" for page in pdf.pages]
    except Exception as exc:
        raise UnreadableSubmission(
            f"'{pdf_path.name}' claims to be a PDF but could not be opened ({exc})."
        ) from exc

    text = "\n\n".join(pages).strip()

    # A 50KB+ PDF yielding almost no text is a graphic or scanned submission.
    file_size = pdf_path.stat().st_size
    looks_scanned = (
        file_size > SCANNED_PDF_SIZE_THRESHOLD
        and len(text) < SCANNED_PDF_TEXT_THRESHOLD
    )

    if looks_scanned or broken_glyph_ratio(text) > BROKEN_GLYPH_RATIO:
        recognised = ocr.ocr_pdf(pdf_path)
        if recognised:
            text = _better_text(text, recognised)

    if not text:
        raise UnreadableSubmission(
            f"'{pdf_path.name}' contains no extractable text"
            + (
                "" if ocr.is_available()
                else " and OCR is unavailable on this machine"
            )
            + "."
        )

    if looks_scanned and len(text) < SCANNED_PDF_TEXT_THRESHOLD:
        raise UnreadableSubmission(
            f"'{pdf_path.name}' yielded only {len(text)} characters of text "
            f"despite being {file_size:,} bytes. It is probably scanned or "
            f"image-based, and OCR could not recover it either."
        )

    return text


def _better_text(embedded: str, recognised: str) -> str:
    """Chooses between the PDF's own text and the OCR transcription.

    Neither source is reliably better. The embedded layer is the candidate's
    literal words but may be riddled with unmapped glyphs; OCR is always
    mapped but invents its own misreadings on dense or graphical layouts.

    The rule: prefer OCR only when the embedded text is actually damaged and
    OCR recovered a comparable amount of text. The length guard matters —
    OCR that captures a third of the page has lost answers, and losing whole
    answers is far worse for grading than a scatter of broken words.
    """
    if not embedded:
        return recognised
    if broken_glyph_ratio(embedded) <= BROKEN_GLYPH_RATIO:
        return embedded  # nothing wrong with it — never trade it for OCR
    if len(recognised) < 0.6 * len(embedded):
        return embedded  # OCR dropped too much of the page
    return recognised


def _extract_docx(path: Path) -> str:
    """Extracts text from a .docx.

    A .docx is a zip holding word/document.xml. Rather than take a
    dependency, we map each <w:p> paragraph to a newline and concatenate the
    <w:t> text runs inside it — enough for prose answers, which is all we
    grade.
    """
    try:
        with zipfile.ZipFile(path) as archive:
            xml = archive.read("word/document.xml").decode("utf-8", "replace")
    except (zipfile.BadZipFile, KeyError) as exc:
        raise UnreadableSubmission(
            f"'{path.name}' looks like a Word document but could not be "
            f"read ({exc})."
        ) from exc

    # Paragraph and line breaks become newlines so answers stay separated.
    xml = re.sub(r"</w:p>", "\n", xml)
    xml = re.sub(r"<w:br[^>]*/>", "\n", xml)
    xml = re.sub(r"<w:tab[^>]*/>", "\t", xml)

    # Walk the document in order, emitting either the contents of a <w:t>
    # text run or one of the newlines introduced above.
    pieces: List[str] = []
    for match in re.finditer(r"<w:t[^>]*>(.*?)</w:t>|\n", xml, flags=re.DOTALL):
        pieces.append(match.group(1) if match.group(1) is not None else "\n")

    return _tidy(unescape("".join(pieces)))


def _extract_pages(path: Path) -> str:
    """Extracts text from an Apple Pages document.

    Pages stores its content in Index/*.iwa: Snappy-compressed protobuf with
    no public schema. Rather than parse the schema (undocumented and version
    specific), we decompress the blocks and recover the printable UTF-8 runs,
    which is where the paragraph text lives. Heuristic by nature, so the
    result is length-checked by the caller and the row is always flagged for
    human attention.
    """
    try:
        with zipfile.ZipFile(path) as archive:
            # Document.iwa only: DocumentStylesheet.iwa and friends hold
            # formatting data whose decompressed bytes look like text but
            # are not.
            targets = [n for n in archive.namelist() if _DOCUMENT_IWA.match(n)]
            blobs = [archive.read(n) for n in sorted(targets)]
    except (zipfile.BadZipFile, KeyError) as exc:
        raise UnreadableSubmission(
            f"'{path.name}' looks like a Pages document but could not be "
            f"read ({exc})."
        ) from exc

    decoded = b"".join(_decompress_iwa(blob) for blob in blobs)

    # Printable runs of 20+ characters: long enough to skip protobuf field
    # tags and short binary noise, short enough to keep real sentences.
    runs = re.findall(
        rb"(?:[\x20-\x7E\n\t]|[\xc2-\xf4][\x80-\xbf]+){20,}", decoded
    )
    lines = [run.decode("utf-8", "replace") for run in runs]

    keep = [
        line for line in "\n".join(lines).splitlines()
        if _looks_like_prose(line)
        and not any(pattern.match(line) for pattern in _PAGES_NOISE)
    ]
    return _tidy("\n".join(keep))


def _looks_like_prose(line: str) -> bool:
    """Filters out decompressed protobuf that merely resembles text.

    Real answers are long, mostly-ASCII and full of spaces; the binary
    residue that survives the printable-run regex is neither.
    """
    if len(line) < 20 or line.count(" ") < 2:
        return False
    ascii_chars = sum(1 for ch in line if ch.isascii())
    return ascii_chars / len(line) >= 0.9


# ============================================================
# INTERNAL HELPERS
# ============================================================
def _decompress_iwa(raw: bytes) -> bytes:
    """Decompresses an IWA file's chain of Snappy blocks.

    Each block is a 4-byte header (one marker byte plus a little-endian
    24-bit compressed length) followed by a raw Snappy stream. Blocks that
    fail to decode are skipped so one bad chunk doesn't lose the document.
    """
    out: List[bytes] = []
    offset = 0
    while offset + 4 <= len(raw):
        length = int.from_bytes(raw[offset + 1 : offset + 4], "little")
        block = raw[offset + 4 : offset + 4 + length]
        if not block:
            break
        try:
            out.append(_snappy_decompress(block))
        except (IndexError, struct.error, ValueError):
            pass  # skip the damaged block, keep the rest of the document
        offset += 4 + length
    return b"".join(out)


def _snappy_decompress(data: bytes) -> bytes:
    """Minimal Snappy raw-format decompressor.

    Implemented here rather than taking a python-snappy dependency (which
    needs a C extension): the format is small and this only ever runs on
    the handful of misformatted submissions in a campaign.
    """
    index = 0

    # Leading varint: the uncompressed length, which we don't need.
    while True:
        byte = data[index]
        index += 1
        if not byte & 0x80:
            break

    out = bytearray()
    while index < len(data):
        tag = data[index]
        element = tag & 0x03

        if element == 0:  # literal
            length = tag >> 2
            if length < 60:
                index += 1
            elif length == 60:
                length = data[index + 1]
                index += 2
            elif length == 61:
                length = struct.unpack("<H", data[index + 1 : index + 3])[0]
                index += 3
            elif length == 62:
                length = int.from_bytes(data[index + 1 : index + 4], "little")
                index += 4
            else:
                length = struct.unpack("<I", data[index + 1 : index + 5])[0]
                index += 5
            length += 1
            out += data[index : index + length]
            index += length
            continue

        # back-reference: copy `length` bytes from `offset` back in the output
        if element == 1:
            length = 4 + ((tag >> 2) & 0x07)
            offset = ((tag >> 5) << 8) | data[index + 1]
            index += 2
        elif element == 2:
            length = (tag >> 2) + 1
            offset = struct.unpack("<H", data[index + 1 : index + 3])[0]
            index += 3
        else:
            length = (tag >> 2) + 1
            offset = struct.unpack("<I", data[index + 1 : index + 5])[0]
            index += 5

        if offset <= 0 or offset > len(out):
            raise ValueError("bad back-reference offset")
        start = len(out) - offset
        for position in range(length):
            out.append(out[start + position])

    return bytes(out)


def _tidy(text: str) -> str:
    """Collapses the ragged whitespace that falls out of both extractors."""
    text = text.replace("\r\n", "\n").replace("\r", "\n")
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    lines = [line.strip() for line in text.split("\n")]
    return "\n".join(lines).strip()


def _guard(text: str, path: Path, fmt: str) -> str:
    """Rejects extractions too short to be a real submission."""
    if len(text) < MIN_EXTRACTED_CHARS:
        raise UnreadableSubmission(
            f"'{path.name}' is a {FORMAT_NAMES[fmt]} document but only "
            f"{len(text)} characters of text could be recovered from it."
        )
    return text
