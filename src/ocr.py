"""OCR fallback for PDFs whose embedded text layer is missing or damaged.

Two failure modes show up in real submissions, and only the first is what
people usually mean by "scanned":

  1. **No text layer.** The candidate exported a graphic, a photo or a
     screenshot-based document. pdfplumber returns nothing to grade.
  2. **A damaged text layer.** The PDF has text, but its font never declared a
     ToUnicode mapping for its ligature glyphs, so 'practical' extracts as
     'prac\\x10cal'. The text looks fine in a viewer and is gibberish to us.

Both are fixed the same way: render the page and read the pixels.

Recognition uses the macOS Vision framework via pyobjc — no Homebrew or
system packages, no per-page API cost, and it handles the multi-column and
table layouts that instructor submissions tend to use. It is macOS-only by
nature; `is_available()` reports honestly and callers fall back to the
embedded text rather than failing, so this module is never load-bearing.
"""

import io
from pathlib import Path
from typing import List, Optional

# Rendering resolution. 300 dpi is the usual floor for reliable OCR of body
# text; higher mainly costs memory on the large multi-page submissions.
RENDER_DPI = 300


def _load_backend():
    """Imports the OCR stack, returning None when it isn't usable here.

    Import failure is an expected condition (non-macOS, or pyobjc absent), not
    an error — grading must still run using whatever text the PDF carries.
    """
    try:
        import pypdfium2
        import Quartz
        import Vision
        from Foundation import NSData
    except ImportError:
        return None
    return pypdfium2, Quartz, Vision, NSData


_BACKEND = _load_backend()


def is_available() -> bool:
    """True when OCR can actually run on this machine."""
    return _BACKEND is not None


def ocr_pdf(pdf_path: Path, dpi: int = RENDER_DPI) -> Optional[str]:
    """Renders every page of a PDF and returns the recognised text.

    Returns None when OCR is unavailable or fails outright, so the caller can
    fall back. A page that yields nothing contributes an empty string rather
    than aborting the document — a single unreadable diagram should not cost
    us the other three pages of answers.
    """
    if _BACKEND is None:
        return None
    pypdfium2, _, _, _ = _BACKEND

    try:
        document = pypdfium2.PdfDocument(str(pdf_path))
    except Exception:
        return None

    try:
        pages: List[str] = []
        for page in document:
            try:
                image = page.render(scale=dpi / 72).to_pil()
                pages.append(_recognise(image) or "")
            except Exception:
                pages.append("")
    finally:
        document.close()

    text = "\n\n".join(p for p in pages if p).strip()
    return text or None


def _recognise(pil_image) -> Optional[str]:
    """Runs Vision text recognition over one rendered page image."""
    if _BACKEND is None:
        return None
    _, Quartz, Vision, NSData = _BACKEND

    buffer = io.BytesIO()
    pil_image.save(buffer, format="PNG")
    raw = buffer.getvalue()

    data = NSData.dataWithBytes_length_(raw, len(raw))
    source = Quartz.CGImageSourceCreateWithData(data, None)
    if source is None:
        return None
    cg_image = Quartz.CGImageSourceCreateImageAtIndex(source, 0, None)
    if cg_image is None:
        return None

    request = Vision.VNRecognizeTextRequest.alloc().init()
    request.setRecognitionLevel_(Vision.VNRequestTextRecognitionLevelAccurate)
    # Language correction repairs the broken words that come out of the
    # ligature-damaged layouts this module exists to rescue.
    request.setUsesLanguageCorrection_(True)

    handler = Vision.VNImageRequestHandler.alloc().initWithCGImage_options_(
        cg_image, None
    )
    success, _ = handler.performRequests_error_([request], None)
    if not success:
        return None

    lines = []
    for observation in request.results() or []:
        candidates = observation.topCandidates_(1)
        if candidates:
            lines.append(candidates[0].string())
    return "\n".join(lines)
