"""Turn fetched bytes (CRS HTML renditions and PDFs, API summaries) into plain text."""

import re
import subprocess
import tempfile
from pathlib import Path

from lxml import html as lxml_html

SOFT_HYPHEN = "\u00ad"
_HTML_BLOCKS = {"p", "div", "br", "li", "tr", "table", "h1", "h2", "h3", "h4", "h5", "h6", "ul", "ol", "blockquote", "pre", "section", "article", "header", "footer", "dt", "dd", "caption", "figcaption"}
_HTML_TAG = re.compile(r"</?(p|br|div|li|ul|ol|b|i|em|strong|a|span|table|h[1-6])\b", re.I)
# What XML 1.0 does not allow: C0 controls other than tab, line feed and carriage return; surrogates; U+FFFE and U+FFFF. lxml refuses to set a text node that holds one.
_NOT_XML = re.compile(r"[\x00-\x08\x0b\x0c\x0e-\x1f\ud800-\udfff\ufffe\uffff]")


def decode(raw):
    try:
        return raw.decode("utf-8")
    except UnicodeDecodeError:
        return raw.decode("cp1252", errors="replace")


def tidy(text):
    text = text.replace("\r\n", "\n").replace("\r", "\n").replace(SOFT_HYPHEN, "").replace("\u00a0", " ")
    text = re.sub(r"[ \t]+\n", "\n", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip("\n").rstrip()


def xml_safe(text):
    """text with each character XML does not allow replaced by a space, so html_text can read it."""
    return _NOT_XML.sub(" ", text)


def html_text(source):
    """Readable text from arbitrary HTML: block elements become line breaks, list items get a dash."""
    if source is None:
        return None
    source = source if isinstance(source, str) else decode(source)
    if not source.strip():
        return None
    doc = lxml_html.fromstring(source)
    for bad in doc.xpath("//script|//style|//noscript"):
        bad.drop_tree()
    for element in doc.iter():
        if not isinstance(element.tag, str):
            continue
        tag = element.tag.lower()
        if tag in _HTML_BLOCKS:
            element.tail = "\n" + (element.tail or "")
            if tag == "li":
                element.text = "- " + (element.text or "")
            elif tag != "br":
                element.text = "\n" + (element.text or "")
        elif tag in ("td", "th"):
            element.tail = "\t" + (element.tail or "")
    lines = (re.sub(r"[ \t\u00a0]+", " ", line).strip() for line in doc.text_content().split("\n"))
    return tidy("\n".join(lines)) or None


def summary_text(value):
    """The API's summary is plain text for the products sampled; HTML is converted in case a product has it."""
    if not value or not value.strip():
        return None
    return html_text(value) if _HTML_TAG.search(value) else tidy(value) or None


def pdf_text(raw, timeout=180, pages=False):
    """Text layer of a PDF via poppler's pdftotext. None when the PDF has no text layer or pdftotext cannot read it; a missing pdftotext raises. pages=True keeps one form feed between pages, so page n is text.split("\\f")[n - 1]."""
    with tempfile.TemporaryDirectory(prefix="crs-pdf-") as tmp:
        path = Path(tmp) / "in.pdf"
        path.write_bytes(raw)
        try:
            done = subprocess.run(["pdftotext", "-enc", "UTF-8", "-q", str(path), "-"], capture_output=True, timeout=timeout)
        except subprocess.TimeoutExpired:
            return None
    if done.returncode != 0:
        return None
    out = done.stdout.decode("utf-8", errors="replace")
    if not pages:
        return tidy(out.replace("\f", "\n")) or None
    # pdftotext ends every page, the last one included, with a form feed.
    sheets = [tidy(sheet) for sheet in out.removesuffix("\f").split("\f")]
    return "\f".join(sheets) if any(sheets) else None
