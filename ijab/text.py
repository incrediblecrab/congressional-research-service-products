"""Turn fetched bytes (GovInfo HTML, bill and USLM XML, CRS HTML, PDF) into plain text and plain dicts."""

import re
import subprocess
import tempfile
from collections import Counter
from pathlib import Path

from lxml import etree
from lxml import html as lxml_html

SOFT_HYPHEN = "\u00ad"
CHAMBERS = {"house": "House", "house of representatives": "House", "h": "House", "senate": "Senate", "s": "Senate", "joint": "Joint", "j": "Joint"}


def chamber_of(value):
    """One spelling for chambers across sources: House, Senate or Joint."""
    value = (value or "").strip()
    return CHAMBERS.get(value.lower(), value.title() or None)


def decode(raw):
    """GovInfo serves mostly UTF-8, but older packages contain Windows-1252 bytes."""
    try:
        return raw.decode("utf-8")
    except UnicodeDecodeError:
        return raw.decode("cp1252", errors="replace")


def tidy(text):
    text = text.replace("\r\n", "\n").replace("\r", "\n").replace(SOFT_HYPHEN, "").replace("\u00a0", " ")
    text = re.sub(r"[ \t]+\n", "\n", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip("\n").rstrip()


def govinfo_html(raw):
    """GovInfo text renditions are <html><title>…</title><body><pre>…</pre>. Returns (title, text)."""
    doc = lxml_html.fromstring(decode(raw))
    title = (doc.findtext(".//title") or "").strip() or None
    pres = doc.findall(".//pre")
    text = "\n\n".join(pre.text_content() for pre in pres) if pres else html_text(doc)
    return title, tidy(text)


_HTML_BLOCKS = {"p", "div", "br", "li", "tr", "table", "h1", "h2", "h3", "h4", "h5", "h6", "ul", "ol", "blockquote", "pre", "section", "article", "header", "footer", "dt", "dd", "caption", "figcaption"}


def html_text(source):
    """Readable text from arbitrary HTML: block elements become line breaks, list items get a dash."""
    if source is None:
        return None
    if isinstance(source, (bytes, str)):
        source = source if isinstance(source, str) else decode(source)
        if not source.strip():
            return None
        doc = lxml_html.fromstring(source)
    else:
        doc = source
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


# Structural elements of the bill DTD and of USLM (public laws) that start a new line.
_XML_BLOCKS = {
    "section", "subsection", "paragraph", "subparagraph", "clause", "subclause", "item", "subitem", "subsubitem",
    "division", "subdivision", "title", "subtitle", "chapter", "subchapter", "part", "subpart", "level",
    "distribution-code", "congress", "session", "legis-num", "current-chamber", "action", "action-date", "action-desc",
    "legis-type", "official-title", "attestation", "attestation-group", "attestor", "endorsement", "legis-body",
    "resolution-body", "engrossed-amendment-body", "amendment-block", "quoted-block", "toc", "toc-entry", "preamble", "whereas",
    "resolved", "enacting-formula", "preface", "longTitle", "enactingFormula", "docNumber", "approvedDate", "main",
    "p", "row", "tr", "signatures", "signature", "note", "notes", "quotedContent", "ttitle", "table", "caption",
}
_XML_TEXT = {"text", "content", "chapeau", "continuation-text", "continuation", "proviso"}
_XML_ENUM = {"enum", "num"}
_XML_SKIP = {"metadata", "meta", "centerRunningHead", "sidenote", "page", "processedBy"}


def xml_text(raw):
    """Readable text from GPO bill XML or USLM XML, keeping one structural unit per line."""
    root = etree.fromstring(raw, parser=etree.XMLParser(recover=True, resolve_entities=False, no_network=True, huge_tree=True))
    out = []

    def newline():
        if out and not out[-1].endswith("\n"):
            out.append("\n")

    def emit(text):
        if text:
            out.append(re.sub(r"\s+", " ", text))

    def walk(element, previous):
        if not isinstance(element.tag, str):
            emit(element.tail)
            return
        name = etree.QName(element).localname
        if name in _XML_SKIP:
            emit(element.tail)
            return
        block = name in _XML_BLOCKS or (name in _XML_TEXT and previous not in _XML_ENUM) or (name == "header" and previous not in _XML_ENUM)
        if block:
            newline()
        emit(element.text)
        prior = None
        for child in element:
            walk(child, prior)
            if isinstance(child.tag, str):
                prior = etree.QName(child).localname
        if name in _XML_ENUM:
            out.append(" ")
        if block:
            newline()
        emit(element.tail)

    walk(root, None)
    lines = (re.sub(r" {2,}", " ", line).strip() for line in "".join(out).split("\n"))
    return tidy("\n".join(line for line in lines if line)) or None


_LIST_CHILDREN = {"item", "summary"}


def xml_dict(element):
    """Plain dict from XML. Repeated tags, and containers of <item>/<summary>, become lists; namespaces are dropped."""
    children = [child for child in element if isinstance(child.tag, str)]
    attributes = {etree.QName(key).localname: value for key, value in element.attrib.items()}
    if not children:
        text = (element.text or "").strip()
        if attributes:
            return dict(attributes, **({"value": text} if text else {}))
        return text or None
    names = [etree.QName(child).localname for child in children]
    if set(names) <= _LIST_CHILDREN and not attributes:
        return [xml_dict(child) for child in children]
    counts = Counter(names)
    result = dict(attributes)
    for child, name in zip(children, names):
        value = xml_dict(child)
        if counts[name] > 1:
            result.setdefault(name, []).append(value)
        else:
            result[name] = value
    return result


def parse_xml(raw):
    return etree.fromstring(raw, parser=etree.XMLParser(recover=True, resolve_entities=False, no_network=True, huge_tree=True))


def pdf_text(raw, timeout=180):
    """Text layer of a PDF via poppler's pdftotext. Returns None when the PDF has no extractable text."""
    with tempfile.TemporaryDirectory(prefix="ijab-pdf-") as tmp:
        path = Path(tmp) / "in.pdf"
        path.write_bytes(raw)
        done = subprocess.run(["pdftotext", "-enc", "UTF-8", "-q", str(path), "-"], capture_output=True, timeout=timeout)
    if done.returncode != 0:
        raise RuntimeError(f"pdftotext exit {done.returncode}")
    text = tidy(done.stdout.decode("utf-8", errors="replace").replace("\f", "\n"))
    return text or None
