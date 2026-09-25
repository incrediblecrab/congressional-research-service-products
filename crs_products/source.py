"""The Congress.gov API's CRS products: the listing, each product's detail, and its text.

Text comes from the PDF the API links to, else from the HTML rendition. PDF comes first because www.congress.gov puts CRS HTML behind a Cloudflare bot challenge some of the time (measured September 23, 2026: the HTML answered 403 with cf-mitigated: challenge while the PDF answered 200; a later request for the same HTML answered 200).

A record that lists only HTML gets its PDF tried first at the path the HTML implies (measured September 25, 2026: R43797's record listed only HTML, which answered the challenge, while R/PDF/R43797/R43797.8.pdf answered 200 with text).
"""

import hashlib
import json
import logging
import re
from urllib.parse import quote

import httpx

from .http import Blocked
from .text import html_text, pdf_text, summary_text, tidy

API = "https://api.congress.gov/v3"
PAGE = 250
NO_TEXT = {"text": None, "text_source": None, "text_url": None, "text_sha256": None}
log = logging.getLogger("crs_products")


def mark(item):
    """What the probe compares: the listing is sorted by updateDate, newest first, so a new or updated product changes its first item."""
    return f"{item.get('id')}@{item.get('updateDate')}" if item else None


def distinct(values):
    return list(dict.fromkeys(value for value in values if value))


def pdf_beside(html_url, version):
    """The PDF path an HTML rendition implies: .../R/HTML/R43797.html at version 8 gives .../R/PDF/R43797/R43797.8.pdf. The directory comes from the HTML, not the id (RL31801 is under RA, numeric ids under RS or RL)."""
    match = re.fullmatch(r"(.+/)HTML/([^/]+)\.html", html_url or "")
    if not match or not str(version or "").isdigit():
        return None
    directory, uid = match.groups()
    return f"{directory}PDF/{uid}/{uid}.{version}.pdf"


def product_row(report):
    url = report.get("url")
    return {
        "id": report.get("id"),
        "content_type": report.get("contentType"),
        "status": report.get("status"),
        "version": int(report["currentVersion"]) if str(report.get("currentVersion") or "").isdigit() else None,
        "title": tidy(report["title"]) if report.get("title") else None,
        "authors": distinct(a.get("author") for a in report.get("authors") or [] if isinstance(a, dict)),
        "topics": distinct(t.get("topic") for t in report.get("topics") or [] if isinstance(t, dict)),
        "publish_date": (report.get("publishDate") or "")[:10] or None,
        "url": f"https://{url}" if url and "://" not in url else url,
        "summary": summary_text(report.get("summary")),
        "metadata": json.dumps(report, ensure_ascii=False, sort_keys=True),
    }


class CrsSource:
    def __init__(self, fetcher):
        self.fetcher = fetcher
        # Set by the first challenged HTML request; the rest of the run skips HTML instead of asking again every 6 seconds.
        self.html_blocked = None

    def page(self, offset):
        data = self.fetcher.json(f"{API}/crsreport", params={"format": "json", "limit": PAGE, "offset": offset}) or {}
        return int((data.get("pagination") or {}).get("count") or 0), data.get("CRSReports") or []

    def head(self):
        """One request, the same one list_all() sends first: how many products the API lists, and the newest. Requests that differ only in limit have answered from different snapshots (tests/test_source.py has the measurement), so a probe asking with another limit can see a newer or older first item than the listing publishes."""
        count, items = self.page(0)
        return {"count": count, "newest": mark(items[0] if items else None)}

    def list_all(self):
        """Every listed product, by id. Pages can shift while they are read (an update moves a product to the top), so ids are deduplicated and a product missed this way is caught by the next listing."""
        count, items = self.page(0)
        head = {"count": count, "newest": mark(items[0] if items else None)}
        found, offset = {}, 0
        while items:
            for item in items:
                if item.get("id"):
                    found.setdefault(item["id"], item)
            offset += len(items)
            if offset >= count:
                break
            _, items = self.page(offset)
        return head, found

    def detail(self, uid):
        data = self.fetcher.json(f"{API}/crsreport/{quote(uid, safe='')}", params={"format": "json"})
        report = (data or {}).get("CRSReport")
        return report if isinstance(report, dict) else None

    def exists(self, uid):
        return self.detail(uid) is not None

    def fetch(self, unit):
        report = self.detail(unit.id)
        if report is None:
            raise RuntimeError(f"the API lists {unit.id} but has no detail for it")
        if report.get("id") != unit.id:
            raise RuntimeError(f"asked for {unit.id}, the API answered {report.get('id')}")
        return dict(product_row(report), **self.text(report))

    def text(self, report):
        formats = {}
        for entry in report.get("formats") or []:
            if isinstance(entry, dict) and entry.get("url"):
                formats.setdefault((entry.get("format") or "").upper(), entry["url"])
        if formats.get("PDF"):
            found = self._get(formats["PDF"], "pdf")
            if found:
                return found
        elif guessed := pdf_beside(formats.get("HTML"), report.get("currentVersion")):
            try:
                found = self._get(guessed, "pdf")
            except httpx.HTTPStatusError:
                # A guessed URL: a refusal means no PDF there, not a failed product.
                found = None
            if found:
                return found
        if formats.get("HTML") and not self.html_blocked:
            try:
                found = self._get(formats["HTML"], "html")
            except Blocked as error:
                self.html_blocked = str(error)
                log.warning("HTML renditions skipped for the rest of this run: %s", error)
                found = None
            if found:
                return found
        return dict(NO_TEXT)

    def _get(self, url, kind):
        """Text from one rendition, or None when it is missing or has no text. A challenge raises Blocked; other failures raise and fail the product so it is retried."""
        response = self.fetcher.get(url)
        if response.status_code in (404, 410):
            return None
        response.raise_for_status()
        raw = response.content
        if kind == "pdf":
            text = pdf_text(raw) if raw[:5] == b"%PDF-" else None
        else:
            text = html_text(raw)
        if not text:
            return None
        return {"text": text, "text_source": kind, "text_url": url, "text_sha256": hashlib.sha256(raw).hexdigest()}
