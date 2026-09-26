"""The Constitution of the United States of America: Analysis and Interpretation (the Constitution Annotated), which the Congressional Research Service prepares, as GovInfo publishes it.

GovInfo's GPO collection holds the printed editions and supplements: 21 packages from GPO-CONAN-1992 to GPO-CONAN-2024-SUPP, measured September 25, 2026. GovInfo splits an edition into granules, each a part of the book such as "Article I - Legislative Branch"; a supplement is usually one PDF with no granules. Some editions nest granules one level deeper: GPO-CONAN-1992-9 ("The Constitution of the United States of America (With Annotations)") is the heading of GPO-CONAN-1992-9-1 to -9-8, and has no PDF of its own (its summary lists no pdfLink, and its PDF path answers 400; measured September 25, 2026). A row is one granule that is not such a heading, or one package that has no granules. The rows are synced by pipeline.sync like the products: list_all, fetch and exists below.

The live edition at constitution.congress.gov is not the source: it sits behind the same Cloudflare challenge as other www.congress.gov pages, which this pipeline does not try to get past.
"""

import hashlib
import json
import re

import pyarrow as pa

from .store import write_parquet
from .text import pdf_text, tidy

API = "https://api.govinfo.gov"
COLLECTION = "GPO"
PREFIX = "GPO-CONAN-"
# The collections service lists the packages modified since a date; no CONAN package predates this one.
SINCE = "1990-01-01T00:00:00Z"
PAGE = 1000
SOURCE_URL = f"{API}/collections/{COLLECTION}"
REPO_ID = "incrediblecrab/crs-constitution"
_PACKAGE = re.compile(r"GPO-CONAN-(?:REV-)?\d{4}(?:-SUPP)?")
_POSITION = re.compile(r"(?:-\d+)*")

SCHEMA = pa.schema([
    ("id", pa.string()),
    ("package_id", pa.string()),
    ("package_title", pa.string()),
    ("kind", pa.string()),
    ("title", pa.string()),
    ("sequence", pa.int32()),
    ("subsequence", pa.int32()),
    ("date_issued", pa.string()),
    ("pages", pa.int32()),
    ("text", pa.large_string()),
    ("url", pa.string()),
    ("pdf_url", pa.string()),
    ("pdf_sha256", pa.string()),
    ("updated_at", pa.string()),
    ("metadata", pa.large_string()),
    ("fetched_at", pa.string()),
])
COLUMNS = SCHEMA.names


def split_id(uid):
    """(package id, position numbers) of a unit id: GPO-CONAN-1992-9-1 gives (GPO-CONAN-1992, (9, 1)) and a package's own id gives (its id, ()). None for an id of another shape."""
    match = _PACKAGE.match(uid or "")
    if not match or not _POSITION.fullmatch(uid, match.end()):
        return None
    return match.group(0), tuple(int(number) for number in uid[match.end():].split("-")[1:])


def partition_of(uid):
    """One partition per package: GPO-CONAN-2022-8 and GPO-CONAN-1992-9-1 are in GPO-CONAN-2022 and GPO-CONAN-1992."""
    parts = split_id(uid)
    return parts[0] if parts else "other"


def headings(granule_ids):
    """The granules that head other granules: GPO-CONAN-1992-9 heads GPO-CONAN-1992-9-1."""
    ids = set(granule_ids)
    return {uid.rsplit("-", 1)[0] for uid in ids} & ids


def kind_of(package_title):
    """edition or supplement, from the package's title; every supplement's title says Supplement (checked on all 21 packages, September 25, 2026)."""
    return "supplement" if "supplement" in (package_title or "").lower() else "edition"


def normalize(row):
    out = {name: row.get(name) for name in COLUMNS}
    for name in ("sequence", "subsequence", "pages"):
        if out[name] is not None:
            out[name] = int(out[name])
    if isinstance(out["metadata"], (dict, list)):
        out["metadata"] = json.dumps(out["metadata"], ensure_ascii=False, sort_keys=True)
    return out


def row_weight(row):
    return len(row["text"] or "") + len(row["metadata"] or "")


def write(rows, path):
    return write_parquet(rows, path, schema=SCHEMA, prepare=normalize, weight=row_weight)


def comparable(row):
    """A row as pipeline.same_content compares it: without the stamps, and without lastModified in the summary record. GovInfo re-stamps whole collections without changing them: every CONAN package's lastModified is from March 7-8 or August 18, 2025, long after it was issued."""
    out = normalize(row)
    del out["updated_at"], out["fetched_at"]
    try:
        metadata = json.loads(out["metadata"]) if out["metadata"] else None
    except ValueError:
        return out
    if isinstance(metadata, dict):
        metadata.pop("lastModified", None)
    out["metadata"] = metadata
    return out


def mark(package):
    return f"{package.get('packageId')}@{package.get('lastModified')}" if package else None


class ConanSource:
    def __init__(self, fetcher):
        self.fetcher = fetcher
        # {package id: the collection listing's record}, from the last list_all(); fetch() takes package titles from it.
        self.packages = {}

    def list_packages(self):
        """Every CONAN package in the GPO collection, by id. The collection is listed in pages of 1,000 (4,347 packages in 5 pages, September 25, 2026)."""
        found, url, params = {}, f"{API}/collections/{COLLECTION}/{SINCE}", {"offsetMark": "*", "pageSize": PAGE}
        while url:
            data = self.fetcher.json(url, params=params) or {}
            for package in data.get("packages") or []:
                if (package.get("packageId") or "").startswith(PREFIX):
                    found[package["packageId"]] = package
            url, params = data.get("nextPage"), None
        return found

    def granules(self, package_id):
        found, url, params = [], f"{API}/packages/{package_id}/granules", {"offsetMark": "*", "pageSize": PAGE}
        while url:
            data = self.fetcher.json(url, params=params) or {}
            found += [granule for granule in data.get("granules") or [] if granule.get("granuleId")]
            url, params = data.get("nextPage"), None
        return found

    def list_all(self):
        """Every granule of every CONAN package except headings, and each package without granules, by id. A unit's updateDate is its package's lastModified, so the listing alone says what to fetch again."""
        self.packages = self.list_packages()
        items = {}
        for package_id, package in sorted(self.packages.items()):
            granules = self.granules(package_id)
            skip = headings(granule["granuleId"] for granule in granules)
            for granule in granules:
                if granule["granuleId"] not in skip:
                    items[granule["granuleId"]] = {"id": granule["granuleId"], "updateDate": package.get("lastModified")}
            if not granules:
                items[package_id] = {"id": package_id, "updateDate": package.get("lastModified")}
        newest = max(self.packages.values(), key=lambda p: (p.get("lastModified") or "", p["packageId"]), default=None)
        return {"count": len(items), "newest": mark(newest)}, items

    def summary(self, uid):
        """The granule's summary record, or the package's for a package without granules; None when GovInfo no longer has it. GovInfo answers 404 for a missing package and 400 "invalid granuleId" for a missing granule of an existing package (measured September 25, 2026)."""
        package_id = partition_of(uid)
        url = f"{API}/packages/{package_id}/summary" if uid == package_id else f"{API}/packages/{package_id}/granules/{uid}/summary"
        response = self.fetcher.get(url, headers={"Accept": "application/json"})
        if response.status_code == 404 or (response.status_code == 400 and b"invalid granuleId" in response.content):
            return None
        response.raise_for_status()
        return response.json()

    def exists(self, uid):
        return self.summary(uid) is not None

    def fetch(self, unit):
        record = self.summary(unit.id)
        if record is None:
            raise RuntimeError(f"GovInfo lists {unit.id} but has no summary for it")
        pdf_url = (record.get("download") or {}).get("pdfLink")
        if not pdf_url:
            raise RuntimeError(f"{unit.id} has no PDF link")
        response = self.fetcher.get(pdf_url)
        response.raise_for_status()
        raw = response.content
        if raw[:5] != b"%PDF-":
            raise RuntimeError(f"{unit.id}: the PDF link did not answer a PDF")
        text = pdf_text(raw, pages=True)
        package_id = partition_of(unit.id)
        package_title = (self.packages.get(package_id) or {}).get("title") or (record.get("title") if unit.id == package_id else None)
        position = (split_id(unit.id) or (None, ()))[1]
        return {
            "id": unit.id,
            "package_id": package_id,
            "package_title": tidy(package_title) if package_title else None,
            "kind": kind_of(package_title) if package_title else None,
            "title": tidy(record["title"]) if record.get("title") else None,
            "sequence": position[0] if position else None,
            "subsequence": position[1] if len(position) > 1 else None,
            "date_issued": record.get("dateIssued"),
            "pages": text.count("\f") + 1 if text else None,
            "text": text,
            "url": record.get("detailsLink"),
            "pdf_url": pdf_url,
            "pdf_sha256": hashlib.sha256(raw).hexdigest(),
            "metadata": json.dumps(record, ensure_ascii=False, sort_keys=True),
        }
