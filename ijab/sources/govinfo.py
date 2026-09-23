"""GovInfo sources: bulk data folders (keyless), API-listed packages (DATA_GOV_API_KEY), and Congressional Record granules."""

import hashlib
import re
import zipfile
from collections import Counter, defaultdict
from datetime import datetime

from .. import text as T
from ..pipeline import FATAL, Adapter, Partition, Unit, dumps

WWW = "https://www.govinfo.gov"
GOVINFO_API = "https://api.govinfo.gov"
CONGRESS_API = "https://api.congress.gov/v3"
EPOCH = "1990-01-01T00:00:00Z"
BULK_ZIP_THRESHOLD = 40
MIN_TEXT_CHARS = 1000

BILL_SLUGS = {"hr": "house-bill", "s": "senate-bill", "hres": "house-resolution", "sres": "senate-resolution",
              "hjres": "house-joint-resolution", "sjres": "senate-joint-resolution",
              "hconres": "house-concurrent-resolution", "sconres": "senate-concurrent-resolution"}


def sha256(raw):
    return hashlib.sha256(raw).hexdigest()


def details_url(package, granule=None):
    return f"{WWW}/app/details/{package}" + (f"/{granule}" if granule else "")


def bulk_time(value):
    """'16-Sep-2024 16:56' -> '2024-09-16T16:56'. GovInfo does not state the time zone; values are only compared for equality."""
    try:
        return datetime.strptime(value, "%d-%b-%Y %H:%M").strftime("%Y-%m-%dT%H:%M")
    except (TypeError, ValueError):
        return value


def first(value):
    return value[0] if isinstance(value, list) and value else value


def mods_fields(mods):
    """Title, date and the GPO extension block (congress, chamber, docClass, number, session, heldDate) from a MODS dict."""
    extensions = mods.get("extension")
    extension = {}
    for block in extensions if isinstance(extensions, list) else [extensions]:
        if isinstance(block, dict) and ("docClass" in block or "congress" in block):
            extension = block
            break
    titles = mods.get("titleInfo")
    title = next((t.get("title") for t in (titles if isinstance(titles, list) else [titles]) if isinstance(t, dict) and isinstance(t.get("title"), str)), None)
    origins = mods.get("originInfo")
    issued = first(next((o.get("dateIssued") for o in (origins if isinstance(origins, list) else [origins]) if isinstance(o, dict) and o.get("dateIssued")), None))
    issued = issued.get("value") if isinstance(issued, dict) else issued
    return title, issued if isinstance(issued, str) and issued else None, extension


class Bulk:
    """https://www.govinfo.gov/bulkdata/json/{path}: folders of per-document XML files plus one ZIP per folder."""

    def __init__(self, fetcher):
        self.fetcher = fetcher

    def listing(self, path):
        data = self.fetcher.json(f"{WWW}/bulkdata/json/{path}") or {}
        return data.get("files") or []

    def congresses(self, root):
        return sorted(int(entry["name"]) for entry in self.listing(root) if entry.get("folder") and str(entry.get("name", "")).isdigit())

    def files(self, path, depth=3):
        """All XML files below path, each tagged with the ZIP (if any) of the folder that holds it."""
        entries = self.listing(path)
        archive = next((e for e in entries if not e.get("folder") and e.get("fileExtension") == "zip"), None)
        for entry in entries:
            if entry.get("folder") and depth > 0:
                yield from self.files(f"{path}/{entry['name']}", depth - 1)
            elif entry.get("fileExtension") == "xml":
                yield {"name": entry["name"], "link": entry["link"], "size": entry.get("size"), "modified": bulk_time(entry.get("formattedLastModifiedTime")),
                       "zip": archive and {"link": archive["link"], "modified": bulk_time(archive.get("formattedLastModifiedTime"))}}


class BulkAdapter(Adapter):
    """One partition per Congress; one unit per XML file; fetched from the folder ZIP when many files changed."""

    root = None
    min_congress = 0

    def plan(self, manifest):
        bulk = Bulk(self.ctx.fetcher)
        for congress in sorted(bulk.congresses(self.root), reverse=True):
            if congress < self.min_congress or not self.wanted(congress):
                continue
            units = {}
            for entry in bulk.files(f"{self.root}/{congress}"):
                uid = entry["name"].rsplit(".", 1)[0]
                units[uid] = Unit(uid, entry["modified"], entry)
            yield Partition(str(congress), units)

    def fetch_many(self, units):
        by_zip = defaultdict(list)
        single = []
        for unit in units:
            archive = unit.info.get("zip")
            if archive and archive["modified"] and unit.info["modified"] and archive["modified"] >= unit.info["modified"]:
                by_zip[archive["link"]].append(unit)
            else:
                single.append(unit)
        for link, group in by_zip.items():
            if len(group) < BULK_ZIP_THRESHOLD:
                single.extend(group)
                continue
            yield from self._from_zip(link, group, single)
        for unit in sorted(single, key=lambda u: u.id):
            try:
                response = self.ctx.fetcher.get(unit.info["link"])
                response.raise_for_status()
                yield unit, self.rows(unit, response.content)
            except FATAL:
                raise
            except Exception as error:  # noqa: BLE001
                yield unit, error

    def _from_zip(self, link, group, fallback):
        path = self.ctx.store.scratch(link.rsplit("/", 1)[-1])
        try:
            response = self.ctx.fetcher.get(link, stream_to=path)
            if response.status_code != 200:
                fallback.extend(group)
                return
            self.ctx.store.measure()
            with zipfile.ZipFile(path) as archive:
                members = {name.rsplit("/", 1)[-1]: name for name in archive.namelist()}
                for unit in sorted(group, key=lambda u: u.id):
                    member = members.get(unit.info["name"])
                    if member is None:
                        fallback.append(unit)
                        continue
                    try:
                        yield unit, self.rows(unit, archive.read(member))
                    except Exception as error:  # noqa: BLE001
                        yield unit, error
        finally:
            path.unlink(missing_ok=True)

    def rows(self, unit, raw):
        raise NotImplementedError


def latest_summary(summaries):
    """Text of the most recent CRS summary. BILLSTATUS files hold the HTML in summary/text or in summary/cdata/text."""
    candidates = [(s.get("actionDate") or "", s.get("updateDate") or "", s.get("text") or (s.get("cdata") or {}).get("text"))
                  for s in summaries or [] if isinstance(s, dict)]
    best = max((c for c in candidates if isinstance(c[2], str) and c[2].strip()), default=None)
    return T.html_text(best[2]) if best else None


class BillStatus(BulkAdapter):
    root = "BILLSTATUS"

    def unit_of(self, row):
        congress, kind, number = row["id"].split("-")
        return f"BILLSTATUS-{congress}{kind}{number}"

    def live_counts(self, keys):
        """Bills per Congress from the Congress.gov API, a system independent of the GovInfo bulk files."""
        return {key: int(((self.ctx.fetcher.json(f"{CONGRESS_API}/bill/{key}", params={"format": "json", "limit": 1}) or {}).get("pagination") or {}).get("count") or 0) for key in keys}

    def rows(self, unit, raw):
        bill = T.xml_dict(T.parse_xml(raw)).get("bill") or {}
        kind = (bill.get("type") or "").lower()
        congress, number = int(bill["congress"]), str(bill["number"])
        summary = latest_summary(bill.get("summaries") if isinstance(bill.get("summaries"), list) else [])
        return [{
            "id": f"{congress}-{kind}-{number}", "congress": congress, "type": kind, "number": number,
            "chamber": T.chamber_of(bill.get("originChamber")), "title": bill.get("title"), "date": bill.get("introducedDate"),
            "url": f"https://www.congress.gov/bill/{congress}/{BILL_SLUGS.get(kind, kind)}/{number}",
            "text": summary, "text_source": "crs-summary" if summary else None,
            "text_url": unit.info["link"], "text_sha256": sha256(raw), "metadata": dumps(bill),
        }]


BILL_ID = re.compile(r"^BILLS-(\d+)([a-z]+?)(\d+)([a-z]+)$")
LAW_ID = re.compile(r"^PLAW-(\d+)(publ|pvtl)(\d+)$")


def dublin_core(root):
    out = {}
    for element in root.iter():
        if isinstance(element.tag, str) and element.tag.startswith("{http://purl.org/dc/elements/1.1/}"):
            out.setdefault(element.tag.split("}", 1)[1], (element.text or "").strip())
    return out


def bill_text_row(unit_id, raw, link, source):
    match = BILL_ID.match(unit_id)
    congress, kind, number, version = (int(match.group(1)), match.group(2), match.group(3), match.group(4)) if match else (None, None, None, None)
    root = T.parse_xml(raw)
    dc = dublin_core(root)
    return {
        "id": unit_id, "congress": congress, "type": kind, "number": number, "version": version,
        "chamber": "House" if (kind or "").startswith("h") else "Senate" if kind else None,
        "title": dc.get("title") or None, "date": dc.get("date") or None, "url": details_url(unit_id),
        "text": T.xml_text(raw), "text_source": source, "text_url": link, "text_sha256": sha256(raw),
        "metadata": dumps({"dublin_core": dc, "attributes": dict(root.attrib)}),
    }


class BillText(BulkAdapter):
    root = "BILLS"

    def live_counts(self, keys):
        return govinfo_counts(self.ctx.fetcher, "BILLS", keys)

    def rows(self, unit, raw):
        return [bill_text_row(unit.id, raw, unit.info["link"], "bill-xml")]


class Laws(BulkAdapter):
    root = "PLAW"

    def live_counts(self, keys):
        return govinfo_counts(self.ctx.fetcher, "PLAW", keys)

    def rows(self, unit, raw):
        match = LAW_ID.match(unit.id)
        root = T.parse_xml(raw)
        meta = next((el for el in root if isinstance(el.tag, str) and el.tag.endswith("meta")), None)
        meta_dict = T.xml_dict(meta) if meta is not None else {}
        dc = dublin_core(root)
        return [{
            "id": unit.id, "congress": int(match.group(1)) if match else None, "type": match.group(2) if match else None,
            "number": match.group(3) if match else None, "title": dc.get("title") or None, "date": dc.get("date") or None,
            "url": details_url(unit.id), "text": T.xml_text(raw), "text_source": "uslm-xml", "text_url": unit.info["link"],
            "text_sha256": sha256(raw), "metadata": dumps(meta_dict),
        }]


class GovInfoApi:
    """https://api.govinfo.gov/collections/{code}/{since}: every package in a collection with its lastModified.

    Measured on 09/23/2026 this lists more packages than the GovInfo sitemaps (CHRG 45,896 vs 45,537; CREC 6,023 vs 6,018). A collection code also covers packages of other series (SERIALSET, GOVPUB, GPO, ERP, HMAN, SMAN), which callers filter out.
    """

    def __init__(self, fetcher):
        self.fetcher = fetcher

    def packages(self, code, congress=None):
        url, params = f"{GOVINFO_API}/collections/{code}/{EPOCH}", {"offsetMark": "*", "pageSize": 1000}
        if congress is not None:
            params["congress"] = congress
        while url:
            data = self.fetcher.json(url, params=params) or {}
            items = data.get("packages") or []
            yield from items
            url, params = (data.get("nextPage") if items else None), None

    def count(self, code, congress=None):
        params = {"offsetMark": "*", "pageSize": 1, **({"congress": congress} if congress is not None else {})}
        return int((self.fetcher.json(f"{GOVINFO_API}/collections/{code}/{EPOCH}", params=params) or {}).get("count") or 0)


CONGRESS_OF = re.compile(r"^[A-Z]+-(\d+)")


def package_text(fetcher, package):
    """(text, text_source, text_url, sha256 of fetched bytes, html title) for a single-document package.

    The HTML rendition is preferred; the PDF text layer is used when the HTML is missing or shorter than MIN_TEXT_CHARS.
    """
    html_link = f"{WWW}/content/pkg/{package}/html/{package}.htm"
    response = fetcher.get(html_link)
    best = (None, None, None, None, None)
    if response.status_code == 200:
        title, text = T.govinfo_html(response.content)
        best = (text or None, "govinfo-html" if text else None, html_link, sha256(response.content), title)
        if text and len(text) >= MIN_TEXT_CHARS:
            return best
    elif response.status_code != 404:
        response.raise_for_status()
    pdf_link = f"{WWW}/content/pkg/{package}/pdf/{package}.pdf"
    response = fetcher.get(pdf_link)
    if response.status_code == 200 and response.content[:5] == b"%PDF-":
        text = T.pdf_text(response.content)
        if text and len(text) > len(best[0] or ""):
            return text, "govinfo-pdf", pdf_link, sha256(response.content), best[4]
    elif response.status_code != 404:
        response.raise_for_status()
    return best


class PackageAdapter(Adapter):
    """API-listed packages holding one document each: text from package_text(), metadata from MODS.

    Only packages whose id starts with the collection code are congress.gov documents; the rest are other GovInfo series. congresses=None lists the whole collection in one pass; a range lists Congress by Congress (used for legacy slices).
    """

    code = None
    congresses = None

    def partition_key(self, package):
        match = CONGRESS_OF.match(package)
        return match.group(1) if match else None

    def listing(self):
        api = GovInfoApi(self.ctx.fetcher)
        for congress in self.congresses or [None]:
            if congress is not None and not self.wanted(congress):
                continue
            for package in api.packages(self.code, congress):
                pid = package.get("packageId") or ""
                if pid.startswith(f"{self.code}-") and self.partition_key(pid) is not None:
                    yield pid, package

    def plan(self, manifest):
        groups = defaultdict(dict)
        for pid, package in self.listing():
            groups[self.partition_key(pid)][pid] = Unit(pid, package.get("lastModified"))
        for key in sorted(groups, key=int, reverse=True):
            yield Partition(key, groups[key])

    def live_counts(self, keys):
        return dict(Counter(self.partition_key(pid) for pid, _ in self.listing()))

    def fetch(self, unit):
        package = unit.id
        mods_response = self.ctx.fetcher.get(f"{WWW}/metadata/pkg/{package}/mods.xml")
        if mods_response.status_code not in (200, 404):
            mods_response.raise_for_status()
        mods = T.xml_dict(T.parse_xml(mods_response.content)) if mods_response.status_code == 200 else {}
        title, issued, extension = mods_fields(mods) if mods else (None, None, {})
        text, source, link, digest, html_title = package_text(self.ctx.fetcher, package)
        held = first(extension.get("heldDate"))
        return [{
            "id": package, "congress": as_int(extension.get("congress")) or as_int(self.partition_key(package)),
            "type": (extension.get("docClass") or "").lower() or None, "number": extension.get("number"),
            "chamber": T.chamber_of(extension.get("chamber")), "title": title or html_title,
            "date": held if isinstance(held, str) else issued, "url": details_url(package),
            "text": text, "text_source": source, "text_url": link, "text_sha256": digest, "metadata": dumps(mods) if mods else None,
        }]


def as_int(value):
    return int(value) if str(value if value is not None else "").isdigit() else None


class LegacyBillText(PackageAdapter):
    """Bill versions of the 103rd-112th Congresses, which have no bulk XML: GovInfo HTML text plus MODS."""

    code = "BILLS"
    congresses = range(112, 102, -1)

    def live_counts(self, keys):
        return govinfo_counts(self.ctx.fetcher, self.code, keys)

    def fetch(self, unit):
        link = f"{WWW}/content/pkg/{unit.id}/html/{unit.id}.htm"
        response = self.ctx.fetcher.get(link)
        response.raise_for_status()
        html_title, text = T.govinfo_html(response.content)
        mods_response = self.ctx.fetcher.get(f"{WWW}/metadata/pkg/{unit.id}/mods.xml")
        mods = T.xml_dict(T.parse_xml(mods_response.content)) if mods_response.status_code == 200 else {}
        title, issued, _ = mods_fields(mods) if mods else (None, None, {})
        match = BILL_ID.match(unit.id)
        congress, kind, number, version = (int(match.group(1)), match.group(2), match.group(3), match.group(4)) if match else (None, None, None, None)
        if not text:
            raise RuntimeError(f"empty HTML text for {unit.id}")
        return [{
            "id": unit.id, "congress": congress, "type": kind, "number": number, "version": version,
            "chamber": "House" if (kind or "").startswith("h") else "Senate" if kind else None,
            "title": title or html_title, "date": issued, "url": details_url(unit.id),
            "text": text, "text_source": "govinfo-html", "text_url": link, "text_sha256": sha256(response.content),
            "metadata": dumps(mods) if mods else None,
        }]


class LegacyLaws(PackageAdapter):
    """Public and private laws of the 104th-112th Congresses, which have no USLM bulk XML."""

    code = "PLAW"
    congresses = range(112, 103, -1)

    def live_counts(self, keys):
        return govinfo_counts(self.ctx.fetcher, self.code, keys)

    def fetch(self, unit):
        rows = super().fetch(unit)
        match = LAW_ID.match(unit.id)
        if match:
            rows[0].update({"congress": int(match.group(1)), "type": match.group(2), "number": match.group(3)})
        return rows


def govinfo_counts(fetcher, code, keys):
    api = GovInfoApi(fetcher)
    return {key: api.count(code, int(key)) for key in keys}


class Composite(Adapter):
    """Bulk XML for recent Congresses and API-listed packages for older ones, as one collection. Partition keys must not overlap."""

    parts = ()

    def __init__(self, ctx, collection):
        super().__init__(ctx, collection)
        self.adapters = [cls(ctx, collection) for cls in self.parts]
        self.owner = {}

    def plan(self, manifest):
        for adapter in self.adapters:
            for partition in adapter.plan(manifest):
                self.owner[partition.key] = adapter
                yield partition

    def fetch_many(self, units):
        by_adapter = defaultdict(list)
        for unit in units:
            by_adapter[self._adapter_for(unit.id)].append(unit)
        for adapter, group in by_adapter.items():
            yield from adapter.fetch_many(group)

    def _adapter_for(self, unit_id):
        match = CONGRESS_OF.match(unit_id)
        return self.owner.get(match.group(1) if match else None, self.adapters[0])

    def live_counts(self, keys):
        return self.adapters[0].live_counts(keys)


class BillTextAll(Composite):
    parts = (BillText, LegacyBillText)


class LawsAll(Composite):
    parts = (Laws, LegacyLaws)


class CommitteeReports(PackageAdapter):
    code = "CRPT"


class Hearings(PackageAdapter):
    code = "CHRG"


class CommitteePrints(PackageAdapter):
    code = "CPRT"


class CongressionalDocuments(PackageAdapter):
    code = "CDOC"


class CongressionalRecord(PackageAdapter):
    """Daily Congressional Record: one partition per year, one unit per issue, one row per granule (article)."""

    code = "CREC"

    def partition_key(self, package):
        match = re.match(r"^CREC-(\d{4})-", package)
        return match.group(1) if match else None

    def unit_of(self, row):
        # Two issues can share a date (CREC-2025-01-03-v170 and -v171), so the package comes from the row's details URL.
        return row["url"].split("/app/details/", 1)[1].split("/", 1)[0]

    def fetch(self, unit):
        package = unit.id
        response = self.ctx.fetcher.get(f"{WWW}/metadata/pkg/{package}/mods.xml")
        response.raise_for_status()
        mods = T.xml_dict(T.parse_xml(response.content))
        _, issued, extension = mods_fields(mods)
        congress = extension.get("congress")
        issue = {key: extension.get(key) for key in ("volume", "issue", "session", "congress", "pages") if extension.get(key) is not None}
        related = mods.get("relatedItem")
        rows = []
        for item in related if isinstance(related, list) else [related]:
            if not isinstance(item, dict) or item.get("type") != "constituent":
                continue
            granule = (item.get("ID") or "").removeprefix("id-")
            title, _, granule_ext = mods_fields(item)
            granule_ext = granule_ext or next((e for e in (item.get("extension") if isinstance(item.get("extension"), list) else [item.get("extension")]) if isinstance(e, dict)), {})
            granule = granule_ext.get("accessId") or granule
            link = f"{WWW}/content/pkg/{package}/html/{granule}.htm"
            html_response = self.ctx.fetcher.get(link)
            text = digest = None
            if html_response.status_code == 200:
                _, text = T.govinfo_html(html_response.content)
                digest = sha256(html_response.content)
            elif html_response.status_code != 404:
                html_response.raise_for_status()
            extent = ((item.get("part") or {}).get("extent") or {}) if isinstance(item.get("part"), dict) else {}
            pages = "-".join(p for p in (extent.get("start"), extent.get("end")) if p) or None
            granule_class = (granule_ext.get("granuleClass") or "").lower() or None
            rows.append({
                "id": granule, "congress": int(congress) if str(congress or "").isdigit() else None, "type": granule_class,
                "number": pages, "chamber": T.chamber_of(granule_ext.get("chamber")) or {"senate": "Senate", "house": "House"}.get(granule_class),
                "title": title, "date": issued, "url": details_url(package, granule), "text": text,
                "text_source": "govinfo-html" if text else None, "text_url": link, "text_sha256": digest,
                "metadata": dumps({"granule": item, "issue": issue}),
            })
        if not rows:
            raise RuntimeError(f"{package}: MODS lists no granules")
        return rows
