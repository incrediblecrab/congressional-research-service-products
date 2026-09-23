"""Source adapters on real trimmed samples (tests/fixtures): ids, parsing and the unit-to-row invariant the sync loop relies on."""

import hashlib
import json
import math
import shutil

import pytest
from conftest import FakeFetcher, fixture_bytes, fixture_json

from ijab import text as T
from ijab.collections import BY_NAME, COLLECTIONS
from ijab.http import Blocked
from ijab.pipeline import Context, Unit
from ijab.sources import congress as C
from ijab.sources import govinfo as G

WWW = "https://www.govinfo.gov"


def adapter(cls, fetcher=None, name=None):
    collection = BY_NAME[name] if name else next(c for c in COLLECTIONS if c.adapter is cls)
    return cls(Context(fetcher=fetcher or FakeFetcher(), store=None, deadline=math.inf), collection)


def test_first_list_shapes():
    assert C.first_list({"pagination": {"count": 1}, "request": {}, "actions": [1]}) == [1]
    assert C.first_list({"request": {}, "summaries": {"item": [2]}}) == [2]
    assert C.first_list({}) == [] and C.first_list(None) == []


def test_ids_from_list_items():
    assert C.crs_prefix({"id": "RL33456"}) == "RL" and C.crs_prefix({"id": ""}) == "other"
    assert C.congress_number({"url": "https://api.congress.gov/v3/congress/119?format=json"}) == "119"
    assert C.congress_number({"name": "93rd Congress"}) == "93"
    assert C.bill_id({"congress": 107, "type": "HCONRES", "number": "1"}) == "107-hconres-1"


def test_chamber_spellings():
    assert [T.chamber_of(v) for v in ("House of Representatives", "senate", "Joint", "H", "", None)] == ["House", "Senate", "Joint", "House", None, None]


API_CASES = [("amendment", C.Amendments), ("committee-meeting", C.CommitteeMeetings), ("committee", C.Committees),
             ("congress", C.Congresses), ("house-communication", C.HouseCommunications), ("house-requirement", C.HouseRequirements),
             ("house-vote", C.HouseVotes), ("member", C.Members), ("nomination", C.Nominations),
             ("senate-communication", C.SenateCommunications), ("treaty", C.Treaties)]


@pytest.mark.parametrize("name,cls", API_CASES)
def test_row_id_equals_listed_unit_id(name, cls):
    """If a row's id differed from its list item's unit id, every run would refetch the unit and verify would fail."""
    item = fixture_json(f"list-{name}.json")[cls.spec.item_key][0]
    fetcher = FakeFetcher(json_map={C.strip_query(item["url"]): fixture_json(f"detail-{name}.json")})
    uid = cls.spec.unit_id(item)
    rows = adapter(cls, fetcher).fetch(Unit(uid, item.get("updateDate"), {"item": item}))
    assert [row["id"] for row in rows] == [uid]
    assert rows[0]["chamber"] in (None, "House", "Senate", "Joint")
    assert rows[0]["title"]
    assert json.loads(rows[0]["metadata"])


def test_treaty_text_comes_from_the_treaty_document():
    item = fixture_json("list-treaty.json")["treaties"][0]
    html_link = f"{WWW}/content/pkg/CDOC-119tdoc2/html/CDOC-119tdoc2.htm"
    raw = fixture_bytes("CDOC-119tdoc2.htm")
    fetcher = FakeFetcher(json_map={C.strip_query(item["url"]): fixture_json("detail-treaty.json")}, get_map={html_link: raw})
    [row] = adapter(C.Treaties, fetcher).fetch(Unit(C.treaty_id(item), item["updateDate"], {"item": item}))
    assert row["id"] == "119-2"
    assert (row["text_source"], row["text_url"], row["text_sha256"]) == ("govinfo-html", html_link, hashlib.sha256(raw).hexdigest())
    assert len(row["text"]) >= G.MIN_TEXT_CHARS


def test_nomination_title_falls_back_to_nominee_groups():
    detail = fixture_json("detail-nomination.json")["nomination"]
    assert C.nomination_title(detail, {}).startswith("Angela Veronica Colmenero")
    groups = {"nominees": [{"organization": "Army", "positionTitle": "Colonel", "nomineeCount": 12}, {"organization": "Navy"}]}
    assert C.nomination_title(groups, {}) == "Army, Colonel, 12 nominee(s); Navy"
    assert C.nomination_title({}, {}) is None


def test_legacy_bill_row_id_matches_unit_id():
    bill = {"congress": 107, "type": "HCONRES", "number": "1", "originChamber": "House", "title": "T", "introducedDate": "2001-01-03",
            "summaries": [{"actionDate": "2001-01-03", "text": "<p>Provides for an adjournment.</p>"}]}
    row = C.legacy_bill_row(bill, {})
    assert row["id"] == C.LegacyBills.spec.unit_id(bill) == "107-hconres-1"
    assert (row["text"], row["url"]) == ("Provides for an adjournment.", "https://www.congress.gov/bill/107/house-concurrent-resolution/1")


def test_latest_summary_reads_both_layouts():
    summaries = [{"actionDate": "2023-01-05", "text": "<p>Old <b>summary</b></p>"},
                 {"actionDate": "2023-03-01", "cdata": {"text": "<p>New summary</p>"}},
                 {"actionDate": "2023-04-01", "text": "   "}]
    assert G.latest_summary(summaries) == "New summary"
    assert G.latest_summary(summaries[:1]) == "Old summary"
    assert G.latest_summary(None) is None


def test_mods_fields_accepts_lists():
    mods = {"titleInfo": [{"title": "Main"}, {"title": "Alt"}], "originInfo": [{"publisher": "GPO"}, {"dateIssued": ["2001-01-02", "x"]}],
            "extension": [{"other": "1"}, {"congress": "107", "docClass": "HR"}]}
    assert G.mods_fields(mods) == ("Main", "2001-01-02", {"congress": "107", "docClass": "HR"})
    assert G.mods_fields({"originInfo": {"dateIssued": {"encoding": "w3cdtf", "value": "1999-05-06"}}})[1] == "1999-05-06"


def test_bill_status_row():
    link = f"{WWW}/bulkdata/BILLSTATUS/118/sconres/BILLSTATUS-118sconres1.xml"
    raw = fixture_bytes("BILLSTATUS-118sconres1.xml")
    bills = adapter(G.BillStatus)
    [row] = bills.rows(Unit("BILLSTATUS-118sconres1", "2024-01-01T00:00", {"link": link}), raw)
    assert row["id"] == "118-sconres-1" and bills.unit_of(row) == "BILLSTATUS-118sconres1"
    assert (row["congress"], row["type"], row["number"], row["chamber"]) == (118, "sconres", "1", "Senate")
    assert row["url"] == "https://www.congress.gov/bill/118/senate-concurrent-resolution/1"
    assert row["text_source"] == "crs-summary" and row["text"].startswith("Adopting Cryptocurrency in Congress")
    assert row["text_sha256"] == hashlib.sha256(raw).hexdigest() and json.loads(row["metadata"])["number"] == "1"


def test_bill_text_row():
    row = G.bill_text_row("BILLS-118sconres1is", fixture_bytes("BILLS-118sconres1is.xml"), "L", "bill-xml")
    assert (row["congress"], row["type"], row["number"], row["version"], row["chamber"]) == (118, "sconres", "1", "is", "Senate")
    assert row["url"] == f"{WWW}/app/details/BILLS-118sconres1is"
    assert "CONCURRENT RESOLUTION" in row["text"].upper() and row["title"]


def test_law_row():
    [row] = adapter(G.Laws, name="laws").rows(Unit("PLAW-118publ1", "t", {"link": "L"}), fixture_bytes("PLAW-118publ1.xml"))
    assert (row["congress"], row["type"], row["number"], row["text_source"]) == (118, "publ", "1", "uslm-xml")
    assert row["text"] and row["title"]


def test_congressional_record_issue_gives_one_row_per_granule():
    package = "CREC-2024-01-02"
    first = f"{WWW}/content/pkg/{package}/html/{package}-pt1-PgD1333.htm"
    fetcher = FakeFetcher(get_map={f"{WWW}/metadata/pkg/{package}/mods.xml": fixture_bytes("CREC-2024-01-02-mods.xml"),
                                   first: b"<html><body><pre>Daily Digest text</pre></body></html>"})
    record = adapter(G.CongressionalRecord, fetcher)
    rows = record.fetch(Unit(package, "t"))
    assert [row["id"] for row in rows] == [f"{package}-pt1-PgD1333", f"{package}-pt1-PgD1333-2"]
    assert {record.unit_of(row) for row in rows} == {package}
    assert all(row["congress"] == 118 and row["date"] == "2024-01-02" for row in rows)
    assert (rows[0]["text"], rows[0]["text_source"]) == ("Daily Digest text", "govinfo-html")
    assert (rows[1]["text"], rows[1]["text_source"]) == (None, None), "a granule whose HTML answers 404 keeps its metadata row"


def test_package_row_without_mods_uses_the_html():
    package = "CDOC-119tdoc2"
    mods, html = f"{WWW}/metadata/pkg/{package}/mods.xml", f"{WWW}/content/pkg/{package}/html/{package}.htm"
    fetcher = FakeFetcher(get_map={html: fixture_bytes("CDOC-119tdoc2.htm")})
    [row] = adapter(G.CongressionalDocuments, fetcher).fetch(Unit(package, "t"))
    assert (row["id"], row["congress"], row["text_source"], row["metadata"]) == (package, 119, "govinfo-html", None)
    assert len(row["text"]) >= G.MIN_TEXT_CHARS
    assert fetcher.requests == [mods, html], "long enough HTML text means the PDF is not fetched"


@pytest.mark.skipif(shutil.which("pdftotext") is None, reason="needs poppler's pdftotext")
def test_package_published_only_as_granules_gives_one_row_per_granule():
    """CDOC-118sdoc2 has no package-level HTML or PDF; its two parts are granules, and a granule's HTML may only say to see its PDF."""
    package, parts = "CDOC-118sdoc2", ["CDOC-118sdoc2-pt1", "CDOC-118sdoc2-pt2"]
    content = f"{WWW}/content/pkg/{package}"
    pdf = minimal_pdf("Report of the Secretary of the Senate, receipts and expenditures")
    fetcher = FakeFetcher(json_map={f"{G.GOVINFO_API}/packages/{package}/granules": {"granules": [{"granuleId": part} for part in parts]}},
                          get_map={f"{content}/html/{parts[0]}.htm": b"<html><body><pre>[TEXT NOT AVAILABLE REFER TO PDF]</pre></body></html>",
                                   f"{content}/pdf/{parts[0]}.pdf": pdf,
                                   f"{content}/html/{parts[1]}.htm": b"<html><body><pre>" + b"Part II. " * 200 + b"</pre></body></html>"})
    documents = adapter(G.CongressionalDocuments, fetcher)
    rows = documents.fetch(Unit(package, "t"))
    assert [row["id"] for row in rows] == parts
    assert {documents.unit_of(row) for row in rows} == {package}
    assert (rows[0]["text_source"], rows[0]["text_url"], rows[0]["text_sha256"]) == ("govinfo-pdf", f"{content}/pdf/{parts[0]}.pdf", hashlib.sha256(pdf).hexdigest())
    assert rows[0]["text"] == "Report of the Secretary of the Senate, receipts and expenditures"
    assert (rows[1]["text_source"], rows[1]["text_url"], rows[1]["url"]) == ("govinfo-html", f"{content}/html/{parts[1]}.htm", f"{WWW}/app/details/{package}/{parts[1]}")
    assert all(row["congress"] == 118 for row in rows)


def test_legacy_law_keeps_one_row_per_unit():
    """LawsAll is a Composite that maps rows to units by id, so a legacy law never takes the granule fallback."""
    package = "PLAW-110publ252"
    granules = f"{G.GOVINFO_API}/packages/{package}/granules"
    fetcher = FakeFetcher(json_map={granules: {"granules": [{"granuleId": f"{package}-pt1"}]}})
    [row] = adapter(G.LegacyLaws, fetcher, name="laws").fetch(Unit(package, "t"))
    assert (row["id"], row["congress"], row["type"], row["number"], row["text"]) == (package, 110, "publ", "252", None)
    assert granules not in fetcher.requests


CRS_ITEM = fixture_json("list-crsreport.json")["CRSReports"][0]
CRS_PDF = "https://www.congress.gov/crs_external_products/IN/PDF/IN12740/IN12740.3.pdf"
CRS_HTML = "https://www.congress.gov/crs_external_products/IN/HTML/IN12740.html"


def test_crs_live_counts_are_per_partition():
    """The API totals only the whole collection, so a single total would be compared with whichever prefixes happen to be complete."""
    listing = fixture_json("list-crsreport.json")
    listing["pagination"] = {"count": len(listing["CRSReports"])}
    crs = adapter(C.CrsReports, FakeFetcher(json_map={f"{C.API}/crsreport": listing}))
    assert crs.live_counts(["IN"]) == {"IN": 1, "RL": 1}


def crs(get_map):
    fetcher = FakeFetcher(json_map={C.strip_query(CRS_ITEM["url"]): fixture_json("detail-crsreport.json")}, get_map=get_map)
    return adapter(C.CrsReports, fetcher), Unit(CRS_ITEM["id"], CRS_ITEM["updateDate"], {"item": CRS_ITEM}), fetcher


def minimal_pdf(line):
    stream = f"BT /F1 12 Tf 72 720 Td ({line}) Tj ET".encode()
    objects = [b"<< /Type /Catalog /Pages 2 0 R >>", b"<< /Type /Pages /Kids [3 0 R] /Count 1 >>",
               b"<< /Type /Page /Parent 2 0 R /MediaBox [0 0 612 792] /Contents 4 0 R /Resources << /Font << /F1 5 0 R >> >> >>",
               b"<< /Length %d >>\nstream\n%s\nendstream" % (len(stream), stream), b"<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica >>"]
    out, offsets = bytearray(b"%PDF-1.4\n"), []
    for number, body in enumerate(objects, 1):
        offsets.append(len(out))
        out += b"%d 0 obj\n%s\nendobj\n" % (number, body)
    xref = len(out)
    out += b"xref\n0 %d\n0000000000 65535 f \n" % (len(objects) + 1) + b"".join(b"%010d 00000 n \n" % o for o in offsets)
    out += b"trailer\n<< /Size %d /Root 1 0 R >>\nstartxref\n%d\n%%%%EOF\n" % (len(objects) + 1, xref)
    return bytes(out)


@pytest.mark.skipif(shutil.which("pdftotext") is None, reason="needs poppler's pdftotext")
def test_crs_text_comes_from_the_pdf_first():
    pdf = minimal_pdf("Hello from CRS")
    reports, unit, fetcher = crs({CRS_PDF: pdf})
    [row] = reports.fetch(unit)
    assert (row["id"], row["version"], row["text"], row["text_source"]) == ("IN12740", "3", "Hello from CRS", "crs-pdf")
    assert (row["text_url"], row["text_sha256"]) == (CRS_PDF, hashlib.sha256(pdf).hexdigest())
    assert CRS_HTML not in fetcher.requests


def test_crs_html_is_used_when_there_is_no_pdf():
    reports, unit, _ = crs({CRS_PDF: 404, CRS_HTML: b"<html><body><p>Hello</p></body></html>"})
    [row] = reports.fetch(unit)
    assert (row["text"], row["text_source"], row["text_url"]) == ("Hello", "crs-html", CRS_HTML)


def test_crs_challenged_html_fails_the_unit_not_the_run():
    reports, unit, _ = crs({CRS_PDF: 404, CRS_HTML: Blocked("bot challenge at www.congress.gov")})
    [(_, result)] = list(reports.fetch_many([unit]))
    assert type(result) is RuntimeError and "bot challenge" in str(result)


def test_crs_challenged_pdf_stops_the_run():
    reports, unit, _ = crs({CRS_PDF: Blocked("bot challenge at www.congress.gov")})
    with pytest.raises(Blocked):
        list(reports.fetch_many([unit]))


def test_every_collection_is_registered_once():
    assert len(BY_NAME) == len(COLLECTIONS) == 21
