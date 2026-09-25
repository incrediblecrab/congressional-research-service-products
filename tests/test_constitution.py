"""The Constitution Annotated adapter on GovInfo-shaped answers: ids and headings, the listing, missing units, rows, and a sync through the products' loop."""

import json
import math
import shutil

import httpx
import pytest

from crs_products import constitution
from crs_products.constitution import API, SINCE, ConanSource, comparable, headings, partition_of, split_id
from crs_products.constitution_card import render
from crs_products.pipeline import Context, Unit, sync
from crs_products.store import LocalStore
from crs_products.verify import verify
from conftest import FakeFetcher
from test_source import tiny_pdf

needs_pdftotext = pytest.mark.skipif(not shutil.which("pdftotext"), reason="pdftotext is not installed")
COLLECTION = f"{API}/collections/GPO/{SINCE}"
PACKAGES = [
    {"packageId": "GPO-CONAN-1992", "lastModified": "2025-03-07T12:00:00Z", "title": "The Constitution of the United States of America: Analysis and Interpretation"},
    {"packageId": "GPO-CONAN-2024-SUPP", "lastModified": "2025-08-18T09:00:00Z", "title": "Constitution of the United States of America: Analysis and Interpretation, 2024 Supplement"},
    {"packageId": "GPO-OTHER-2020", "lastModified": "2026-01-01T00:00:00Z", "title": "Not the Constitution Annotated"},
]
GRANULES = ["GPO-CONAN-1992-1", "GPO-CONAN-1992-9", "GPO-CONAN-1992-9-1", "GPO-CONAN-1992-9-2"]


def summary_url(uid):
    package = partition_of(uid)
    return f"{API}/packages/{package}/summary" if uid == package else f"{API}/packages/{package}/granules/{uid}/summary"


def record(uid, title, pdf=True, modified="2025-03-07T12:00:00Z"):
    package = partition_of(uid)
    out = {"title": title, "dateIssued": "1992-01-01", "lastModified": modified, "detailsLink": f"https://www.govinfo.gov/app/details/{package}/{uid}"}
    if pdf:
        out["download"] = {"pdfLink": f"{API}/packages/{package}/granules/{uid}/pdf"}
    return out


def govinfo(get_map=None):
    """A fetcher answering the collection in two pages, each package's granules, and the given GETs."""
    json_map = {
        COLLECTION: lambda params: {"packages": PACKAGES[:2], "nextPage": f"{COLLECTION}?offsetMark=next"},
        f"{COLLECTION}?offsetMark=next": lambda params: {"packages": PACKAGES[2:], "nextPage": None},
        f"{API}/packages/GPO-CONAN-1992/granules": lambda params: {"granules": [{"granuleId": uid} for uid in GRANULES], "nextPage": None},
        f"{API}/packages/GPO-CONAN-2024-SUPP/granules": lambda params: {"granules": [], "nextPage": None},
    }
    return FakeFetcher(json_map=json_map, get_map=get_map or {})


def test_an_id_splits_into_its_package_and_position():
    assert split_id("GPO-CONAN-1992-9-1") == ("GPO-CONAN-1992", (9, 1))
    assert split_id("GPO-CONAN-REV-2016-3") == ("GPO-CONAN-REV-2016", (3,))
    assert split_id("GPO-CONAN-2024-SUPP") == ("GPO-CONAN-2024-SUPP", ())
    assert split_id("GPO-CONAN-2020-SUPP-2") == ("GPO-CONAN-2020-SUPP", (2,))
    assert split_id("GPO-CONAN-1992-9a") is None and partition_of("R40001") == "other"
    assert headings(GRANULES) == {"GPO-CONAN-1992-9"}


def test_the_listing_skips_headings_and_keeps_a_package_without_granules():
    source = ConanSource(govinfo())
    head, items = source.list_all()
    assert sorted(items) == ["GPO-CONAN-1992-1", "GPO-CONAN-1992-9-1", "GPO-CONAN-1992-9-2", "GPO-CONAN-2024-SUPP"]
    assert head == {"count": 4, "newest": "GPO-CONAN-2024-SUPP@2025-08-18T09:00:00Z"}
    assert items["GPO-CONAN-1992-9-1"]["updateDate"] == "2025-03-07T12:00:00Z" and "GPO-OTHER-2020" not in source.packages


def test_a_missing_unit_is_none_and_any_other_refusal_raises():
    fetcher = govinfo({
        summary_url("GPO-CONAN-2030-SUPP"): 404,
        summary_url("GPO-CONAN-1992-99"): (400, b'{"message":"invalid granuleId"}'),
        summary_url("GPO-CONAN-1992-9"): (400, b'{"message":"The requested resource does not exist."}'),
    })
    source = ConanSource(fetcher)
    assert source.summary("GPO-CONAN-2030-SUPP") is None and not source.exists("GPO-CONAN-1992-99")
    with pytest.raises(httpx.HTTPStatusError):
        source.summary("GPO-CONAN-1992-9")


@needs_pdftotext
def test_a_row_carries_the_text_positions_and_provenance():
    uid = "GPO-CONAN-1992-9-1"
    pdf = tiny_pdf("First Amendment")
    source = ConanSource(govinfo({summary_url(uid): json.dumps(record(uid, "First\u00a0Amendment ")).encode(), f"{API}/packages/GPO-CONAN-1992/granules/{uid}/pdf": pdf}))
    source.list_all()
    row = source.fetch(Unit(uid, "2025-03-07T12:00:00Z"))
    assert (row["package_id"], row["kind"], row["sequence"], row["subsequence"], row["pages"]) == ("GPO-CONAN-1992", "edition", 9, 1, 1)
    assert row["title"] == "First Amendment" and "First Amendment" in row["text"] and row["url"].endswith(f"/GPO-CONAN-1992/{uid}")
    assert len(row["pdf_sha256"]) == 64 and json.loads(row["metadata"])["lastModified"] == "2025-03-07T12:00:00Z"


@pytest.mark.parametrize("answer, message", [(None, "has no PDF link"), (b"<html>not a pdf</html>", "did not answer a PDF")])
def test_a_unit_without_a_pdf_fails(answer, message):
    uid = "GPO-CONAN-1992-1"
    get_map = {summary_url(uid): json.dumps(record(uid, "Title Page", pdf=answer is not None)).encode()}
    if answer is not None:
        get_map[f"{API}/packages/GPO-CONAN-1992/granules/{uid}/pdf"] = answer
    with pytest.raises(RuntimeError, match=message):
        ConanSource(govinfo(get_map)).fetch(Unit(uid, None))


def test_a_restamped_unit_compares_equal_and_a_changed_one_does_not():
    row = {"id": "GPO-CONAN-1992-1", "package_id": "GPO-CONAN-1992", "text": "a", "pages": 1, "updated_at": "2025-03-07T12:00:00Z", "fetched_at": "x", "metadata": json.dumps({"title": "T", "lastModified": "2025-03-07T12:00:00Z"})}
    restamped = dict(row, updated_at="2025-08-18T09:00:00Z", fetched_at="y", metadata=json.dumps({"title": "T", "lastModified": "2025-08-18T09:00:00Z"}))
    assert comparable(row) == comparable(restamped)
    assert comparable(row) != comparable(dict(restamped, text="b"))
    assert comparable(row) != comparable(dict(restamped, metadata=json.dumps({"title": "U", "lastModified": "2025-08-18T09:00:00Z"})))


@needs_pdftotext
def test_a_sync_through_the_products_loop_verifies(tmp_path):
    get_map = {}
    for uid in ("GPO-CONAN-1992-1", "GPO-CONAN-1992-9-1", "GPO-CONAN-1992-9-2", "GPO-CONAN-2024-SUPP"):
        get_map[summary_url(uid)] = json.dumps(record(uid, f"Part {uid}")).encode()
        get_map[f"{API}/packages/{partition_of(uid)}/granules/{uid}/pdf"] = tiny_pdf(uid)
    store = LocalStore(tmp_path / "hub", workdir=tmp_path, card=render, write=constitution.write)
    ctx = Context(store=store, deadline=math.inf, writer="local", partition_of=partition_of, comparable=comparable, source_url=constitution.SOURCE_URL)
    run = sync(ctx, ConanSource(govinfo(get_map)))
    assert run["finished"] and run["fetched"] == 4 and run["failed"] == 0
    report = verify(store, partition_of=partition_of, tally="kind")
    assert report["problems"] == [] and report["kinds"] == {"edition": 3, "supplement": 1}
    assert "**4 of 4 units**" in (tmp_path / "hub" / "README.md").read_text()
    again = sync(Context(store=store, deadline=math.inf, writer="local", partition_of=partition_of, comparable=comparable, source_url=constitution.SOURCE_URL), ConanSource(govinfo(get_map)))
    assert again["fetched"] == 0 and again["commits"] == 0
