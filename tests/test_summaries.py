"""The bill summaries sync against a fake /summaries listing that, like the real one, returns summaries sharing an updateDate in a new order on every request."""

import copy
import json
import math
import random
import re

import pytest

from crs_products import summaries
from crs_products.pipeline import Context
from crs_products.store import LocalStore
from crs_products.summaries import API, SummariesSource, partition_of, summary_id, summary_row, sync
from crs_products.summaries_card import render
from crs_products.verify import verify


def item(congress, bill_type, number, version="00", updated="2026-09-01T10:00:00Z", text="<p>A summary.</p>", title=None):
    return {"actionDate": "2026-01-02", "actionDesc": "Introduced in House", "currentChamber": "House", "currentChamberCode": "H", "lastSummaryUpdateDate": updated, "text": text, "updateDate": updated, "versionCode": version,
            "bill": {"congress": congress, "number": str(number), "originChamber": "House", "originChamberCode": "H", "title": title or f"A bill numbered {number}", "type": bill_type.upper(), "updateDateIncludingText": updated, "url": f"{API}/bill/{congress}/{bill_type}/{number}"}}


class FakeAPI:
    """Answers /congress/current, /summaries[/congress/type] and /bill/.../summaries. The listing filters by fromDateTime and toDateTime (both included) and orders by updateDate, desc unless sort says asc; summaries that share an updateDate come in a new random order on every request. hidden: ids the listing counts but never returns. bill_only: ids the bill endpoint has though the listing lacks them. disorder: requests whose page comes back shuffled."""

    def __init__(self, items=(), current=94, seed=7):
        self.items = {summary_id(i): i for i in items}
        self.current = current
        self.random = random.Random(seed)
        self.hidden, self.bill_only, self.disorder = set(), {}, set()
        self.requests = []

    def json(self, url, params=None):
        params = dict(params or {})
        self.requests.append((url.removeprefix(API), params))
        path = url.removeprefix(API)
        if path == "/congress/current":
            return {"congress": {"number": self.current}}
        bill = re.fullmatch(r"/bill/(\d+)/(\w+)/(\d+)/summaries", path)
        if bill:
            prefix = f"{int(bill[1])}-{bill[2]}-{bill[3]}-"
            known = [i for uid, i in self.items.items() if uid.startswith(prefix)] + [i for uid, i in self.bill_only.items() if uid.startswith(prefix)]
            return {"summaries": [{"versionCode": i["versionCode"]} for i in known]}
        listing = re.fullmatch(r"/summaries(?:/(\d+)/(\w+))?", path)
        assert listing, path
        assert params.get("fromDateTime"), "every listing request names fromDateTime"
        lo, hi = params["fromDateTime"], params.get("toDateTime")
        chosen = [i for uid, i in self.items.items() if (not listing[1] or uid.startswith(f"{int(listing[1])}-{listing[2]}-")) and lo <= i["updateDate"] and (hi is None or i["updateDate"] <= hi)]
        keyed = sorted(((i["updateDate"], self.random.random(), i) for i in chosen), key=lambda t: t[:2], reverse=params.get("sort") != "updateDate asc")
        visible = [i for _, _, i in keyed if summary_id(i) not in self.hidden]
        offset, limit = int(params.get("offset", 0)), int(params.get("limit", 20))
        page = visible[offset:offset + limit]
        if len(self.requests) in self.disorder:
            self.random.shuffle(page)
        return {"pagination": {"count": len(chosen)}, "summaries": copy.deepcopy(page)}

    def listed(self, path):
        return [r for r in self.requests if r[0] == path]


def bulk(congress, bill_type, n, updated, start=1):
    return [item(congress, bill_type, number, updated=updated) for number in range(start, start + n)]


def store_at(tmp_path):
    return LocalStore(tmp_path / "hub", workdir=tmp_path, card=render, write=summaries.write)


def run(store, api, **context):
    ctx = Context(store=store, deadline=context.pop("deadline", math.inf), writer="local", partition_of=partition_of, comparable=summaries.comparable, source_url=summaries.SOURCE_URL, **context)
    return sync(ctx, SummariesSource(api)), ctx


def stored_ids(store, key):
    return {row["id"] for row in store.read_partition(key)}


def age_manifest(store, **stamps):
    """Rewrites the local manifest's stamps, as if runs had happened long ago."""
    path = store.root / "manifest.json"
    manifest = json.loads(path.read_text())
    for name, value in stamps.items():
        if name == "collected_at":
            for entry in manifest["partitions"].values():
                entry["collected_at"] = value
        else:
            manifest[name] = value
    path.write_text(json.dumps(manifest))


def test_a_row_carries_the_summary_and_its_bill():
    row = summary_row(item(119, "hr", 8893, version="07", text=" <p>Line one.</p><ul><li>a</li></ul>", title="A\u00a0bill "))
    assert row["id"] == "119-hr-8893-07" and (row["congress"], row["bill_type"], row["bill_number"], row["version_code"]) == (119, "hr", 8893, "07")
    assert row["text"] == "Line one.\n\n- a" and row["html"] == " <p>Line one.</p><ul><li>a</li></ul>" and row["title"] == "A bill"
    assert partition_of("93-s-1-00") == "093-s" and partition_of("119-hr-8893-07") == "119-hr" and partition_of("R40001") == "other"


def test_collect_reads_every_summary_where_offset_paging_skips_some():
    items = bulk(100, "hr", 700, "2015-10-01T15:15:02Z") + bulk(100, "hr", 300, "2015-10-01T15:14:59Z", start=701) + [item(100, "hr", n, updated=f"2016-01-01T00:00:{n % 60:02d}Z") for n in range(1001, 1201)]
    api = FakeAPI(items)
    source = SummariesSource(api)
    naive, offset = set(), 0
    while offset < 1200:
        naive |= {summary_id(i) for i in source.page("/summaries/100/hr", offset=offset)[1]}
        offset += 250
    assert len(naive) < 1200, "the fake should skip summaries under offset paging, as the API does"
    count, oldest, newest = source.bounds("/summaries/100/hr")
    found, short = source.collect("/summaries/100/hr", oldest, newest)
    assert (count, len(found), short) == (1200, 1200, {})


def test_a_window_includes_both_ends_and_nothing_outside():
    api = FakeAPI([item(94, "hr", 1, updated="2026-09-01T10:00:00Z"), item(94, "hr", 2, updated="2026-09-01T10:00:05Z"), item(94, "hr", 3, updated="2026-09-01T10:00:06Z"), item(94, "hr", 4, updated="2026-09-01T09:59:59Z")])
    found, short = SummariesSource(api).collect("/summaries/94/hr", "2026-09-01T10:00:00Z", "2026-09-01T10:00:05Z")
    assert sorted(found) == ["94-hr-1-00", "94-hr-2-00"] and short == {}


def test_a_crowded_second_that_never_returns_some_summaries_is_recorded():
    api = FakeAPI(bulk(94, "hr", 600, "2026-09-01T10:00:00Z"))
    api.hidden = {"94-hr-5-00", "94-hr-6-00"}
    found, short = SummariesSource(api).collect("/summaries/94/hr", "2026-09-01T10:00:00Z", "2026-09-01T10:00:00Z")
    assert len(found) == 598 and short == {"2026-09-01T10:00:00Z": [600, 598]}
    assert len(api.requests) <= len(summaries.TIE_PLANS) * 4


def test_a_page_out_of_order_is_split_and_still_read_whole():
    api = FakeAPI([item(94, "hr", n, updated=f"2026-09-01T10:{n // 60:02d}:{n % 60:02d}Z") for n in range(1, 400)])
    api.disorder = {1}
    found, short = SummariesSource(api).collect("/summaries/94/hr", "2026-09-01T10:00:00Z", "2026-09-01T11:00:00Z")
    assert len(found) == 399 and short == {}


def backfilled(tmp_path, items):
    api, store = FakeAPI(items), store_at(tmp_path)
    record, _ = run(store, api)
    assert record["finished"] and record["stopped"] is None
    return api, store


BASE = bulk(94, "hr", 30, "2026-09-01T10:00:00Z") + bulk(94, "s", 5, "2026-09-02T10:00:00Z") + bulk(93, "hr", 300, "2015-10-01T15:15:02Z")


def test_a_backfill_reads_every_slice_and_verify_passes(tmp_path):
    api, store = backfilled(tmp_path, BASE)
    manifest = store.read_manifest()
    assert len(manifest["partitions"]) == 16 and all(entry["complete"] for entry in manifest["partitions"].values())
    assert {key: entry["rows"] for key, entry in manifest["partitions"].items() if entry["rows"]} == {"094-hr": 30, "094-s": 5, "093-hr": 300}
    assert manifest["listing"]["count"] == 335 and manifest["through"] == "2026-09-02T10:00:00Z"
    report = verify(store, partition_of=partition_of, tally="bill_type")
    assert report["problems"] == [] and report["rows"] == 335 and report["bill_types"] == {"hr": 330, "s": 5}
    assert "**335 of 335 summaries**" in (store.root / "README.md").read_text()


def test_verify_catches_a_row_in_the_wrong_slice(tmp_path):
    api, store = backfilled(tmp_path, BASE)
    rows = store.read_partition("094-s") + [dict(store.read_partition("094-hr")[0])]
    stats = summaries.write(rows, store.root / "data" / "094-s.parquet")
    manifest = store.read_manifest()
    manifest["partitions"]["094-s"].update(stats)
    (store.root / "manifest.json").write_text(json.dumps(manifest))
    problems = verify(store, partition_of=partition_of, tally="bill_type")["problems"]
    assert any("094-s: ids that belong elsewhere" in p for p in problems) and any("094-s: complete, but 6 rows" in p for p in problems)


def test_a_later_run_reads_only_what_changed_and_rewrites_only_those_slices(tmp_path):
    api, store = backfilled(tmp_path, BASE)
    run(store, api)  # the first daily check
    before = store.read_manifest()["partitions"]
    api.items["94-hr-3-00"] = item(94, "hr", 3, updated="2026-09-03T08:00:00Z", text="<p>Revised.</p>")
    new = item(94, "hr", 31, version="00", updated="2026-09-03T09:00:00Z")
    api.items[summary_id(new)] = new
    api.requests.clear()
    record, _ = run(store, api)
    paths = {path for path, _ in api.requests}
    assert paths == {"/summaries", "/congress/current"}, "no slice is read again before the next daily check"
    assert record["fetched"] >= 2 and record["removed"] == 0 and record["commits"] == 1
    after = store.read_manifest()
    assert after["partitions"]["094-hr"]["rows"] == 31 and after["partitions"]["094-hr"]["listed"] == 31
    assert {key for key in after["partitions"] if after["partitions"][key]["sha256"] != before[key]["sha256"]} == {"094-hr"}
    rows = {row["id"]: row for row in store.read_partition("094-hr")}
    assert rows["94-hr-3-00"]["text"] == "Revised." and "94-hr-31-00" in rows
    assert after["through"] == "2026-09-03T09:00:00Z"
    assert verify(store, partition_of=partition_of, tally="bill_type")["problems"] == []


def test_a_run_limited_to_some_slices_leaves_the_others_and_the_high_water_mark_alone(tmp_path):
    api, store = backfilled(tmp_path, BASE)
    run(store, api)  # the first daily check
    for new in (item(94, "hr", 31, updated="2026-09-03T09:00:00Z"), item(94, "s", 6, updated="2026-09-03T10:00:00Z")):
        api.items[summary_id(new)] = new
    run(store, api, only=frozenset({"094-hr"}))
    after = store.read_manifest()
    assert (after["partitions"]["094-hr"]["rows"], after["partitions"]["094-s"]["rows"]) == (31, 5)
    assert after["through"] == "2026-09-02T10:00:00Z", "a limited run must not move the mark past changes it left unread"
    run(store, api)
    assert store.read_manifest()["partitions"]["094-s"]["rows"] == 6


def test_a_run_with_nothing_new_commits_nothing(tmp_path):
    api, store = backfilled(tmp_path, BASE)
    run(store, api)
    record, _ = run(store, api)
    assert record["commits"] == 0 and record["fetched"] > 0 and record["unchanged"] == record["fetched"]


def test_the_daily_check_removes_a_summary_the_api_no_longer_lists(tmp_path):
    api, store = backfilled(tmp_path, BASE)
    del api.items["94-hr-7-00"]
    age_manifest(store, reconciled_at="2026-01-01T00:00:00Z")
    api.requests.clear()
    record, _ = run(store, api)
    assert record["removed"] == 1 and "94-hr-7-00" not in stored_ids(store, "094-hr")
    assert not any(path.startswith("/bill/") for path, _ in api.requests), "a slice read whole needs no per-bill check"
    manifest = store.read_manifest()
    assert manifest["partitions"]["094-hr"]["rows"] == 29 == manifest["partitions"]["094-hr"]["listed"] and manifest["reconciled"]["differ"] == 1


def test_a_slice_read_short_keeps_what_its_bill_still_has(tmp_path):
    api, store = backfilled(tmp_path, BASE)
    api.hidden = {"94-hr-2-00"}
    del api.items["94-hr-9-00"]
    record, _ = run(store, api, refetch=True, only=frozenset({"094-hr"}))
    ids = stored_ids(store, "094-hr")
    assert "94-hr-2-00" in ids and "94-hr-9-00" not in ids and record["removed"] == 1
    assert sorted(path for path, _ in api.requests if path.startswith("/bill/")) == ["/bill/94/hr/2/summaries", "/bill/94/hr/9/summaries"]


def test_a_listing_that_lost_most_of_a_slice_is_suspect(tmp_path):
    api, store = backfilled(tmp_path, BASE)
    for n in range(1, 25):
        del api.items[f"94-hr-{n}-00"]
    record, _ = run(store, api, refetch=True, only=frozenset({"094-hr"}))
    assert record["suspect_listings"] == 1 and record["removed"] == 0 and len(stored_ids(store, "094-hr")) == 30


def test_the_budget_stops_between_slices_and_the_next_run_resumes(tmp_path):
    api, store = FakeAPI(BASE), store_at(tmp_path)
    reads = []
    original = SummariesSource.bounds

    def bounds(self, path):
        reads.append(path)
        if len(reads) == 3:
            ctx.deadline = 0
        return original(self, path)

    SummariesSource.bounds = bounds
    try:
        ctx = Context(store=store, deadline=math.inf, writer="local", partition_of=partition_of, comparable=summaries.comparable, source_url=summaries.SOURCE_URL)
        first = sync(ctx, SummariesSource(api))
        assert (first["finished"], first["stopped"]) == (False, "budget") and len(reads) == 3
        assert store.read_manifest().get("listing") is None
        second, _ = run(store, api)
    finally:
        SummariesSource.bounds = original
    assert second["finished"] and len(reads) == 16 and len(set(reads)) == 16
    assert verify(store, partition_of=partition_of, tally="bill_type")["problems"] == []


def test_the_daily_check_rereads_the_slices_read_longest_ago(tmp_path):
    api, store = backfilled(tmp_path, BASE)
    age_manifest(store, reconciled_at="2026-01-01T00:00:00Z", collected_at="2026-01-01T00:00:00Z")
    api.requests.clear()
    record, _ = run(store, api)
    # A full read starts with bounds(), the only request on a slice path sorted newest first; the daily counts send no sort.
    reread = [path for path, params in api.requests if path.startswith("/summaries/") and params.get("sort") == "updateDate desc"]
    assert reread == ["/summaries/93/hconres"], "ceil(16 / 30) = 1 slice, the first by (collected_at, congress, type)"
    collected = {key: entry["collected_at"] for key, entry in store.read_manifest()["partitions"].items()}
    assert collected.pop("093-hconres") > "2026-01-01T00:00:00Z" and set(collected.values()) == {"2026-01-01T00:00:00Z"}
    assert record["finished"]


def test_every_new_dataset_card_documents_every_column():
    from crs_products import constitution, constitution_card, summaries_card

    for module, schema in ((summaries_card, summaries.SCHEMA), (constitution_card, constitution.SCHEMA)):
        assert set(module.COLUMN_DOCS) == set(schema.names)
        text = module.render({})
        assert text.startswith("---\n") and "The first sync has not listed" in text


@pytest.mark.parametrize("congress, years", [(93, "1973-1974"), (119, "2025-2026")])
def test_the_card_gives_each_congress_its_years(congress, years):
    from crs_products.summaries_card import years as years_of

    assert years_of(congress) == years
