"""The sync loop against a scripted source and a local store."""

from datetime import datetime, timedelta, timezone

import pytest

from crs_products import pipeline
from crs_products.http import Blocked
from crs_products.pipeline import MAX_ATTEMPTS, Partition, Unit, decide, order
from crs_products.store import CARD, MANIFEST, LocalStore, Superseded
from crs_products.card import render
from conftest import ScriptedSource, local_store, run_once, scripted

T1, T2 = "2026-09-01T10:00:00Z", "2026-09-02T10:00:00Z"


def stored(store, key):
    return {row["id"]: row for row in store.read_partition(key)}


def shift_clock(monkeypatch, hours):
    """Moves the pipeline's own clock (retry and text-retry times), not the lease's."""
    monkeypatch.setattr(pipeline, "utcnow", lambda: (datetime.now(timezone.utc) + timedelta(hours=hours)).strftime(pipeline.STAMP))


def test_first_sync_stores_every_product_and_publishes_the_listing(tmp_path):
    store, state = local_store(tmp_path), scripted(units={"R40001": T1, "R40002": T1, "R40003": T1, "IN12001": T2})
    run = run_once(store, state)
    assert run["finished"] and run["stopped"] is None and run["fetched"] == 4
    m = store.read_manifest()
    assert set(stored(store, "R40")) == {"R40001", "R40002", "R40003"} and set(stored(store, "IN12")) == {"IN12001"}
    assert all(m["partitions"][key]["complete"] for key in ("R40", "IN12"))
    assert m["listing"]["count"] == 4 and m["listing"]["listed"] == 4 and m["listing"]["newest"] == f"IN12001@{T2}"
    assert m["listing"]["partitions"] == {"IN12": 1, "R40": 3}
    assert m["writer"]["by"] == "local" and m["runs"][-1]["fetched"] == 4
    row = stored(store, "R40")["R40001"]
    assert row["updated_at"] == T1 and row["fetched_at"] and row["authors"] == ["A. Author"]
    assert decide(m, ScriptedSource(state).head(), "local") == (False, "up to date")


def test_every_commit_carries_the_manifest_and_the_card_rendered_from_it(tmp_path):
    store, state = local_store(tmp_path), scripted(units={"R40001": T1, "IN12001": T2})
    run_once(store, state)
    assert store.commits and all({MANIFEST, CARD} <= set(commit["files"]) for commit in store.commits)
    assert store.read_text(CARD) == render(store.read_manifest())


def test_idle_rerun_fetches_and_commits_nothing(tmp_path):
    store, state = local_store(tmp_path), scripted(units={"R40001": T1, "IN12001": T2})
    run_once(store, state)
    commits, fetched = len(store.commits), len(state.fetched)
    run = run_once(store, state)
    assert run["finished"] and run["commits"] == 0
    assert len(store.commits) == commits and len(state.fetched) == fetched


def test_a_run_that_fetches_claims_the_lease_before_its_first_fetch(tmp_path):
    fetched_at_commit = []

    class Recording(LocalStore):
        def commit(self, message):
            fetched_at_commit.append((message, len(state.fetched)))
            return super().commit(message)

    store = Recording(tmp_path / "hub", workdir=tmp_path, card=render)
    state = scripted(units={"R40001": T1, "R40002": T1})
    run_once(store, state)
    state.units["R40003"] = T2
    before = len(state.fetched)
    run = run_once(store, state)
    claims = [(message, n) for message, n in fetched_at_commit if "takes the writer lease" in message]
    assert claims[-1][1] == before, "the claim commit happens before the run fetches anything"
    assert run["commits"] == 2, "a real-time run commits twice: the claim, then the result"


def test_changed_new_and_removed_products(tmp_path):
    store, state = local_store(tmp_path), scripted(units={"R40001": T1, "R40002": T1, "R40003": T1})
    run_once(store, state)
    state.fetched.clear()
    state.units.update({"R40001": T2, "R40004": T2})
    del state.units["R40002"]
    run = run_once(store, state)
    assert sorted(state.fetched) == ["R40001", "R40004"] and run["removed"] == 1
    assert state.exists_asked == ["R40002"], "removal is confirmed with the API first"
    rows = stored(store, "R40")
    assert set(rows) == {"R40001", "R40003", "R40004"} and rows["R40001"]["updated_at"] == T2
    assert store.read_manifest()["partitions"]["R40"]["listed"] == 3


def test_a_product_missing_from_the_listing_but_still_served_is_kept(tmp_path):
    store, state = local_store(tmp_path), scripted(units={"R40001": T1, "R40002": T1, "R40003": T1})
    run_once(store, state)
    state.fetched.clear()
    del state.units["R40002"]
    state.exists = {"R40002"}
    run = run_once(store, state)
    assert run["removed"] == 0 and state.fetched == []
    assert set(stored(store, "R40")) == {"R40001", "R40002", "R40003"}
    entry = store.read_manifest()["partitions"]["R40"]
    assert entry["complete"] and entry["rows"] + entry["failed"] == entry["listed"] == 3


def test_every_product_of_a_partition_removed(tmp_path):
    store, state = local_store(tmp_path), scripted(units={"R40001": T1, "IN12001": T1})
    run_once(store, state)
    del state.units["IN12001"]
    run = run_once(store, state)
    assert run["removed"] == 1 and stored(store, "IN12") == {}
    assert "IN12" not in store.read_manifest()["listing"]["partitions"]


def test_failures_are_retried_then_exhausted_then_retried_daily(tmp_path, monkeypatch):
    store, state = local_store(tmp_path), scripted(units={"R40001": T1, "R40002": T1}, fail={"R40002"})
    for attempt in range(1, MAX_ATTEMPTS + 1):
        run_once(store, state)
        m = store.read_manifest()
        assert m["failures"]["R40002"]["attempts"] == attempt
        assert m["partitions"]["R40"]["complete"] == (attempt == MAX_ATTEMPTS), "a product with attempts left keeps its partition open"
    entry = m["partitions"]["R40"]
    assert entry["rows"] == 1 and entry["failed"] == 1 and entry["listed"] == 2
    state.fetched.clear()
    run_once(store, state)
    assert state.fetched == [], "an exhausted product is left alone until its retry is due"
    shift_clock(monkeypatch, pipeline.RETRY_AFTER_HOURS + 1)
    run_once(store, state)
    assert state.fetched == ["R40002"] and store.read_manifest()["failures"]["R40002"]["attempts"] == MAX_ATTEMPTS + 1
    monkeypatch.undo()
    state.fetched.clear()
    state.units["R40002"] = T2
    run_once(store, state)
    assert state.fetched == ["R40002"] and store.read_manifest()["failures"]["R40002"]["attempts"] == 1, "a changed product starts its attempts over"


def test_products_without_text_are_fetched_again_after_a_week(tmp_path, monkeypatch):
    store, state = local_store(tmp_path), scripted(units={"R40001": T1, "R40002": T1}, texts={"R40002": None})
    run_once(store, state)
    entry = store.read_manifest()["partitions"]["R40"]
    assert entry["text_rows"] == 1 and entry["text_retry_at"]
    state.fetched.clear()
    run_once(store, state)
    assert state.fetched == []
    shift_clock(monkeypatch, pipeline.TEXT_RETRY_HOURS + 1)
    state.texts = {}
    run_once(store, state)
    assert state.fetched == ["R40002"]
    entry = store.read_manifest()["partitions"]["R40"]
    assert entry["text_rows"] == 2 and entry["text_retry_at"] is None


def test_suspect_listing_is_skipped_without_removing_anything(tmp_path):
    units = {f"R40{n:03d}": T1 for n in range(30)}
    store, state = local_store(tmp_path), scripted(units=dict(units))
    run_once(store, state)
    state.units = dict(list(units.items())[:10])
    run = run_once(store, state)
    assert run["suspect_listings"] == 1 and run["removed"] == 0 and state.exists_asked == []
    assert len(stored(store, "R40")) == 30


def test_a_run_cut_by_the_budget_resumes_without_refetching(tmp_path):
    units = {f"R40{n:03d}": T1 for n in range(5)}
    store, state = local_store(tmp_path), scripted(units=dict(units), stop_after=2)
    run = run_once(store, state)
    assert run["stopped"] == "budget" and len(stored(store, "R40")) == 2
    m = store.read_manifest()
    assert not m["partitions"]["R40"]["complete"] and m["listing"] is None, "only a finished run publishes the listing"
    assert decide(m, ScriptedSource(state).head(), "local")[0]
    state.stop_after = None
    state.fetched.clear()
    run = run_once(store, state)
    assert run["finished"] and len(state.fetched) == 3 and len(stored(store, "R40")) == 5


def test_only_and_max_units_bound_a_smoke_run(tmp_path):
    store, state = local_store(tmp_path), scripted(units={"R40001": T1, "R40002": T1, "IN12001": T1})
    run = run_once(store, state, only=frozenset({"R40"}), max_units=1)
    assert state.fetched == ["R40001"] and run["finished"]
    m = store.read_manifest()
    assert "IN12" not in m["partitions"] and not m["partitions"]["R40"]["complete"] and m["listing"] is None


def test_refetch_fetches_again(tmp_path):
    store, state = local_store(tmp_path), scripted(units={"R40001": T1, "R40002": T1})
    run_once(store, state)
    state.fetched.clear()
    run_once(store, state, refetch=True)
    assert sorted(state.fetched) == ["R40001", "R40002"]


def stamp(minutes_ago):
    return (datetime.now(timezone.utc) - timedelta(minutes=minutes_ago)).strftime(pipeline.STAMP)


@pytest.mark.parametrize("by, minutes_ago, deferred", [("github-actions", 5, True), ("github-actions", 60, False), ("local", 5, False)])
def test_another_writers_fresh_lease_defers_the_run(tmp_path, by, minutes_ago, deferred):
    store, state = local_store(tmp_path), scripted(units={"R40001": T1})
    run_once(store, state)
    m = store.read_manifest()
    m["writer"] = {"by": by, "at": stamp(minutes_ago)}
    store.stage_manifest(m)
    store.commit("someone else wrote")
    commits = len(store.commits)
    state.units["R40002"] = T2
    state.fetched.clear()
    run = run_once(store, state)
    assert (run["stopped"] == "deferred") == deferred
    assert (state.fetched == []) == deferred and (len(store.commits) == commits) == deferred


def test_a_fatal_error_keeps_the_fetched_work_and_backs_the_probe_off(tmp_path):
    store, state = local_store(tmp_path), scripted(units={"R40001": T1, "R40002": T1, "R40003": T1}, fatal={"R40003": Blocked("bot challenge at www.congress.gov/x.pdf")})
    run = run_once(store, state)
    assert run["stopped"].startswith("Blocked") and not run["finished"]
    assert set(stored(store, "R40")) == {"R40001", "R40002"}, "products fetched before the stop are committed"
    m = store.read_manifest()
    assert m["failures"] == {}, "a fatal error is not held against the product"
    assert m["runs"][-1]["stopped"].startswith("Blocked")
    needed, reason = decide(m, ScriptedSource(state).head(), "local")
    assert not needed and reason.startswith("backing off 15 minutes")


def test_a_superseded_store_stops_the_run(tmp_path):
    class Refusing(LocalStore):
        def commit(self, message):
            if self.staged and self.commits:
                raise Superseded("another writer committed")
            return super().commit(message)

    store = Refusing(tmp_path / "hub", workdir=tmp_path, card=render)
    state = scripted(units={"R40001": T1, "IN12001": T1})
    run = run_once(store, state)
    assert run["stopped"] == "superseded" and len(store.commits) == 1


def test_order_puts_changed_complete_partitions_before_the_backfill():
    units = lambda *ids: {uid: Unit(uid, stamp) for uid, stamp in ids}
    partitions = {
        "A1": Partition("A1", units(("A1001", "2026-01-01"))),
        "B1": Partition("B1", units(("B1001", "2026-03-01"))),
        "C1": Partition("C1", units(("C1001", "2026-02-01"))),
        "D1": Partition("D1", units(("D1001", "2026-04-01"))),
        "E1": Partition("E1", units(("E1001", "2026-05-01"))),
    }
    manifest = {"failures": {}, "partitions": {
        "A1": {"complete": True, "fingerprint": "stale"},
        "D1": {"complete": True, "fingerprint": partitions["D1"].fingerprint},
        "E1": {"complete": False},
    }}
    assert [p.key for p in order(manifest, partitions)] == ["A1", "E1", "B1", "C1", "D1"]


def test_decide():
    head = {"count": 3, "newest": f"R40003@{T2}"}
    listing = {"count": 3, "newest": f"R40003@{T2}", "listed": 3, "at": stamp(10), "partitions": {"R40": 3}}
    done = {"partitions": {"R40": {"complete": True}}, "listing": listing, "runs": [{"stopped": None, "ended": stamp(10)}], "writer": {"by": "local", "at": stamp(10)}}
    assert decide(None, head, "local") == (True, "no manifest yet")
    assert decide(done, head, "local") == (False, "up to date")
    assert decide(done, head, "github-actions")[1].startswith("deferred")
    assert decide(dict(done, writer={"by": "local", "at": stamp(50)}), head, "github-actions") == (False, "up to date")
    assert decide(dict(done, listing=None), head, "local") == (True, "never listed")
    assert decide(dict(done, partitions={"R40": {"complete": False}}), head, "local")[1].startswith("1 partitions incomplete")
    assert decide(done, dict(head, count=4), "local") == (True, "count 3 -> 4")
    assert decide(done, dict(head, newest="R40004@x"), "local")[1].startswith("newest")
    assert decide(dict(done, listing=dict(listing, at=stamp(25 * 60))), head, "local")[1].startswith("last full listing")
    failed = [{"stopped": "Blocked: x", "ended": stamp(5)}]
    assert decide(dict(done, runs=failed), head, "local")[1].startswith("backing off 15 minutes")
    assert decide(dict(done, runs=failed * 3), head, "local")[1].startswith("backing off 60 minutes")
    assert decide(dict(done, runs=[{"stopped": "Blocked: x", "ended": stamp(20)}]), head, "local") == (False, "up to date")
