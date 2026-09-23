"""The sync loop against a scripted source: completion, idleness, change detection, retries, guards, resumption."""

from conftest import run_once, scripted_collection

from ijab.pipeline import MAX_ATTEMPTS
from ijab.store import LocalStore


def store_at(tmp_path):
    return LocalStore(tmp_path / "repo", workdir=tmp_path)


def partition(store, name="scripted"):
    return store.read_manifest(name)["partitions"]["p"]


def stored(store, name="scripted"):
    return {row["id"]: row for row in store.read_partition(name, "p")}


def test_first_sync_completes(tmp_path):
    store = store_at(tmp_path)
    collection, state = scripted_collection(units={"a": "1", "b": "1", "c": "1"})
    result = run_once(store, collection)
    entry = partition(store)
    assert result["finished"] and result["fetched"] == 3
    assert entry["complete"] and entry["fingerprint"]
    assert (entry["rows"], entry["units"], entry["source_count"], entry["text_rows"]) == (3, 3, 3, 3)
    assert list(stored(store)) == ["a", "b", "c"]


def test_idle_rerun_makes_no_request_and_no_commit(tmp_path):
    store = store_at(tmp_path)
    collection, state = scripted_collection(units={"a": "1", "b": "1"})
    run_once(store, collection)
    commits = len(store.commits)
    result = run_once(store, collection)
    assert result["finished"] and result["fetched"] == 0 and result["commits"] == 0
    assert state.fetched == ["a", "b"]
    assert len(store.commits) == commits


def test_changed_new_and_removed_units(tmp_path):
    store = store_at(tmp_path)
    collection, state = scripted_collection(units={"a": "1", "b": "1", "c": "1"})
    run_once(store, collection)
    state.units = {"a": "2", "b": "1", "d": "1"}
    result = run_once(store, collection)
    rows = stored(store)
    assert state.fetched[3:] == ["a", "d"]
    assert result["removed"] == 1
    assert list(rows) == ["a", "b", "d"]
    assert rows["a"]["title"] == "a as of 2" and rows["a"]["updated_at"] == "2"
    assert partition(store)["complete"]


def test_failures_retry_then_exhaust_then_reset_on_change(tmp_path):
    store = store_at(tmp_path)
    collection, state = scripted_collection(units={"a": "1", "x": "1"}, fail={"x"})
    for attempt in range(1, MAX_ATTEMPTS + 1):
        run_once(store, collection)
        manifest = store.read_manifest("scripted")
        assert manifest["failures"]["x"]["attempts"] == attempt
        assert manifest["partitions"]["p"]["complete"] == (attempt == MAX_ATTEMPTS)
    entry = partition(store)
    assert (entry["units"], entry["failed_units"], entry["source_count"]) == (1, 1, 2)

    fetched = len(state.fetched)
    run_once(store, collection)
    assert len(state.fetched) == fetched, "an exhausted unit is not retried while its last-modified value is unchanged"

    state.units["x"] = "2"
    state.fail = set()
    run_once(store, collection)
    entry = partition(store)
    assert state.fetched[-1] == "x"
    assert "x" not in store.read_manifest("scripted")["failures"]
    assert entry["complete"] and (entry["units"], entry["failed_units"]) == (2, 0)


def test_suspect_listing_is_skipped_but_small_removals_proceed(tmp_path):
    store = store_at(tmp_path)
    units = {f"u{i:02d}": "1" for i in range(30)}
    collection, state = scripted_collection(units=dict(units))
    run_once(store, collection)

    state.units = dict(list(units.items())[:5])
    result = run_once(store, collection)
    assert result["suspect_listings"] == 1 and result["removed"] == 0
    assert len(stored(store)) == 30

    state.units = dict(list(units.items())[:27])
    result = run_once(store, collection)
    assert result["suspect_listings"] == 0 and result["removed"] == 3
    assert len(stored(store)) == 27


def test_out_of_time_run_resumes_without_refetching(tmp_path):
    store = store_at(tmp_path)
    collection, state = scripted_collection(units={k: "1" for k in "abcde"}, stop_after=2)
    result = run_once(store, collection)
    assert not result["finished"] and result["stopped"] == "budget"
    assert not partition(store)["complete"] and partition(store)["rows"] == 2

    state.stop_after = None
    result = run_once(store, collection)
    assert result["finished"] and partition(store)["complete"]
    assert sorted(state.fetched) == list("abcde"), "each unit fetched exactly once across the two runs"


def test_max_units_cap_leaves_partition_incomplete_until_finished(tmp_path):
    store = store_at(tmp_path)
    collection, state = scripted_collection(units={k: "1" for k in "abcde"})
    run_once(store, collection, max_units=2)
    entry = partition(store)
    assert entry["rows"] == 2 and not entry["complete"] and entry["fingerprint"] is None and entry["source_count"] == 5

    run_once(store, collection)
    entry = partition(store)
    assert entry["rows"] == 5 and entry["complete"] and entry["fingerprint"]


def test_incremental_listing_never_removes(tmp_path):
    store = store_at(tmp_path)
    collection, state = scripted_collection(units={"a": "1", "b": "1", "c": "1"})
    run_once(store, collection)
    state.units, state.complete_listing = {"d": "1"}, False
    result = run_once(store, collection)
    entry = partition(store)
    assert result["removed"] == 0
    assert list(stored(store)) == ["a", "b", "c", "d"]
    assert entry["source_count"] == 4 and entry["units"] == 4


def test_partitions_outside_only_are_skipped(tmp_path):
    store = store_at(tmp_path)
    collection, state = scripted_collection(units={"a": "1"})
    result = run_once(store, collection, only=frozenset({"other"}))
    assert result["finished"] and state.fetched == []
    assert store.read_manifest("scripted")["partitions"] == {}
