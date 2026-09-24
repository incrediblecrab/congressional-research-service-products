"""The store: partition keys, Parquet round trips, and the Hub commit fence against a fake Hub API."""

from pathlib import Path
from types import SimpleNamespace

import httpx
import pyarrow.parquet as pq
import pytest
from huggingface_hub.errors import HfHubHTTPError

from crs_products import store as store_module
from crs_products.store import HubStore, Superseded, git_blob_sha1, partition_of, read_parquet, write_parquet


def http_error(status):
    response = httpx.Response(status, request=httpx.Request("POST", "https://huggingface.co/api/datasets/x/y/commit/main"))
    return HfHubHTTPError(f"{status} from the fake Hub", response=response)


class FakeApi:
    """The parts of HfApi that HubStore.commit uses. A commit on a parent that is not the head answers 412, as the Hub did when measured."""

    token = False

    def __init__(self):
        self.head, self.files, self.commits, self.attempts = "c0", {}, [], 0
        self.lose_next_response = False

    def dataset_info(self, repo_id):
        return SimpleNamespace(sha=self.head)

    def create_commit(self, repo_id, operations, commit_message, repo_type, parent_commit):
        self.attempts += 1
        if parent_commit != self.head:
            raise http_error(412)
        for operation in operations:
            self.files[operation.path_in_repo] = Path(operation.path_or_fileobj).read_bytes()
        self.commits.append(commit_message)
        self.head = f"c{len(self.commits)}"
        if self.lose_next_response:
            self.lose_next_response = False
            raise http_error(502)
        return SimpleNamespace(oid=self.head)

    def get_paths_info(self, repo_id, paths, repo_type, revision):
        return [SimpleNamespace(path=path, blob_id=git_blob_sha1(self.files[path]), lfs=None) for path in paths if path in self.files]


@pytest.fixture
def hub(tmp_path, monkeypatch):
    monkeypatch.setattr(store_module.time, "sleep", lambda seconds: None)
    api = FakeApi()
    return HubStore("x/y", workdir=tmp_path, api=api, card=lambda manifest: f"card for {manifest}"), api


class UnreachableApi(FakeApi):
    def dataset_info(self, repo_id):
        raise httpx.ConnectError("planted: the Hub cannot be reached")


def test_a_hub_that_cannot_be_reached_leaves_no_scratch_directory(tmp_path):
    with pytest.raises(httpx.ConnectError):
        HubStore("x/y", workdir=tmp_path, api=UnreachableApi())
    assert list(tmp_path.iterdir()) == []


def test_a_commit_on_a_stale_parent_is_superseded_and_the_store_writes_nothing_more(hub):
    store, api = hub
    store.stage_manifest({"n": 1})
    assert store.commit("first") == "c1" and set(api.files) == {"manifest.json", "README.md"}
    api.head = "someone-else"
    store.stage_manifest({"n": 2})
    with pytest.raises(Superseded):
        store.commit("second")
    attempts = api.attempts
    store.stage_manifest({"n": 3})
    with pytest.raises(Superseded):
        store.commit("third")
    assert api.attempts == attempts, "a superseded store does not ask the Hub again"
    assert api.commits == ["first"]


def test_a_retried_commit_that_had_landed_is_adopted_not_superseded(hub):
    store, api = hub
    api.lose_next_response = True
    store.stage_manifest({"n": 1})
    assert store.commit("first") == "c1"
    assert api.commits == ["first"] and store.revision == "c1" and store.superseded is None
    store.stage_manifest({"n": 2})
    assert store.commit("second") == "c2"


def test_a_412_whose_head_holds_a_different_manifest_is_superseded(hub):
    store, api = hub
    store.stage_manifest({"n": 1})
    real_create = api.create_commit

    def other_writer_first(*args, **kwargs):
        # Another writer's manifest lands between this store's attempts.
        api.create_commit = real_create
        api.files["manifest.json"] = b'{"n": "theirs"}\n'
        api.head = "theirs"
        raise http_error(502)

    api.create_commit = other_writer_first
    with pytest.raises(Superseded):
        store.commit("first")


def test_git_blob_sha1_matches_git():
    assert git_blob_sha1(b"") == "e69de29bb2d1d6434b8b29ae775ad8c2e48c5391"
    assert git_blob_sha1(b"hello\n") == "ce013625030ba8dba906f756967f9e9ca394464a"


@pytest.mark.parametrize("uid, key", [("R49359", "R49"), ("RL34480", "RL34"), ("IN12740", "IN12"), ("LSB11001", "LSB11"), ("R40", "R0"), ("98-684", "numeric"), ("", "other"), ("abc", "other")])
def test_partition_of(uid, key):
    assert partition_of(uid) == key


def test_rows_round_trip_with_list_columns_and_typed_version(tmp_path):
    path = tmp_path / "p.parquet"
    stats = write_parquet([{"id": "R40002", "version": "3", "authors": None, "metadata": {"a": 1}, "text": "t"}, {"id": "R40001", "topics": ["T"]}], path)
    assert stats["rows"] == 2 and stats["text_rows"] == 1 and len(stats["sha256"]) == 64
    rows = read_parquet(path)
    assert [row["id"] for row in rows] == ["R40001", "R40002"], "rows are sorted by id"
    assert rows[1]["version"] == 3 and rows[1]["authors"] == [] and rows[1]["metadata"] == '{"a": 1}' and rows[0]["topics"] == ["T"]
    assert str(pq.read_schema(path).field("version").type) == "int32"
    empty = write_parquet([], tmp_path / "empty.parquet")
    assert empty["rows"] == 0 and read_parquet(tmp_path / "empty.parquet") == []
