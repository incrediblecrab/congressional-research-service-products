"""The verification gate: a clean store passes, and each planted defect is caught, through the function and the CLI exit code."""

import json
import shutil

import pytest
from conftest import run_once, scripted_collection

from ijab import cli
from ijab.store import LocalStore, read_parquet, write_parquet
from ijab.verify import check_collection

UNITS = 23


def synced(tmp_path, name="scripted"):
    collection, state = scripted_collection(name, units={f"u{i:02d}": "1" for i in range(UNITS)})
    store = LocalStore(tmp_path / "repo", workdir=tmp_path)
    run_once(store, collection)
    return store, collection, state


def edit_manifest(store, name, change):
    path = store.root / "manifests" / f"{name}.json"
    manifest = json.loads(path.read_text())
    change(manifest["partitions"]["p"])
    path.write_text(json.dumps(manifest))


def plant_duplicate(store, name):
    """Duplicates one row and brings the manifest's rows and sha256 up to date, so only the id check can catch it."""
    path = store.root / "data" / name / "p.parquet"
    rows = read_parquet(path)
    stats = write_parquet(rows + rows[:1], path)
    edit_manifest(store, name, lambda entry: entry.update(rows=stats["rows"], sha256=stats["sha256"]))


def test_clean_store_passes(tmp_path):
    store, collection, _ = synced(tmp_path)
    problems, summary = check_collection(store, collection)
    assert problems == []
    assert (summary["rows"], summary["units"], summary["complete"], summary["hash_checked"]) == (UNITS, UNITS, 1, 1)


def plant_rows(store, name):
    edit_manifest(store, name, lambda entry: entry.update(rows=entry["rows"] + 1))


def plant_sha(store, name):
    edit_manifest(store, name, lambda entry: entry.update(sha256="0" * 64))


def plant_missing(store, name):
    (store.root / "data" / name / "p.parquet").unlink()


def plant_unlisted(store, name):
    shutil.copyfile(store.root / "data" / name / "p.parquet", store.root / "data" / name / "extra.parquet")


def plant_source_count(store, name):
    edit_manifest(store, name, lambda entry: entry.update(source_count=entry["source_count"] + 1))


@pytest.mark.parametrize("plant,expected", [
    (plant_duplicate, "duplicate ids"),
    (plant_rows, "rows in file, manifest says"),
    (plant_sha, "differs from manifest"),
    (plant_missing, "no such file"),
    (plant_unlisted, "is not in the manifest"),
    (plant_source_count, "listed"),
])
def test_planted_defect_is_caught(tmp_path, plant, expected):
    store, collection, _ = synced(tmp_path)
    plant(store, collection.name)
    problems, _ = check_collection(store, collection)
    assert len(problems) == 1 and expected in problems[0], problems


@pytest.mark.parametrize("extra,flagged", [(20, True), (5, False)])
def test_live_drift_beyond_tolerance_is_caught(tmp_path, extra, flagged):
    store, collection, state = synced(tmp_path)
    state.live = {"p": UNITS + extra}
    problems, summary = check_collection(store, collection, live=True)
    assert bool(problems) == flagged, problems
    assert summary["live_compared"] == 1 and summary["live_drift"] == int(flagged)


def test_cli_verify_exit_code_gate(tmp_path, capsys):
    store, collection, _ = synced(tmp_path, name="congresses")
    argv = ["verify", "--local", str(store.root), "--collections", "congresses"]
    assert cli.main(argv) == 0
    assert json.loads(capsys.readouterr().out)["problems"] == []
    plant_duplicate(store, "congresses")
    assert cli.main(argv) == 1
    problems = json.loads(capsys.readouterr().out)["problems"]
    assert len(problems) == 1 and "duplicate ids" in problems[0]
