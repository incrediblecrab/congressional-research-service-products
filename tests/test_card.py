"""The dataset card: front matter the Hub's own parser accepts, numbers taken from the manifest, every column documented."""

import yaml
from huggingface_hub import DatasetCard

from crs_products.card import COLUMN_DOCS, render
from crs_products.pipeline import PROBE_KEY, new_manifest, probe_state
from crs_products.store import COLUMNS
from conftest import local_store, run_once, scripted

T1 = "2026-09-01T10:00:00Z"


def front_matter(card):
    assert card.startswith("---\n")
    head, separator, _ = card.removeprefix("---\n").partition("\n---\n")
    assert separator, "the front matter block is closed"
    meta = yaml.safe_load(head)
    assert DatasetCard(card).data.to_dict() == {key: value for key, value in meta.items()}
    return meta


def test_an_empty_manifest_renders_a_card_without_data_files():
    card = render(new_manifest())
    meta = front_matter(card)
    assert "configs" not in meta and meta["license"] == "other" and meta["size_categories"] == ["n<1K"]
    assert "The first sync has not listed the API yet." in card and "| (none yet) | 0 | 0 | 0 | 0 of 0 |" in card


def test_a_synced_manifest_renders_its_numbers(tmp_path):
    store, state = local_store(tmp_path), scripted(units={"R40001": T1, "R40002": T1, "IN12001": T1}, texts={"R40002": None})
    run_once(store, state)
    card = render(store.read_manifest())
    meta = front_matter(card)
    assert meta["configs"] == [{"config_name": "default", "data_files": [{"split": "train", "path": "data/*.parquet"}]}]
    assert "**3 of 3 products** (100.0%)" in card and "2 rows have text" in card and "Last complete sync:" in card
    assert "| R | 2 | 1 | 2 | 1 of 1 |" in card and "| IN | 1 | 1 | 1 | 1 of 1 |" in card


def test_a_partial_backfill_shows_the_share_stored_so_far(tmp_path):
    store, state = local_store(tmp_path), scripted(units={f"R40{n:03d}": T1 for n in range(8)}, stop_after=2)
    run_once(store, state)
    card = render(store.read_manifest())
    assert "**2 of 8 products** (25.0%)" in card and "Last complete sync:" not in card
    assert "| R | 2 | 2 | 8 | 0 of 1 |" in card


def test_every_column_is_documented_in_the_schema_table():
    card = render(new_manifest())
    assert set(COLUMN_DOCS) == set(COLUMNS)
    for column in COLUMNS:
        assert f"| `{column}` |" in card


def test_the_front_matter_carries_the_probe_state_and_no_error_text(tmp_path):
    store, state = local_store(tmp_path), scripted(units={"R40001": T1, "R40002": T1}, stop_after=1)
    run_once(store, state)
    manifest = store.read_manifest()
    assert front_matter(render(manifest))[PROBE_KEY] == probe_state(manifest) and probe_state(manifest)["incomplete"] == []
    reason = "HTTPStatusError: Client error '404 Not Found' for url \"https://x/y?a=b\": #1\n---\nkey: value"
    manifest["runs"].append({"stopped": reason, "ended": T1})
    meta = front_matter(render(manifest))
    assert meta[PROBE_KEY]["failed_runs"] == [{"stopped": "HTTPStatusError", "ended": T1}] and "key" not in meta
    assert reason not in render(manifest).partition("\n---\n")[0]
