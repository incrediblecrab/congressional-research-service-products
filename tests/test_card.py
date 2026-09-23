"""The dataset card: valid YAML front matter with a config only for collections that have files, and a status row per collection."""

import yaml

from ijab.card import render


def test_card_front_matter_and_status():
    manifest = {"partitions": {"R": {"rows": 2, "text_rows": 2, "units": 2, "source_count": 5, "complete": False}},
                "failures": {"R1": {"attempts": 3}, "R2": {"attempts": 1}}, "updated_at": "2026-09-23T22:00:00Z"}
    text = render({"crs_reports": manifest}, ["data/crs_reports/R.parquet"])
    front = yaml.safe_load(text.split("---\n")[1])
    assert front["configs"] == [{"config_name": "crs_reports", "data_files": [{"split": "train", "path": "data/crs_reports/*.parquet"}], "default": True}]
    assert front["license"] == "other" and front["license_name"] and front["license_link"]
    assert "| crs_reports | 2 | 2 | 2 | 5 | 1 | 0 / 1 | 2026-09-23 22:00:00 |" in text
    assert "| bills | not started |" in text
