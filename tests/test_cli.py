"""The CLI and its workflow: squash only past the commit threshold, the Trusted Publishing resource derived from --repo inside GitHub Actions, and a lane matrix equal to the registry's lanes."""

import json
import os
import re
from pathlib import Path

import huggingface_hub
import pytest
import yaml

from ijab import cli
from ijab.collections import LANES
from ijab.store import SQUASH_AFTER_COMMITS

WORKFLOW = Path(__file__).resolve().parent.parent / ".github" / "workflows" / "pipeline.yml"


class FakeApi:
    commits = 0
    squashed = []

    def list_repo_commits(self, repo_id, repo_type=None):
        return [object()] * FakeApi.commits

    def super_squash_history(self, repo_id, repo_type=None, commit_message=None):
        FakeApi.squashed.append((repo_id, repo_type))


@pytest.fixture
def api(monkeypatch):
    monkeypatch.setattr(huggingface_hub, "HfApi", FakeApi)
    # setenv before delenv makes teardown restore the original state, which cli.main changes with os.environ.setdefault.
    monkeypatch.setenv("HF_OIDC_RESOURCE", "")
    monkeypatch.delenv("HF_OIDC_RESOURCE")
    monkeypatch.delenv("GITHUB_ACTIONS", raising=False)
    FakeApi.commits, FakeApi.squashed = 0, []
    return FakeApi


@pytest.mark.parametrize("commits, squashed", [(SQUASH_AFTER_COMMITS, False), (SQUASH_AFTER_COMMITS + 1, True)])
def test_squash_only_past_threshold(api, commits, squashed):
    api.commits = commits
    assert cli.main(["squash"]) == 0
    assert api.squashed == ([(cli.DEFAULT_REPO, "dataset")] if squashed else [])


def test_oidc_resource_follows_repo_in_github_actions(api, monkeypatch):
    monkeypatch.setenv("GITHUB_ACTIONS", "true")
    cli.main(["squash", "--repo", "someone/other"])
    assert os.environ["HF_OIDC_RESOURCE"] == "datasets/someone/other"


def test_oidc_resource_untouched_outside_actions_local_or_preset(api, monkeypatch, tmp_path):
    cli.main(["squash"])
    assert "HF_OIDC_RESOURCE" not in os.environ

    monkeypatch.setenv("GITHUB_ACTIONS", "true")
    cli.main(["card", "--local", str(tmp_path)])
    assert "HF_OIDC_RESOURCE" not in os.environ

    monkeypatch.setenv("HF_OIDC_RESOURCE", "")
    cli.main(["squash"])
    assert os.environ["HF_OIDC_RESOURCE"] == ""


def test_workflow_lane_matrix_equals_registry():
    matrix = yaml.safe_load(WORKFLOW.read_text())["jobs"]["lane"]["strategy"]["matrix"]["lane"]
    assert json.loads(re.search(r"'(\[.*\])'", matrix).group(1)) == LANES
