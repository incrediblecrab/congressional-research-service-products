"""The CLI's contract with the workflow: exit codes, $GITHUB_OUTPUT keys, Trusted Publishing, and a workflow that calls only commands, options and outputs the CLI has."""

import os
import re
import subprocess
from pathlib import Path

import httpx
import huggingface_hub
import pytest
import yaml
from huggingface_hub.errors import HfHubHTTPError

from crs_products import cli
from crs_products.http import Unavailable

WORKFLOW = Path(__file__).parent.parent / ".github" / "workflows" / "pipeline.yml"
# What each command writes to $GITHUB_OUTPUT. The probe and run tests check the commands against this, and the workflow test checks the workflow's if: expressions against it.
OUTPUTS = {"probe": {"needed"}, "run": {"commits"}}


@pytest.fixture
def actions(tmp_path, monkeypatch):
    """A GitHub Actions environment with $GITHUB_OUTPUT as a file. HF_OIDC_RESOURCE starts unset and is restored afterwards, since the CLI sets it in os.environ."""
    output = tmp_path / "github_output"
    output.touch()
    monkeypatch.setenv("GITHUB_ACTIONS", "true")
    monkeypatch.setenv("GITHUB_OUTPUT", str(output))
    monkeypatch.setenv("HF_OIDC_RESOURCE", "placeholder")
    monkeypatch.delenv("HF_OIDC_RESOURCE")
    monkeypatch.setattr(cli.shutil, "which", lambda name: f"/usr/bin/{name}")
    return output


def outputs(path):
    return dict(line.split("=", 1) for line in path.read_text().splitlines())


def hub_error(status, message):
    return HfHubHTTPError(message, response=httpx.Response(status, request=httpx.Request("POST", "https://huggingface.co/oauth/token")))


def raising(error):
    def fail(*args, **kwargs):
        raise error
    return fail


def test_trusted_publishing_is_requested_only_in_actions_and_only_for_the_hub(actions, monkeypatch, tmp_path):
    seen = []
    monkeypatch.setattr(cli, "cmd_probe", lambda args: seen.append(os.environ.get("HF_OIDC_RESOURCE")) or 0)
    assert cli.main(["probe", "--repo", "someone/some-dataset"]) == 0
    monkeypatch.delenv("HF_OIDC_RESOURCE")
    cli.main(["probe", "--local", str(tmp_path)])
    monkeypatch.delenv("GITHUB_ACTIONS")
    cli.main(["probe", "--repo", "someone/some-dataset"])
    assert seen == ["datasets/someone/some-dataset", None, None]


def test_run_without_a_trusted_publisher_is_neutral(actions, monkeypatch):
    monkeypatch.setattr(cli, "open_store", raising(hub_error(400, f"400 Client Error: Bad Request for url: https://huggingface.co/oauth/token ({cli.NO_PUBLISHER} for datasets/x/y)")))
    assert cli.main(["run"]) == 0
    assert outputs(actions) == {"commits": "0"}


def test_run_with_any_other_hub_refusal_fails(actions, monkeypatch):
    monkeypatch.setattr(cli, "open_store", raising(hub_error(401, "401 Client Error: Unauthorized")))
    with pytest.raises(HfHubHTTPError):
        cli.main(["run"])


def test_run_refuses_to_start_without_pdftotext(monkeypatch, tmp_path):
    monkeypatch.setattr(cli.shutil, "which", lambda name: None)
    with pytest.raises(SystemExit, match="pdftotext is missing"):
        cli.main(["run", "--local", str(tmp_path)])


@pytest.mark.parametrize("stopped, code", [(None, 0), ("budget", 0), ("deferred", 0), ("superseded", 0), ("Blocked: bot challenge at www.congress.gov/x.pdf", 1), ("RuntimeError: boom", 1)])
def test_run_exit_codes(actions, monkeypatch, tmp_path, stopped, code):
    monkeypatch.setattr(cli, "sync", lambda ctx, source: {"stopped": stopped, "finished": stopped is None, "commits": 3})
    assert cli.main(["run", "--local", str(tmp_path / "hub")]) == code
    assert outputs(actions) == {"commits": "3"} and set(outputs(actions)) == OUTPUTS["run"]


class FakeSource:
    """Stands in for CrsSource(fetcher): head() answers the given head or raises the given error."""

    def __init__(self, head=None, error=None):
        self.value, self.error = head, error

    def __call__(self, fetcher):
        return self

    def head(self):
        if self.error:
            raise self.error
        return self.value


def test_probe_asks_for_a_sync_when_the_hub_has_no_manifest(actions, monkeypatch, tmp_path, capsys):
    monkeypatch.setattr(cli, "CrsSource", FakeSource(head={"count": 5, "newest": "R40001@x"}))
    assert cli.main(["probe", "--local", str(tmp_path)]) == 0
    assert outputs(actions) == {"needed": "true"} and set(outputs(actions)) == OUTPUTS["probe"]
    assert '"reason": "no manifest yet"' in capsys.readouterr().out


@pytest.mark.parametrize("error", [Unavailable("HTTP 503 from api.congress.gov/v3/crsreport"), hub_error(503, "503 Server Error"), hub_error(429, "429 Too Many Requests"), httpx.ConnectError("refused")])
def test_a_transient_outage_skips_the_probe_quietly(actions, monkeypatch, tmp_path, error):
    monkeypatch.setattr(cli, "CrsSource", FakeSource(error=error))
    assert cli.main(["probe", "--local", str(tmp_path)]) == 0
    assert outputs(actions) == {"needed": "false"}


@pytest.mark.parametrize("error", [RuntimeError("a bug"), hub_error(404, "404 Client Error: Repository Not Found")])
def test_any_other_probe_error_fails_the_job(actions, monkeypatch, tmp_path, error):
    monkeypatch.setattr(cli, "CrsSource", FakeSource(error=error))
    with pytest.raises(type(error)):
        cli.main(["probe", "--local", str(tmp_path)])
    assert outputs(actions) == {}


@pytest.mark.parametrize("commits, squashed", [(cli.SQUASH_AFTER_COMMITS, False), (cli.SQUASH_AFTER_COMMITS + 1, True)])
def test_squash_only_past_the_threshold(monkeypatch, commits, squashed):
    calls = []

    class FakeHfApi:
        def __init__(self, token=None):
            pass

        def list_repo_commits(self, repo_id, repo_type):
            return [None] * commits

        def super_squash_history(self, repo_id, repo_type, commit_message):
            calls.append((repo_id, commit_message))

    monkeypatch.setattr(huggingface_hub, "HfApi", FakeHfApi)
    assert cli.main(["squash", "--repo", "x/y"]) == 0
    assert bool(calls) == squashed


def shell_argv(line, env):
    """The argv bash builds for one `python -m crs_products ...` line of a workflow step, with the step's env."""
    command = line.split("|")[0].strip().replace("python -m crs_products", "printf '%s\\0'", 1)
    done = subprocess.run(["bash", "-c", command], capture_output=True, text=True, env={"PATH": os.environ["PATH"], **env}, check=True)
    return done.stdout.split("\0")[:-1]


def test_the_workflow_calls_only_commands_options_and_outputs_the_cli_has(monkeypatch):
    workflow = yaml.safe_load(WORKFLOW.read_text())
    triggers = workflow.get("on", workflow.get(True))  # PyYAML reads the key `on` as True
    crons = [entry["cron"] for entry in triggers["schedule"]]
    jobs = workflow["jobs"]
    assert jobs["keepalive"]["if"] == f"github.event.schedule == '{crons[1]}'"

    example = re.search(r"e\.g\. (.+?) \(", triggers["workflow_dispatch"]["inputs"]["args"]["description"]).group(1)
    parsed = []
    for name in ("run", "probe", "verify", "squash"):
        monkeypatch.setattr(cli, f"cmd_{name}", lambda args: parsed.append(args) or 0)
    command_of = {}
    for step in jobs["sync"]["steps"]:
        for line in (step.get("run") or "").splitlines():
            if "python -m crs_products" not in line:
                continue
            envs = [{}]
            if "$EXTRA_ARGS" in line or "$BUDGET" in line:
                envs = [{}, {"BUDGET": "30", "EXTRA_ARGS": example}]
            for env in envs:
                argv = shell_argv(line, env)
                assert cli.main(argv) == 0, f"{step.get('name')}: {argv}"
                command_of[step.get("id")] = argv[0]
    assert {args.command for args in parsed} == {"run", "probe", "verify", "squash"}
    smoke = next(args for args in parsed if args.command == "run" and args.local)
    assert smoke.budget_minutes == 30 and smoke.partitions and smoke.max_units

    expressions = " ".join(str(step.get("if", "")) for step in jobs["sync"]["steps"])
    referenced = re.findall(r"steps\.(\w+)\.outputs\.(\w+)", expressions)
    assert referenced
    for step_id, key in referenced:
        assert key in OUTPUTS[command_of[step_id]], f"steps.{step_id}.outputs.{key}: `{command_of[step_id]}` does not write {key}"
