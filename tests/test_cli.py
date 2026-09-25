"""The CLI's contract with the workflow: exit codes, $GITHUB_OUTPUT keys, Trusted Publishing, and a workflow that calls only commands, options and outputs the CLI has. Also the workflow's keepalive job."""

import json
import os
import re
import subprocess
import time
from pathlib import Path

import httpx
import huggingface_hub
import pytest
import yaml
from huggingface_hub.errors import HfHubHTTPError

from crs_products import cli
from crs_products.http import Unavailable
from crs_products.pipeline import Context
from crs_products.store import CARD, LocalStore
from conftest import ScriptedSource, local_store, run_once, scripted

WORKFLOW = Path(__file__).parent.parent / ".github" / "workflows" / "pipeline.yml"
# What each command writes to $GITHUB_OUTPUT. The probe and run tests check the commands against this, and the workflow test checks the workflow's if: expressions against it.
OUTPUTS = {"probe": {"needed"}, "run": {"commits", "more"}}


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
    assert outputs(actions) == {"commits": "0", "more": "false"}


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
    monkeypatch.setattr(cli, "sync", lambda ctx, source: {"stopped": stopped, "finished": stopped is None, "commits": 3, "fetched": 1})
    assert cli.main(["run", "--local", str(tmp_path / "hub")]) == code
    assert outputs(actions)["commits"] == "3" and set(outputs(actions)) == OUTPUTS["run"]


# sync() leaves fetched out of a deferred run's record.
@pytest.mark.parametrize("stopped, fetched, more", [("budget", 4, "true"), ("budget", 0, "false"), (None, 4, "false"), ("superseded", 4, "false"), ("RuntimeError: boom", 4, "false"), ("deferred", None, "false")])
def test_only_a_run_that_ran_out_of_budget_while_fetching_asks_for_the_next_run(actions, monkeypatch, tmp_path, stopped, fetched, more):
    run = {"stopped": stopped, "finished": stopped is None, "commits": 1} | ({} if fetched is None else {"fetched": fetched})
    monkeypatch.setattr(cli, "sync", lambda ctx, source: run)
    cli.main(["run", "--local", str(tmp_path / "hub")])
    assert outputs(actions)["more"] == more


def test_more_follows_the_record_the_real_sync_returns(actions, monkeypatch, tmp_path):
    state = scripted(units={f"R40{n:03d}": "2026-09-01T10:00:00Z" for n in range(5)}, stop_after=2)
    monkeypatch.setattr(cli, "CrsSource", lambda fetcher: ScriptedSource(state))
    monkeypatch.setattr(cli, "Context", lambda **kwargs: setattr(state, "ctx", Context(**kwargs)) or state.ctx)
    argv = ["run", "--local", str(tmp_path / "hub"), "--workdir", str(tmp_path)]
    assert cli.main(argv) == 0 and outputs(actions)["more"] == "true"
    state.stop_after = None
    actions.write_text("")
    assert cli.main(argv) == 0 and outputs(actions)["more"] == "false" and len(state.fetched) == 5


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


def test_probe_decides_from_the_card_without_reading_the_manifest(actions, monkeypatch, tmp_path, capsys):
    state = scripted(units={"R40001": "2026-09-01T10:00:00Z", "IN12001": "2026-09-02T10:00:00Z"})
    run_once(local_store(tmp_path), state, writer="github-actions")
    monkeypatch.setattr(cli, "CrsSource", FakeSource(head=ScriptedSource(state).head()))
    monkeypatch.setattr(LocalStore, "read_manifest", lambda self: pytest.fail("planted: the probe read the manifest"))
    assert cli.main(["probe", "--local", str(tmp_path / "hub")]) == 0
    out = json.loads(capsys.readouterr().out)
    assert (out["needed"], out["reason"], out["state_from"]) == (False, "up to date", "card") and outputs(actions) == {"needed": "false"}


@pytest.mark.parametrize("card", ["---\nlicense: other\n---\n# a card from before the probe state\n", None])
def test_probe_falls_back_to_the_manifest_when_the_card_has_no_state(actions, monkeypatch, tmp_path, capsys, card):
    state = scripted(units={"R40001": "2026-09-01T10:00:00Z"})
    run_once(local_store(tmp_path), state, writer="github-actions")
    readme = tmp_path / "hub" / CARD
    readme.write_text(card) if card else readme.unlink()
    monkeypatch.setattr(cli, "CrsSource", FakeSource(head=dict(ScriptedSource(state).head(), count=2)))
    assert cli.main(["probe", "--local", str(tmp_path / "hub")]) == 0
    out = json.loads(capsys.readouterr().out)
    assert (out["needed"], out["reason"], out["state_from"]) == (True, "count 1 -> 2", "manifest")


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
    jobs = workflow["jobs"]

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

    expressions = " ".join([str(step.get("if", "")) for step in jobs["sync"]["steps"]] + list(jobs["sync"].get("outputs", {}).values()))
    referenced = re.findall(r"steps\.(\w+)\.outputs\.(\w+)", expressions)
    assert referenced
    for step_id, key in referenced:
        assert key in OUTPUTS[command_of[step_id]], f"steps.{step_id}.outputs.{key}: `{command_of[step_id]}` does not write {key}"

    needed = [(name, key) for job in jobs.values() for name, key in re.findall(r"needs\.(\w+)\.outputs\.(\w+)", str(job.get("if", "")))]
    assert needed
    for name, key in needed:
        assert key in jobs[name].get("outputs", {}), f"needs.{name}.outputs.{key}: job {name} declares no output {key}"
    # A job that starts runs starts this workflow, never after a bounded test, and holds no permission to touch the dataset; the job that parses downloads cannot start runs.
    starters = [job for job in jobs.values() if any("gh workflow run" in (step.get("run") or "") for step in job["steps"])]
    assert starters
    for job in starters:
        assert all(f"gh workflow run {WORKFLOW.name} " in step["run"] for step in job["steps"] if "gh workflow run" in (step.get("run") or ""))
        assert "!inputs.args" in job["if"] and job["permissions"] == {"actions": "write"}
    assert "actions" not in jobs["sync"]["permissions"]


@pytest.mark.parametrize("idle_days, pushed", [(44, False), (46, True)])
def test_keepalive_runs_on_every_schedule_and_commits_after_45_idle_days(tmp_path, idle_days, pushed):
    """The keepalive step run the way GitHub runs a step (bash -e), in a checkout made the way actions/checkout makes one, against a local origin."""
    job = yaml.safe_load(WORKFLOW.read_text())["jobs"]["keepalive"]
    assert job["if"] == "github.event_name == 'schedule'"
    (step,) = [step for step in job["steps"] if "run" in step]
    env = {"PATH": os.environ["PATH"], "HOME": str(tmp_path), "GIT_CONFIG_NOSYSTEM": "1", "GIT_CONFIG_GLOBAL": os.devnull}

    def git(*args, cwd, **extra):
        return subprocess.run(["git", *args], cwd=cwd, env={**env, **extra}, capture_output=True, text=True, check=True).stdout

    origin, seed, work = tmp_path / "origin.git", tmp_path / "seed", tmp_path / "work"
    for path in (origin, seed, work):
        path.mkdir()
    git("init", "-q", "--bare", "-b", "main", cwd=origin)
    git("init", "-q", "-b", "main", cwd=seed)
    stamp = f"@{int(time.time()) - idle_days * 86400} +0000"
    who = {"GIT_AUTHOR_NAME": "a", "GIT_AUTHOR_EMAIL": "a@example.com", "GIT_COMMITTER_NAME": "a", "GIT_COMMITTER_EMAIL": "a@example.com"}
    for title in ("first change", "last change"):
        git("commit", "-q", "--allow-empty", "-m", title, cwd=seed, GIT_AUTHOR_DATE=stamp, GIT_COMMITTER_DATE=stamp, **who)
    git("push", "-q", origin.as_uri(), "main", cwd=seed)
    # actions/checkout on a scheduled run: a shallow fetch, then a local main that tracks origin's.
    git("init", "-q", cwd=work)
    git("remote", "add", "origin", origin.as_uri(), cwd=work)
    git("fetch", "-q", "--depth=1", "origin", "+refs/heads/main:refs/remotes/origin/main", cwd=work)
    git("checkout", "-q", "--force", "-B", "main", "refs/remotes/origin/main", cwd=work)
    assert git("rev-parse", "--is-shallow-repository", cwd=work).strip() == "true"

    done = subprocess.run(["bash", "-e", "-c", step["run"]], cwd=work, env={**env, "SCHEDULE": "2/5 * * * *"}, capture_output=True, text=True)
    assert done.returncode == 0, done.stderr
    assert "schedule: 2/5 * * * *" in done.stdout
    titles = git("log", "--format=%s", "main", cwd=origin).splitlines()
    assert titles == (["Keep the schedule enabled: no commit in 45 days"] if pushed else []) + ["last change", "first change"]
    assert pushed or f"last commit {idle_days} days ago" in done.stdout
