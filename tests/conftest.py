"""Shared fakes: a scripted source adapter for the sync loop, and a fetcher that serves fixtures by URL."""

import json
import math
from pathlib import Path
from types import SimpleNamespace

import httpx

from ijab.collections import Collection
from ijab.pipeline import Adapter, Context, Partition, Unit, sync_collection

FIXTURES = Path(__file__).parent / "fixtures"


def fixture_bytes(name):
    return (FIXTURES / name).read_bytes()


def fixture_json(name):
    return json.loads(fixture_bytes(name))


class FakeFetcher:
    """json() answers from a URL map ({} otherwise); get() answers bytes as 200, an int as that status, or raises an exception."""

    def __init__(self, json_map=None, get_map=None):
        self.json_map = json_map or {}
        self.get_map = get_map or {}
        self.requests = []

    def json(self, url, params=None):
        self.requests.append(url)
        return self.json_map.get(url, {})

    def get(self, url, params=None, headers=None, stream_to=None):
        self.requests.append(url)
        value = self.get_map.get(url, 404)
        if isinstance(value, Exception):
            raise value
        request = httpx.Request("GET", url)
        if isinstance(value, int):
            return httpx.Response(value, request=request)
        return httpx.Response(200, content=value, request=request)


class ScriptedAdapter(Adapter):
    """Lists state.units ({id: updated_at}) as partition "p". fetch() fails for ids in state.fail and uses up the time budget once state.stop_after units have been fetched."""

    state = None

    def plan(self, manifest):
        units = {uid: Unit(uid, stamp) for uid, stamp in self.state.units.items()}
        yield Partition("p", units, complete_listing=self.state.complete_listing)

    def fetch(self, unit):
        self.state.fetched.append(unit.id)
        if self.state.stop_after and len(self.state.fetched) >= self.state.stop_after:
            self.ctx.deadline = 0
        if unit.id in self.state.fail:
            raise RuntimeError(f"planted failure for {unit.id}")
        return [{"id": unit.id, "title": f"{unit.id} as of {unit.updated_at}", "text": f"text of {unit.id}", "url": f"https://example.test/{unit.id}"}]

    def live_counts(self, keys):
        return dict(self.state.live)


def scripted_collection(name="scripted", **overrides):
    state = SimpleNamespace(**{"units": {}, "fail": set(), "complete_listing": True, "stop_after": None, "live": {}, "fetched": [], **overrides})
    adapter = type("Scripted", (ScriptedAdapter,), {"state": state})
    return Collection(name, "test", adapter, "scripted source", "scripted text"), state


def run_once(store, collection, **context):
    """One lane run for one collection: sync, then commit the staged run record, as cli.cmd_run does."""
    ctx = Context(fetcher=None, store=store, deadline=math.inf, **context)
    result = sync_collection(ctx, collection)
    store.commit("run records")
    return result
