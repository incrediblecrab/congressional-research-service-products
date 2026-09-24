"""Shared fakes: a scripted CRS source for the sync loop, and a fetcher that serves fixtures by URL."""

import json
import math
from pathlib import Path
from types import SimpleNamespace

import httpx

from crs_products.card import render
from crs_products.pipeline import Context, sync
from crs_products.store import LocalStore

FIXTURES = Path(__file__).parent / "fixtures"


def fixture_bytes(name):
    return (FIXTURES / name).read_bytes()


def fixture_json(name):
    return json.loads(fixture_bytes(name))


class FakeFetcher:
    """json() answers from a URL map: a dict, None (a 404), an exception to raise, or a callable of the params. get() answers bytes as 200, an int as that status, or raises an exception."""

    def __init__(self, json_map=None, get_map=None):
        self.json_map = json_map or {}
        self.get_map = get_map or {}
        self.requests = []

    def json(self, url, params=None):
        self.requests.append(url)
        value = self.json_map.get(url)
        if isinstance(value, Exception):
            raise value
        return value(params or {}) if callable(value) else value

    def get(self, url, params=None, headers=None):
        self.requests.append(url)
        value = self.get_map.get(url, 404)
        if isinstance(value, Exception):
            raise value
        request = httpx.Request("GET", url)
        if isinstance(value, int):
            return httpx.Response(value, request=request)
        return httpx.Response(200, content=value, request=request)


def scripted(**overrides):
    """State for ScriptedSource. units: {id: updateDate} the API lists. fail: ids whose fetch fails. fatal: {id: exception} raised as is. exists: ids the API still serves though the listing lacks them. texts: {id: text or None}. stop_after: use up the budget after that many fetches."""
    return SimpleNamespace(**{"units": {}, "fail": set(), "fatal": {}, "exists": set(), "texts": {}, "stop_after": None, "count": None,
                              "fetched": [], "exists_asked": [], "ctx": None, **overrides})


class ScriptedSource:
    def __init__(self, state):
        self.state = state

    def list_all(self):
        units = self.state.units
        newest = max(units.items(), key=lambda item: (item[1], item[0])) if units else None
        head = {"count": len(units) if self.state.count is None else self.state.count, "newest": f"{newest[0]}@{newest[1]}" if newest else None}
        return head, {uid: {"id": uid, "updateDate": stamp} for uid, stamp in units.items()}

    def head(self):
        return self.list_all()[0]

    def exists(self, uid):
        self.state.exists_asked.append(uid)
        return uid in self.state.exists

    def fetch(self, unit):
        self.state.fetched.append(unit.id)
        if self.state.stop_after and len(self.state.fetched) >= self.state.stop_after:
            self.state.ctx.deadline = 0
        if unit.id in self.state.fatal:
            raise self.state.fatal[unit.id]
        if unit.id in self.state.fail:
            raise RuntimeError(f"planted failure for {unit.id}")
        text = self.state.texts.get(unit.id, f"text of {unit.id}")
        return {"id": unit.id, "title": f"{unit.id} as of {unit.updated_at}", "status": "Active", "version": 1, "authors": ["A. Author"], "topics": [],
                "text": text, "text_source": "pdf" if text else None, "url": f"https://www.congress.gov/crs-report/{unit.id}"}


def local_store(tmp_path):
    return LocalStore(tmp_path / "hub", workdir=tmp_path, card=render)


def run_once(store, state, **context):
    context.setdefault("writer", "local")
    ctx = Context(store=store, deadline=math.inf, **context)
    state.ctx = ctx
    return sync(ctx, ScriptedSource(state))
