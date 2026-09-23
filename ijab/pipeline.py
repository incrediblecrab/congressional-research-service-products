"""The sync loop shared by every collection.

A source adapter lists a partition as units (a file, a package, an API item) with the source's own last-modified value. The loop loads the partition already on the Hub, fetches only units that are new or whose last-modified value changed, drops units the source no longer lists, and commits the partition with its manifest. The Parquet file is the state: nothing else records which units were fetched, so a run that dies loses at most one checkpoint interval.
"""

import hashlib
import json
import logging
import time
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timezone

from .http import Blocked, MissingKey, QuotaExhausted

log = logging.getLogger("ijab")

MAX_ATTEMPTS = 3
RUNS_KEPT = 20
# A listing that would delete more than this share of a stored partition is treated as an upstream glitch, not a deletion.
MAX_REMOVED_SHARE = 0.5
MIN_REMOVED_GUARD = 10
MANIFEST_VERSION = 1
FATAL = (Blocked, QuotaExhausted, MissingKey)


def utcnow():
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


@dataclass
class Unit:
    id: str
    updated_at: str | None
    info: dict = field(default_factory=dict)


@dataclass
class Partition:
    key: str
    units: dict
    complete_listing: bool = True

    @property
    def fingerprint(self):
        if not self.complete_listing:
            return None
        digest = hashlib.sha256()
        for uid in sorted(self.units):
            digest.update(f"{uid}\t{self.units[uid].updated_at}\n".encode())
        return digest.hexdigest()


class Adapter:
    """Base for source adapters. Subclasses implement plan() and fetch()."""

    def __init__(self, ctx, collection):
        self.ctx = ctx
        self.collection = collection

    def plan(self, manifest):
        raise NotImplementedError

    def fetch(self, unit):
        raise NotImplementedError

    def fetch_many(self, units):
        for unit in units:
            try:
                yield unit, self.fetch(unit)
            except FATAL:
                raise
            except Exception as error:  # noqa: BLE001 - recorded per unit, retried on later runs
                yield unit, error

    def unit_of(self, row):
        """The unit a stored row came from. Most units produce one row with the unit's id."""
        return row["id"]

    def wanted(self, key):
        """False for partitions a bounded run (Context.only) skips, so plans need not list them."""
        return self.ctx.only is None or str(key) in self.ctx.only


@dataclass
class Context:
    """deadline is a time.monotonic() value; the lane runner sets it to each collection's share of the run.

    only (partition keys) and max_units (units fetched per partition) bound a run for smoke tests; a capped partition is written as incomplete and resumes on the next run.

    refetch fetches every unit of the partitions the run reaches again, as after a parser change; a partition that does not finish starts over on the next refetch run.
    """

    fetcher: object
    store: object
    deadline: float
    checkpoint_seconds: float = 2400.0
    only: frozenset | None = None
    max_units: int | None = None
    refetch: bool = False
    stats: dict = field(default_factory=lambda: defaultdict(int))

    def out_of_time(self):
        return time.monotonic() > self.deadline


def new_manifest(collection):
    return {"version": MANIFEST_VERSION, "collection": collection.name, "lane": collection.lane, "source": collection.source,
            "partitions": {}, "failures": {}, "watermark": None, "updated_at": None, "runs": []}


def sync_collection(ctx, collection):
    """Returns the run record; run["finished"] is True when every planned partition was finished (nothing left for this run).

    The run record is staged, not committed: the lane commits all of its run records at once, and only when something changed or the last record is a day old, so an idle dataset does not accumulate a commit per collection per run.
    """
    adapter = collection.adapter(ctx, collection)
    manifest = ctx.store.read_manifest(collection.name) or new_manifest(collection)
    started, before = utcnow(), dict(ctx.stats)
    finished, reason, fatal = True, None, False
    try:
        for partition in adapter.plan(manifest):
            if ctx.only is not None and partition.key not in ctx.only:
                continue
            if ctx.out_of_time() or not sync_partition(ctx, collection, adapter, manifest, partition):
                finished, reason = False, "budget"
                break
    except FATAL as error:
        finished, reason = False, f"{type(error).__name__}: {error}"
        fatal = True
        log.warning("%s stopped: %s", collection.name, reason)
    except Exception as error:  # noqa: BLE001 - a listing failure stops this collection, not the lane
        finished, reason = False, f"{type(error).__name__}: {error}"[:300]
        log.exception("%s failed", collection.name)
    run = {"started": started, "ended": utcnow(), "finished": finished, "stopped": reason}
    run.update({key: ctx.stats[key] - before.get(key, 0) for key in ("fetched", "failed", "removed", "commits", "suspect_listings")})
    runs = manifest.get("runs") or []
    stale = not runs or age_hours(runs[-1]["ended"]) > 23
    manifest["runs"] = runs[-(RUNS_KEPT - 1):] + [run]
    if run["commits"] or stale or reason not in (None, "budget"):
        ctx.store.stage_manifest(collection.name, manifest)
    return dict(run, collection=collection.name, fatal=fatal)


def age_hours(stamp):
    """Hours since a utcnow() stamp; infinite for a missing one."""
    if not stamp:
        return float("inf")
    then = datetime.strptime(stamp, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
    return (datetime.now(timezone.utc) - then).total_seconds() / 3600


def sync_partition(ctx, collection, adapter, manifest, partition):
    """Bring one partition up to date. Returns False if the budget ran out before it was finished."""
    entry = manifest["partitions"].get(partition.key) or {}
    failures = manifest.setdefault("failures", {})
    fingerprint = partition.fingerprint
    if fingerprint and entry.get("complete") and entry.get("fingerprint") == fingerprint and not ctx.refetch:
        entry["listed_at"] = utcnow()
        return True

    by_unit = defaultdict(list)
    for row in (ctx.store.read_partition(collection.name, partition.key) if entry else []):
        by_unit[adapter.unit_of(row)].append(row)
    removed = [uid for uid in by_unit if partition.complete_listing and uid not in partition.units]
    if len(removed) > max(MIN_REMOVED_GUARD, MAX_REMOVED_SHARE * len(by_unit)):
        log.warning("%s/%s: listing drops %d of %d stored units; skipped as a suspect listing", collection.name, partition.key, len(removed), len(by_unit))
        ctx.stats["suspect_listings"] += 1
        return True
    for uid in removed:
        del by_unit[uid]
        failures.pop(uid, None)

    todo = []
    for uid in sorted(partition.units):
        unit = partition.units[uid]
        if uid in by_unit and by_unit[uid][0]["updated_at"] == unit.updated_at and not ctx.refetch:
            continue
        failure = failures.get(uid)
        if failure and failure["attempts"] >= MAX_ATTEMPTS and failure.get("updated_at") == unit.updated_at and not ctx.refetch:
            continue
        todo.append(unit)
    if not todo and not removed and entry and fingerprint is None:
        return True

    fetched = failed = 0
    done_ids = set()
    finished = True
    checkpoint_at = time.monotonic() + ctx.checkpoint_seconds
    # The cap limits what is fetched, not todo: units left unfetched keep the partition incomplete.
    stream = adapter.fetch_many(todo if ctx.max_units is None else todo[:ctx.max_units])
    try:
        for unit, result in stream:
            done_ids.add(unit.id)
            if isinstance(result, Exception):
                previous = failures.get(unit.id) or {}
                attempts = previous.get("attempts", 0) + 1 if previous.get("updated_at") == unit.updated_at else 1
                failures[unit.id] = {"partition": partition.key, "updated_at": unit.updated_at, "attempts": attempts,
                                     "error": f"{type(result).__name__}: {result}"[:300], "at": utcnow()}
                failed += 1
                log.info("%s %s failed (%d): %s", collection.name, unit.id, attempts, result)
            else:
                stamp = utcnow()
                for row in result:
                    row.setdefault("collection", collection.name)
                    row["updated_at"] = unit.updated_at
                    row.setdefault("fetched_at", stamp)
                by_unit[unit.id] = result
                failures.pop(unit.id, None)
                fetched += 1
            if ctx.out_of_time():
                finished = False
                break
            if time.monotonic() > checkpoint_at:
                _write(ctx, collection, manifest, partition, by_unit, todo, done_ids, fetched, failed, removed, final=False)
                fetched = failed = 0
                removed = []
                checkpoint_at = time.monotonic() + ctx.checkpoint_seconds
    finally:
        stream.close()
    _write(ctx, collection, manifest, partition, by_unit, todo, done_ids, fetched, failed, removed, final=finished)
    return finished


def _write(ctx, collection, manifest, partition, by_unit, todo, done_ids, fetched, failed, removed, final):
    failures = manifest["failures"]
    open_units = [unit for unit in todo if unit.id not in done_ids or (unit.id in failures and failures[unit.id]["attempts"] < MAX_ATTEMPTS)]
    complete = final and not open_units
    exhausted = sum(1 for failure in failures.values() if failure.get("partition") == partition.key and failure["attempts"] >= MAX_ATTEMPTS)
    rows = [row for rows in by_unit.values() for row in rows]
    stats = ctx.store.stage_partition(collection.name, partition.key, rows)
    entry = manifest["partitions"].get(partition.key) or {}
    listed = len(partition.units) if partition.complete_listing else None
    entry.update(stats)
    entry.update({
        "units": len(by_unit),
        "failed_units": exhausted,
        "source_count": listed if listed is not None else entry.get("source_count"),
        "complete": complete if partition.complete_listing else bool(entry.get("complete")) and complete,
        "fingerprint": partition.fingerprint if complete else None,
        "updated_at": utcnow(),
    })
    if partition.complete_listing:
        entry["listed_at"] = entry["updated_at"]
    elif entry.get("source_count") is not None:
        entry["source_count"] = max(entry["source_count"], len(by_unit) + exhausted)
    manifest["partitions"][partition.key] = entry
    manifest["updated_at"] = entry["updated_at"]
    ctx.store.stage_manifest(collection.name, manifest)
    state = "complete" if complete else "partial"
    message = f"{collection.name}/{partition.key}: {fetched} fetched, {failed} failed, {len(removed)} removed, {entry['rows']} rows ({state})"
    if ctx.store.commit(message):
        ctx.stats["commits"] += 1
    ctx.stats["fetched"] += fetched
    ctx.stats["failed"] += failed
    ctx.stats["removed"] += len(removed)
    log.info(message)


def dumps(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True)
