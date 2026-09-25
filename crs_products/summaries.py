"""CRS summaries of bills and resolutions, from the Congress.gov API's /summaries listing.

CRS writes a summary of each bill and resolution when it is introduced, and a new one at later major actions; versionCode names the action. The listing's items carry the whole summary (its text as HTML), so rows come from listing pages and nothing is fetched per summary. A row's id is congress-type-number-versionCode, for example 119-hr-8893-00; a partition is one congress and bill type, like 119-hr.

The listing pages by offset over items ordered by updateDate, and items that share an updateDate come back in a different order from one page to the next, so offset paging skips some and repeats others: one pass over /summaries/100/hr read 6,644 distinct summaries of 6,765 (measured September 25, 2026). collect() does not page by offset. It reads the first page of a window of updateDates, which holds every item newer than the page's oldest second, and moves the window's end down to that second; only a second that holds a whole page is paged by offset, in several orders, until the distinct items reach its count.

A run first reads what changed since the last run (a window of updateDates on /summaries), then reads every slice not read yet (the backfill), then once a day compares each slice's count with the rows stored, and reads again the slices that differ and the ones read longest ago. Rows are written with the products' store and lease (pipeline.Context, flush); the manifest has the products' shape, so the probe and the card work the same way.
"""

import copy
import json
import logging
import math
import time
from datetime import datetime, timedelta, timezone

import pyarrow as pa

from .pipeline import CLEAN_STOPS, FATAL, MANIFEST_VERSION, MAX_REMOVED_SHARE, MIN_REMOVED_GUARD, RUNS_KEPT, STAMP, age_hours, flush, new_manifest, other_writer, utcnow
from .store import Superseded, write_parquet
from .text import html_text, tidy, xml_safe

log = logging.getLogger("crs_products")

API = "https://api.congress.gov/v3"
SOURCE_URL = f"{API}/summaries"
REPO_ID = "incrediblecrab/congressional-research-service-bill-summaries"
PAGE = 250
# The listing starts with the 93rd Congress (1973-1974).
FIRST_CONGRESS = 93
TYPES = ("hr", "s", "hjres", "sjres", "hconres", "sconres", "hres", "sres")
# Every request names a start: without fromDateTime the listing answers a recent window only (3 of 3,304 summaries for /summaries/119/hr, measured September 25, 2026).
EPOCH = "1900-01-01T00:00:00Z"
# Each run reads again what changed this long before the last run's newest updateDate.
OVERLAP_HOURS = 48
# How often .github/workflows/summaries.yml runs (its cron says the same; a test checks).
SCHEDULE_HOURS = 6
# The daily check runs on the first run this long after the last one: under 24 hours and over 24 - SCHEDULE_HOURS, so it keeps to one slot of the schedule instead of slipping to the next whenever GitHub starts a run a little earlier than the day before.
RECONCILE_HOURS = 20
# Each daily reconciliation also reads again the slices read longest ago, so every slice is read in full at least this often.
RESYNC_DAYS = 30
# (sort, limit, first offset) orders for one second's items. Inside one second of /summaries/100/hr (1,391 summaries at 2015-10-01T15:15:02Z) the first four together read all 1,391 in 23 requests, where any one alone read 1,100 to 1,380 (measured September 25, 2026).
TIE_PLANS = ((None, PAGE, 0), ("updateDate asc", PAGE, 0), ("updateDate desc", PAGE, 0), (None, PAGE, PAGE // 2), (None, 199, 0), ("updateDate asc", PAGE, PAGE // 2), (None, 173, 0), (None, 157, 0), (None, 131, 0), (None, 113, 0))

SCHEMA = pa.schema([
    ("id", pa.string()),
    ("congress", pa.int32()),
    ("bill_type", pa.string()),
    ("bill_number", pa.int32()),
    ("version_code", pa.string()),
    ("action_date", pa.string()),
    ("action_desc", pa.string()),
    ("title", pa.string()),
    ("origin_chamber", pa.string()),
    ("current_chamber", pa.string()),
    ("text", pa.large_string()),
    ("html", pa.large_string()),
    ("summary_update_date", pa.string()),
    ("fetched_at", pa.string()),
])
COLUMNS = SCHEMA.names


def to_epoch(stamp):
    return math.floor(datetime.fromisoformat(stamp.replace("Z", "+00:00")).timestamp())


def to_stamp(epoch):
    return (datetime(1970, 1, 1, tzinfo=timezone.utc) + timedelta(seconds=epoch)).strftime(STAMP)


def slice_key(congress, bill_type):
    return f"{int(congress):03d}-{bill_type}"


def slice_path(congress, bill_type):
    return f"/summaries/{int(congress)}/{bill_type}"


def summary_id(item):
    bill = item.get("bill") or {}
    congress, bill_type, number, version = bill.get("congress"), bill.get("type"), bill.get("number"), item.get("versionCode")
    if not (str(congress or "").isdigit() and bill_type and number and version):
        return None
    return f"{int(congress)}-{bill_type.lower()}-{number}-{version}"


def partition_of(uid):
    """One partition per congress and bill type: 119-hr-8893-00 is in 119-hr, 93-s-1-00 in 093-s."""
    parts = (uid or "").split("-")
    return slice_key(parts[0], parts[1]) if len(parts) == 4 and parts[0].isdigit() and parts[1] else "other"


def summary_row(item):
    bill = item.get("bill") or {}
    number = str(bill.get("number") or "")
    html = item.get("text")
    return {
        "id": summary_id(item),
        "congress": int(bill["congress"]),
        "bill_type": bill["type"].lower(),
        "bill_number": int(number) if number.isdigit() else None,
        "version_code": item.get("versionCode"),
        "action_date": item.get("actionDate"),
        "action_desc": item.get("actionDesc"),
        "title": tidy(bill["title"]) if bill.get("title") else None,
        "origin_chamber": bill.get("originChamber"),
        "current_chamber": item.get("currentChamber"),
        # The html column keeps the API's characters; the text drops the ones XML does not allow, such as the form feed that ends a paragraph of the 108th Congress's H.R. 4503 summary (version 81).
        "text": html_text(xml_safe(html)) if html else None,
        "html": html,
        "summary_update_date": item.get("lastSummaryUpdateDate"),
    }


def normalize(row):
    out = {name: row.get(name) for name in COLUMNS}
    for name in ("congress", "bill_number"):
        if out[name] is not None:
            out[name] = int(out[name])
    return out


def row_weight(row):
    return len(row["text"] or "") + len(row["html"] or "")


def write(rows, path):
    return write_parquet(rows, path, schema=SCHEMA, prepare=normalize, weight=row_weight)


def comparable(row):
    out = normalize(row)
    del out["fetched_at"]
    return out


def keep(found, items):
    """Adds items to found by id, a later updateDate winning; returns the ids of these items."""
    ids = set()
    for item in items:
        uid = summary_id(item)
        if uid is None or not item.get("updateDate"):
            continue
        ids.add(uid)
        if uid not in found or item["updateDate"] >= found[uid]["updateDate"]:
            found[uid] = item
    return ids


class SummariesSource:
    def __init__(self, fetcher):
        self.fetcher = fetcher

    def page(self, path, lo=EPOCH, hi=None, offset=0, limit=PAGE, sort=None):
        params = {"format": "json", "fromDateTime": lo, "offset": offset, "limit": limit}
        if hi:
            params["toDateTime"] = hi
        if sort:
            params["sort"] = sort
        data = self.fetcher.json(f"{API}{path}", params=params) or {}
        return int((data.get("pagination") or {}).get("count") or 0), [item for item in data.get("summaries") or [] if isinstance(item, dict)]

    def head(self):
        """One request: how many summaries the API lists, and the newest updateDate. Only the date, not which summary: summaries that share it come first in no fixed order."""
        count, items = self.page("/summaries", limit=1, sort="updateDate desc")
        return {"count": count, "newest": items[0].get("updateDate") if items else None}

    def current_congress(self):
        data = self.fetcher.json(f"{API}/congress/current", params={"format": "json"}) or {}
        return int((data.get("congress") or {})["number"])

    def slices(self):
        """(congress, bill type) of every slice, the newest congress first."""
        return [(congress, bill_type) for congress in range(self.current_congress(), FIRST_CONGRESS - 1, -1) for bill_type in TYPES]

    def slice_count(self, congress, bill_type):
        return self.page(slice_path(congress, bill_type), limit=1)[0]

    def bounds(self, path):
        """(count, oldest updateDate, newest updateDate) of a listing, in two requests."""
        count, newest = self.page(path, limit=1, sort="updateDate desc")
        if not count or not newest:
            return count, None, None
        _, oldest = self.page(path, limit=1, sort="updateDate asc")
        return count, (oldest or newest)[0]["updateDate"], newest[0]["updateDate"]

    def collect(self, path, lo, hi):
        """Every summary path lists with an updateDate from lo to hi (whole seconds, both included), by id; and {window: [count, read]} for the windows whose items could not all be read."""
        found, short = {}, {}
        windows = [(to_epoch(lo), to_epoch(hi))]
        while windows:
            a, b = windows.pop()
            if a > b:
                continue
            count, items = self.page(path, to_stamp(a), to_stamp(b))
            got = keep(found, items)
            if count <= len(got):
                continue
            seconds = [to_epoch(item["updateDate"]) for item in items if summary_id(item) and item.get("updateDate")]
            if not seconds:
                short[f"{to_stamp(a)}/{to_stamp(b)}"] = [count, 0]
            elif all(x >= y for x, y in zip(seconds, seconds[1:])) and a <= seconds[-1] and seconds[0] <= b:
                # Newest first: everything after the page's oldest second is on the page.
                if seconds[-1] < b:
                    windows.append((a, seconds[-1]))
                else:
                    self.tie(path, b, found, short)
                    windows.append((a, b - 1))
            elif a < b:
                log.warning("%s: a page out of updateDate order; splitting %s to %s", path, to_stamp(a), to_stamp(b))
                middle = (a + b) // 2
                windows += [(middle + 1, b), (a, middle)]
            else:
                self.tie(path, a, found, short)
        return found, short

    def tie(self, path, second, found, short):
        """Pages one second's items in each of TIE_PLANS in turn until the distinct ids reach its count."""
        at, seen, count = to_stamp(second), set(), None
        for sort, limit, first in TIE_PLANS:
            offset = first
            while count is None or (offset < count and len(seen) < count):
                listed, items = self.page(path, at, at, offset=offset, limit=limit, sort=sort)
                count = listed if count is None else count
                if not items:
                    break
                seen |= keep(found, items)
                offset += limit
            if len(seen) >= count:
                return
        short[at] = [count, len(seen)]
        log.warning("%s: read %d of %d summaries at %s", path, len(seen), count, at)

    def exists(self, uid):
        """Whether the bill's own summaries still include this one."""
        congress, bill_type, number, version = uid.split("-", 3)
        data = self.fetcher.json(f"{API}/bill/{congress}/{bill_type}/{number}/summaries", params={"format": "json", "limit": PAGE})
        return any(item.get("versionCode") == version for item in (data or {}).get("summaries") or [])


class _Run:
    """What one sync() holds between its steps: the rows of each partition it has read or written, so a partition staged and not yet committed is never read back from the Hub's older file."""

    def __init__(self, ctx, manifest):
        self.ctx, self.manifest, self.rows = ctx, manifest, {}

    def stored(self, key):
        if key not in self.rows:
            entry = self.manifest["partitions"].get(key) or {}
            self.rows[key] = {row["id"]: row for row in self.ctx.store.read_partition(key)} if entry.get("sha256") else {}
        return self.rows[key]

    def claim(self):
        if not self.ctx.claimed:
            flush(self.ctx, self.manifest, f"{self.ctx.writer} takes the writer lease")

    def merge(self, key, items, complete=None, listed=None, short=None, removals=()):
        """Writes collected items into one partition: new and changed rows replace stored ones, and removals (ids the collection proved gone) are dropped. With complete, the partition was just read in full: listed is the API's count, and short the windows not fully read."""
        stored = self.stored(key)
        stats, now = self.ctx.stats, utcnow()
        rows = {}
        for uid, item in items.items():
            row = summary_row(item)
            old = stored.get(uid)
            if old is None or comparable(old) != comparable(row):
                rows[uid] = dict(row, fetched_at=now)
        added = sum(1 for uid in rows if uid not in stored)
        stats["fetched"] += len(items)
        stats["unchanged"] += len(items) - len(rows)
        if not rows and not removals and complete is None:
            return
        stored.update(rows)
        for uid in removals:
            del stored[uid]
        stats["removed"] += len(removals)
        entry = self.manifest["partitions"].setdefault(key, {})
        if rows or removals or not entry.get("sha256"):
            entry.update(self.ctx.store.stage_partition(key, list(stored.values())))
        if complete is not None:
            entry.update({"complete": complete, "listed": listed, "failed": max(0, listed - len(stored)), "short": short or {}, "collected_at": now})
        elif entry.get("complete"):
            # Summaries new to a slice that was read in full: the API lists them now, so its count went up by as many.
            entry["listed"] = (entry.get("listed") or 0) + added
        entry["updated_at"] = now
        self.manifest["updated_at"] = now
        self.ctx.pending.append(f"{key}: {added} added, {len(rows) - added} changed, {len(removals)} removed, {entry['rows']} rows" + (f", {len(short)} windows short" if short else ""))
        log.info(self.ctx.pending[-1])
        if time.monotonic() - self.ctx.last_commit >= self.ctx.checkpoint_seconds:
            flush(self.ctx, self.manifest)

    def resync(self, source, congress, bill_type):
        """Reads one slice in full and writes it. A stored summary the slice no longer lists is removed when the slice was read whole, and otherwise only when its bill no longer has it either; neither when that would remove a suspect share of the partition. Reading a slice can take minutes, so the writer lease is taken first; a run that only reads recent changes commits once, at its end."""
        self.claim()
        key, path = slice_key(congress, bill_type), slice_path(congress, bill_type)
        count, oldest, newest = source.bounds(path)
        if not count:
            found, short = {}, {}
        elif not oldest:
            found, short = {}, {f"{EPOCH}/": [count, 0]}
        else:
            found, short = source.collect(path, oldest, newest)
        if count and oldest and len(found) < count and not short:
            # Summaries updated while the slice was read have moved past newest.
            count, _, latest = source.bounds(path)
            if latest and latest > newest:
                more, short = source.collect(path, newest, latest)
                found.update(more)
        stored = self.stored(key)
        missing = sorted(uid for uid in stored if uid not in found)
        removals = []
        if len(missing) > max(MIN_REMOVED_GUARD, MAX_REMOVED_SHARE * len(stored)):
            log.warning("%s: the listing lacks %d of %d stored summaries; skipped as a suspect listing", key, len(missing), len(stored))
            self.ctx.stats["suspect_listings"] += 1
        elif len(found) >= count and not short:
            removals = missing
        else:
            removals = [uid for uid in missing if not source.exists(uid)]
        self.merge(key, found, complete=True, listed=count, short=short, removals=removals)


def reconcile_due(manifest):
    return age_hours(manifest.get("reconciled_at")) >= RECONCILE_HOURS


def sync(ctx, source):
    """One run; returns its record, like pipeline.sync."""
    started = utcnow()
    base = ctx.store.read_manifest() or new_manifest(SOURCE_URL)
    holder = other_writer(base, ctx.writer)
    if holder:
        log.info("deferring to %s, which wrote at %s", holder["by"], holder["at"])
        return {"started": started, "ended": utcnow(), "writer": ctx.writer, "finished": False, "stopped": "deferred", "holder": holder, "commits": 0}
    manifest = copy.deepcopy(base)
    manifest.setdefault("partitions", {})
    manifest.setdefault("failures", {})
    manifest["version"] = MANIFEST_VERSION
    run = _Run(ctx, manifest)
    finished, reason, head = True, None, None
    try:
        head = source.head()
        slices = [(congress, bill_type) for congress, bill_type in source.slices() if ctx.only is None or slice_key(congress, bill_type) in ctx.only]
        manifest["seen"] = {"count": head["count"], "at": started}
        through = manifest.get("through")
        if through and head["newest"] and not ctx.refetch:
            lo = to_stamp(to_epoch(through) - OVERLAP_HOURS * 3600)
            found, short = source.collect("/summaries", lo, max(head["newest"], through))
            by_key = {}
            for uid, item in found.items():
                by_key.setdefault(partition_of(uid), {})[uid] = item
            for key in sorted(by_key):
                if ctx.only is None or key in ctx.only:
                    run.merge(key, by_key[key])
            if short:
                # A missed change is caught when its slice is next read in full; reading the same crowded seconds on every run would not find it sooner.
                log.warning("changes since %s: %d windows not fully read: %s", lo, len(short), json.dumps(short)[:300])
                manifest["short_changes"] = short
            else:
                manifest.pop("short_changes", None)
        if ctx.only is None and head["newest"]:
            manifest["through"] = max(head["newest"], through or "")
        entries = manifest["partitions"]
        todo = [(c, t) for c, t in slices if ctx.refetch or not (entries.get(slice_key(c, t)) or {}).get("complete")]
        if not todo and (ctx.only is not None or reconcile_due(manifest)):
            # Every slice is complete here, so each has an entry; its row count comes from the manifest, not from downloading the partition.
            counts, differ = {}, []
            for congress, bill_type in slices:
                if ctx.out_of_time():
                    raise _OutOfTime
                counts[(congress, bill_type)] = count = source.slice_count(congress, bill_type)
                entry = entries[slice_key(congress, bill_type)]
                if count == entry.get("rows"):
                    entry.update({"listed": count, "failed": 0})
                else:
                    differ.append((congress, bill_type))
            quota = math.ceil(len(slices) / RESYNC_DAYS)
            stale = sorted((entries[slice_key(c, t)].get("collected_at") or "", c, t) for c, t in slices if (c, t) not in differ)
            todo = differ + [(c, t) for at, c, t in stale[:quota] if age_hours(at) >= RESYNC_DAYS * 24]
            manifest["reconciled"] = {"at": started, "slices": len(counts), "listed": sum(counts.values()), "differ": len(differ)}
        for congress, bill_type in todo:
            if ctx.out_of_time():
                raise _OutOfTime
            run.resync(source, congress, bill_type)
        if ctx.only is None and "reconciled" in manifest and manifest["reconciled"]["at"] == started:
            manifest["reconciled_at"] = started
    except _OutOfTime:
        finished, reason = False, "budget"
    except Superseded as error:
        finished, reason = False, "superseded"
        log.warning("stopped: %s", error)
    except FATAL as error:
        finished, reason = False, f"{type(error).__name__}: {error}"[:300]
        log.warning("stopped: %s", reason)
    except Exception as error:  # noqa: BLE001 - recorded in the run record; the probe backs off
        finished, reason = False, f"{type(error).__name__}: {error}"[:300]
        log.exception("run failed")
    record = {"started": started, "ended": utcnow(), "writer": ctx.writer, "finished": finished, "stopped": reason}
    record.update({key: ctx.stats[key] for key in ("fetched", "unchanged", "failed", "removed", "suspect_listings")})
    if reason == "superseded":
        return dict(record, commits=ctx.stats["commits"])
    runs = base.get("runs") or []
    changed = False
    if finished and ctx.only is None and head:
        entries = manifest["partitions"]
        listing = {"count": head["count"], "newest": head["newest"], "listed": sum(entry.get("rows") or 0 for entry in entries.values()), "at": started,
                   "partitions": {key: entries[key].get("listed") for key in sorted(entries)}}
        old = base.get("listing") or {}
        changed = any(listing[key] != old.get(key) for key in ("count", "newest", "listed", "partitions")) or age_hours(old.get("at")) > 23 or manifest.get("reconciled_at") != base.get("reconciled_at")
        manifest["listing"] = listing
    if ctx.store.staged or ctx.stats["commits"] or changed or reason not in CLEAN_STOPS or not runs or age_hours(runs[-1].get("ended")) > 23:
        manifest["runs"] = runs[-(RUNS_KEPT - 1):] + [dict(record, commits=ctx.stats["commits"] + 1)]
        try:
            flush(ctx, manifest)
        except Superseded as error:
            record.update(finished=False, stopped="superseded")
            log.warning("stopped: %s", error)
        except Exception as error:  # noqa: BLE001 - the Hub refused the final commit
            record.update(finished=False, stopped=f"{type(error).__name__}: {error}"[:300])
            log.exception("final commit failed")
    return dict(record, commits=ctx.stats["commits"])


class _OutOfTime(Exception):
    pass


def live_counts(manifest, stored, source, problems, partition_of=partition_of):
    """For verify --live: each slice's count now against the rows the Hub holds. Only a complete slice that differs by more than its tolerance is a problem."""
    from .verify import SAMPLE, tolerance

    head = source.head()
    counts = {slice_key(c, t): source.slice_count(c, t) for c, t in source.slices()}
    entries = manifest.get("partitions") or {}
    differ = {}
    for key, count in sorted(counts.items()):
        rows = len(stored.get(key) or ())
        if rows != count:
            differ[key] = [rows, count]
            if (entries.get(key) or {}).get("complete") and abs(rows - count) > tolerance(count):
                problems.append(f"{key}: complete, but {rows} rows against {count} listed now (tolerance {tolerance(count)})")
    return {"count": head["count"], "slices": len(counts), "listed": sum(counts.values()), "on_hub": sum(len(ids) for ids in stored.values()),
            "slices_differ": len(differ), "differ_sample": dict(list(differ.items())[:SAMPLE])}
