"""Checks the published artifacts against their manifests and, with --live, against the sources' current counts.

Hard checks read the Parquet files themselves (only the id and url columns), never trusting the manifest alone: every manifest partition exists with the recorded sha256 and row count, ids are non-null and unique, the distinct units match the manifest, a complete partition accounts for every listed unit, and no data file is missing from its manifest. The live check compares each complete partition with the source's own current count, within tolerance().
"""

import math
from collections import Counter

from .pipeline import MAX_ATTEMPTS, Context
from .store import partition_path

# Fixed before any measurement (contract v1): drift up to max(10, 0.5%) is publication lag, beyond it a defect.
TOLERANCE_MIN = 10
TOLERANCE_SHARE = 0.005


def tolerance(count):
    return max(TOLERANCE_MIN, math.ceil(TOLERANCE_SHARE * count))


def check_collection(store, collection, fetcher=None, live=False, files=None, hashes=None):
    """Returns (problems, summary) for one collection."""
    problems = []
    manifest = store.read_manifest(collection.name)
    if manifest is None:
        return problems, {"collection": collection.name, "status": "not started"}
    adapter = collection.adapter(Context(fetcher=fetcher, store=store, deadline=math.inf), collection)
    exhausted = Counter(f.get("partition") for f in (manifest.get("failures") or {}).values() if f.get("attempts", 0) >= MAX_ATTEMPTS)
    files = files if files is not None else set(store.list_files(f"data/{collection.name}/"))
    hashes = hashes if hashes is not None else store.file_sha256s([partition_path(collection.name, key) for key in manifest["partitions"]])
    units_by_key, rows_total, text_total = {}, 0, 0
    for key, entry in sorted(manifest["partitions"].items()):
        where = f"{collection.name}/{key}"
        path = partition_path(collection.name, key)
        if path not in files:
            problems.append(f"{where}: manifest lists {path} but the repo has no such file")
            continue
        if path in hashes and hashes[path] != entry.get("sha256"):
            problems.append(f"{where}: file sha256 {hashes[path][:12]} differs from manifest {str(entry.get('sha256'))[:12]}")
        columns = store.read_columns(path, ["id", "url"])
        ids = columns["id"]
        if len(ids) != entry.get("rows"):
            problems.append(f"{where}: {len(ids)} rows in file, manifest says {entry.get('rows')}")
        if any(value is None for value in ids):
            problems.append(f"{where}: null id")
        duplicates = [value for value, n in Counter(ids).items() if n > 1]
        if duplicates:
            problems.append(f"{where}: {len(duplicates)} duplicate ids, e.g. {duplicates[0]}")
        units = len({adapter.unit_of({"id": i, "url": u}) for i, u in zip(ids, columns["url"]) if i is not None})
        units_by_key[key] = units
        if units != entry.get("units"):
            problems.append(f"{where}: {units} distinct units in file, manifest says {entry.get('units')}")
        if entry.get("complete") and entry.get("source_count") is not None and units + exhausted[key] != entry["source_count"]:
            problems.append(f"{where}: complete, but {units} units + {exhausted[key]} failed != {entry['source_count']} listed")
        rows_total += len(ids)
        text_total += entry.get("text_rows") or 0
    listed = {partition_path(collection.name, key) for key in manifest["partitions"]}
    for path in sorted(files - listed):
        problems.append(f"{collection.name}: {path} is not in the manifest")

    complete = [key for key, entry in manifest["partitions"].items() if entry.get("complete")]
    summary = {"collection": collection.name, "partitions": len(manifest["partitions"]), "complete": len(complete),
               "rows": rows_total, "text_rows": text_total, "units": sum(units_by_key.values()), "failed_units": sum(exhausted.values()),
               "hash_checked": sum(1 for key in manifest["partitions"] if partition_path(collection.name, key) in hashes)}
    if live:
        keys = sorted(manifest["partitions"], key=str)
        try:
            counts = adapter.live_counts(keys)
        except Exception as error:  # noqa: BLE001 - an unreachable source is a verification failure, reported as such
            problems.append(f"{collection.name}: live count failed: {type(error).__name__}: {error}"[:300])
            return problems, summary
        if "*" in counts:
            pairs = [("*", summary["units"] + summary["failed_units"], counts["*"])] if len(complete) == len(keys) else []
            source_total = counts["*"]
        else:
            pairs = [(key, units_by_key.get(key, 0) + exhausted[key], counts[key]) for key in complete if key in counts]
            source_total = sum(counts.get(key, 0) for key in keys)
        drift = 0
        for key, have, source in pairs:
            if abs(have - source) > tolerance(source):
                drift += 1
                problems.append(f"{collection.name}/{key}: {have} units (incl. failed) vs {source} at the source, beyond tolerance {tolerance(source)}")
        summary.update({"live_compared": len(pairs), "live_drift": drift, "source_units": source_total,
                        "source_partitions_not_started": len(set(counts) - set(keys) - {"*"})})
    return problems, summary


def check(store, collections, fetcher=None, live=False):
    files = set(store.list_files("data/"))
    problems, summaries = [], []
    for collection in collections:
        found, summary = check_collection(store, collection, fetcher, live, files={f for f in files if f.startswith(f"data/{collection.name}/")})
        problems.extend(found)
        summaries.append(summary)
    return problems, summaries
