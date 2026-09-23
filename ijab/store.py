"""Where partitions live: a Hugging Face dataset repo, or a local directory for tests and dry runs.

Every collection owns data/{collection}/*.parquet and manifests/{collection}.json, so parallel lanes never touch the same file. A partition is committed together with its manifest, so the two cannot disagree on the Hub.
"""

import hashlib
import json
import logging
import os
import shutil
import tempfile
import time
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

SCHEMA = pa.schema([
    ("id", pa.string()),
    ("collection", pa.string()),
    ("congress", pa.int32()),
    ("type", pa.string()),
    ("number", pa.string()),
    ("version", pa.string()),
    ("chamber", pa.string()),
    ("title", pa.string()),
    ("date", pa.string()),
    ("updated_at", pa.string()),
    ("url", pa.string()),
    ("text", pa.large_string()),
    ("text_source", pa.string()),
    ("text_url", pa.string()),
    ("text_sha256", pa.string()),
    ("metadata", pa.large_string()),
    ("fetched_at", pa.string()),
])
COLUMNS = SCHEMA.names
ROW_GROUP_ROWS = 20_000
ROW_GROUP_BYTES = 64 << 20
COMMIT_ATTEMPTS = 4
# The Hub's docs say the repo experience degrades after a few thousand commits; history is squashed past this many.
SQUASH_AFTER_COMMITS = 1000
log = logging.getLogger("ijab")


def partition_path(collection, key):
    return f"data/{collection}/{key}.parquet"


def manifest_path(collection):
    return f"manifests/{collection}.json"


def normalize(row):
    out = {name: row.get(name) for name in COLUMNS}
    if out["congress"] is not None:
        out["congress"] = int(out["congress"])
    for name in ("number", "version"):
        if out[name] is not None:
            out[name] = str(out[name])
    if isinstance(out["metadata"], (dict, list)):
        out["metadata"] = json.dumps(out["metadata"], ensure_ascii=False, sort_keys=True)
    return out


def write_parquet(rows, path):
    """Rows sorted by id, zstd, content-defined chunking so an updated partition re-uploads only changed chunks."""
    rows = sorted((normalize(row) for row in rows), key=lambda row: row["id"])
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    writer = pq.ParquetWriter(path, SCHEMA, compression="zstd", compression_level=9, use_content_defined_chunking=True)
    try:
        batch, size = [], 0
        for row in rows:
            batch.append(row)
            size += len(row["text"] or "") + len(row["metadata"] or "")
            if len(batch) >= ROW_GROUP_ROWS or size >= ROW_GROUP_BYTES:
                writer.write_table(pa.Table.from_pylist(batch, schema=SCHEMA))
                batch, size = [], 0
        if batch or not rows:
            writer.write_table(pa.Table.from_pylist(batch, schema=SCHEMA))
    finally:
        writer.close()
    return {"rows": len(rows), "bytes": path.stat().st_size, "sha256": sha256_file(path), "text_rows": sum(1 for row in rows if row["text"])}


def read_parquet(path):
    return pq.read_table(path).to_pylist()


def sha256_file(path):
    digest = hashlib.sha256()
    with open(path, "rb") as handle:
        for chunk in iter(lambda: handle.read(1 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()


def dir_bytes(path):
    total = 0
    for root, _, files in os.walk(path):
        for name in files:
            try:
                total += os.lstat(os.path.join(root, name)).st_size
            except FileNotFoundError:
                pass
    return total


class _Staging:
    """Scratch space for one run. Tracks the largest footprint it ever reached, which is the local-disk bound."""

    def __init__(self, workdir=None):
        self.dir = Path(tempfile.mkdtemp(prefix="ijab-", dir=workdir))
        self.staged = {}
        self.peak_bytes = 0

    def measure(self):
        self.peak_bytes = max(self.peak_bytes, dir_bytes(self.dir))

    def scratch(self, name):
        path = self.dir / "scratch" / name
        path.parent.mkdir(parents=True, exist_ok=True)
        return path

    def stage_partition(self, collection, key, rows):
        repo_path = partition_path(collection, key)
        local = self.dir / "stage" / repo_path
        stats = write_parquet(rows, local)
        self.staged[repo_path] = local
        self.measure()
        return dict(stats, file=repo_path)

    def stage_manifest(self, collection, manifest):
        repo_path = manifest_path(collection)
        local = self.dir / "stage" / repo_path
        local.parent.mkdir(parents=True, exist_ok=True)
        local.write_text(json.dumps(manifest, indent=1, sort_keys=True, ensure_ascii=False) + "\n")
        self.staged[repo_path] = local
        return repo_path

    def put_text(self, repo_path, text, message):
        local = self.dir / "stage" / repo_path
        local.parent.mkdir(parents=True, exist_ok=True)
        local.write_text(text)
        self.staged[repo_path] = local
        return self.commit(message)

    def clear(self):
        for local in self.staged.values():
            local.unlink(missing_ok=True)
        self.staged = {}

    def close(self):
        shutil.rmtree(self.dir, ignore_errors=True)


class LocalStore(_Staging):
    def __init__(self, root, workdir=None):
        super().__init__(workdir)
        self.root = Path(root)
        self.commits = []

    def read_manifest(self, collection):
        path = self.root / manifest_path(collection)
        return json.loads(path.read_text()) if path.exists() else None

    def read_partition(self, collection, key):
        path = self.root / partition_path(collection, key)
        return read_parquet(path) if path.exists() else []

    def read_columns(self, repo_path, columns):
        return pq.read_table(self.root / repo_path, columns=columns).to_pydict()

    def file_sha256s(self, repo_paths):
        return {repo_path: sha256_file(self.root / repo_path) for repo_path in repo_paths if (self.root / repo_path).exists()}

    def list_files(self, prefix=""):
        return sorted(str(p.relative_to(self.root)) for p in self.root.rglob("*") if p.is_file() and str(p.relative_to(self.root)).startswith(prefix))

    def read_text(self, repo_path):
        path = self.root / repo_path
        return path.read_text() if path.exists() else None

    def commit(self, message):
        if not self.staged:
            return None
        for repo_path, local in self.staged.items():
            target = self.root / repo_path
            target.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(local, target)
        self.commits.append({"message": message, "files": sorted(self.staged)})
        self.clear()
        return str(len(self.commits))


class HubStore(_Staging):
    def __init__(self, repo_id, workdir=None, token=None):
        from huggingface_hub import HfApi

        super().__init__(workdir)
        self.repo_id = repo_id
        self.api = HfApi(token=token)
        self.revision = self.api.dataset_info(repo_id).sha

    def _download(self, repo_path):
        from huggingface_hub import hf_hub_download
        from huggingface_hub.errors import EntryNotFoundError, RemoteEntryNotFoundError

        target = self.dir / "download"
        try:
            local = hf_hub_download(self.repo_id, repo_path, repo_type="dataset", revision=self.revision, local_dir=target, token=self.api.token)
        except (EntryNotFoundError, RemoteEntryNotFoundError):
            return None
        self.measure()
        return Path(local)

    def read_manifest(self, collection):
        local = self._download(manifest_path(collection))
        if local is None:
            return None
        try:
            return json.loads(local.read_text())
        finally:
            local.unlink(missing_ok=True)

    def read_partition(self, collection, key):
        local = self._download(partition_path(collection, key))
        if local is None:
            return []
        try:
            return read_parquet(local)
        finally:
            local.unlink(missing_ok=True)

    def read_columns(self, repo_path, columns):
        """Reads only the named columns, by HTTP range requests, so verification never downloads the text."""
        from huggingface_hub import HfFileSystem

        fs = HfFileSystem(token=self.api.token)
        with fs.open(f"datasets/{self.repo_id}@{self.revision}/{repo_path}", "rb") as handle:
            return pq.read_table(handle, columns=columns).to_pydict()

    def file_sha256s(self, repo_paths):
        out = {}
        paths = list(repo_paths)
        for start in range(0, len(paths), 100):
            for info in self.api.get_paths_info(self.repo_id, paths[start:start + 100], repo_type="dataset", revision=self.revision):
                lfs = getattr(info, "lfs", None)
                if lfs is not None:
                    out[info.path] = lfs.sha256
        return out

    def list_files(self, prefix=""):
        return sorted(path for path in self.api.list_repo_files(self.repo_id, repo_type="dataset", revision=self.revision) if path.startswith(prefix))

    def read_text(self, repo_path):
        local = self._download(repo_path)
        if local is None:
            return None
        try:
            return local.read_text()
        finally:
            local.unlink(missing_ok=True)

    def commit(self, message):
        """One atomic commit of everything staged. Retries rate limits and server errors; a retried commit that already landed only re-adds identical files, so the retry cannot corrupt the repo."""
        from huggingface_hub import CommitOperationAdd
        from huggingface_hub.errors import HfHubHTTPError

        if not self.staged:
            return None
        for attempt in range(COMMIT_ATTEMPTS):
            operations = [CommitOperationAdd(path_in_repo=repo_path, path_or_fileobj=str(local)) for repo_path, local in sorted(self.staged.items())]
            try:
                info = self.api.create_commit(self.repo_id, operations=operations, commit_message=message, repo_type="dataset")
                break
            except HfHubHTTPError as error:
                status = getattr(error.response, "status_code", None)
                if attempt == COMMIT_ATTEMPTS - 1 or status not in (408, 429, 500, 502, 503, 504):
                    raise
                log.warning("commit attempt %d failed with HTTP %s; retrying", attempt + 1, status)
                time.sleep(60 * (attempt + 1))
        self.revision = info.oid
        self.clear()
        return info.oid
