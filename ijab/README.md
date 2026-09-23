# ijab

The pipeline package, run as `python -m ijab {run,card,verify,squash}`.

**Objective:** keep every collection's Parquet partitions equal to what its source currently lists, spending little of each source's rate limit and little local disk.

**Inputs:** the sources named in `collections.py`, the manifests on the Hub, and `DATA_GOV_API_KEY` from the environment.

**Files:**

- `cli.py`, `__main__.py`: the commands. `run` syncs one lane within a time budget, `card` regenerates the dataset card, `verify` checks files against manifests (with `--live`, also against source counts), `squash` shortens long Hub history.
- `collections.py`: the registry of collections: lane, adapter, source, and what the `text` column holds.
- `pipeline.py`: the sync loop. It lists units, compares them with the manifest, fetches only new or changed ones, retries failures, refuses suspiciously short listings and resumes where the last run stopped.
- `http.py`: the fetcher: per-host pacing, bounded retries, soft-404 and bot-challenge detection, the key sent only in a header.
- `store.py`: the Parquet schema and the Hub and local stores.
- `text.py`: HTML, XML and PDF to plain text.
- `verify.py`: the publication gate.
- `card.py`: renders the dataset card from the manifests.
- [`sources/`](sources/README.md): the source adapters.
