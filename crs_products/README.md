# crs_products

The pipeline package, run as `python -m crs_products {run,probe,card,verify,squash}`.

**Objective:** keep each Parquet partition equal to what the Congress.gov API lists for its id range, with one writer at a time and little local disk.

**Inputs:** the API's CRS listing and product records, the PDF and HTML renditions, the manifest on the Hub, and `DATA_GOV_API_KEY` from the environment.

**Files:**

- `cli.py`, `__main__.py`: the commands. `probe` decides from one API request whether a sync is needed, `run` syncs within a time budget, `verify` checks the Hub against the manifest (with `--live`, also against the API's listing), `card` re-renders the dataset card, `squash` shortens long Hub history.
- `pipeline.py`: the sync loop, the writer lease, and the probe's decision.
- `source.py`: the API adapter: listing, product records, and text from the PDF, else the HTML.
- `http.py`: per-host pacing, bounded retries, bot-challenge detection, the key sent only in a header.
- `store.py`: the schema, the partitions, and the Hub store's parent-commit fence.
- `text.py`: HTML and PDF to plain text.
- `verify.py`: the publication check.
- `card.py`: renders the dataset card from the manifest.
