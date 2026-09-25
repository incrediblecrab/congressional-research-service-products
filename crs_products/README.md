# crs_products

The pipeline package, run as `python -m crs_products {run,probe,card,verify,squash} --dataset {products,summaries,constitution}` (default products).

**Objective:** keep each dataset's Parquet partitions equal to what its source lists, with one writer at a time and little local disk.

**Inputs:** the Congress.gov API's CRS products and summaries, product PDFs and HTML, GovInfo's Constitution Annotated, each manifest on the Hub, and `DATA_GOV_API_KEY`.

**Files:**

- `cli.py`, `__main__.py`: the commands. `probe` (products only) decides from one API request whether a sync is needed, `run` syncs within a time budget, `verify` checks the Hub against the manifest (with `--live`, also against the source), `card` re-renders the dataset card, `squash` shortens long Hub history.
- `pipeline.py`: the sync loop for products and the Constitution Annotated, the writer lease, the probe's decision.
- `source.py`: the products' API adapter.
- `summaries.py`: the summaries' listing reader and sync.
- `constitution.py`: the Constitution Annotated's GovInfo adapter.
- `card.py`, `summaries_card.py`, `constitution_card.py`: each dataset's card, rendered from its manifest.
- `http.py`: per-host pacing, bounded retries, bot-challenge detection, the key sent only in a header.
- `store.py`: the products' schema, partitions, and the Hub store's parent-commit fence.
- `text.py`: HTML and PDF to plain text.
- `verify.py`: the publication check.
