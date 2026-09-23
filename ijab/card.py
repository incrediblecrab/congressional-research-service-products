"""Renders the Hugging Face dataset card (README.md) from the manifests on the Hub. Every number in it comes from a manifest."""

import json

from .collections import COLLECTIONS, LANES
from .pipeline import MAX_ATTEMPTS, RUNS_KEPT
from .store import SCHEMA, SQUASH_AFTER_COMMITS, manifest_path
from .verify import TOLERANCE_MIN, TOLERANCE_SHARE

GITHUB = "https://github.com/incrediblecrab/im-just-a-bill"
COLUMN_DOCS = {
    "id": "Stable identifier within the collection (GovInfo package or granule id, Congress.gov-derived key, or CRS product number)",
    "collection": "Collection name, the same as the config name",
    "congress": "Congress number, when the source gives one",
    "type": "Document type in lowercase as the source names it (for example hr, sres, hrpt, chrg, speech, in)",
    "number": "Document number within its type, or page range for Congressional Record granules",
    "version": "Bill version code, CRS product version, roll-call session, or nomination part",
    "chamber": "House, Senate or Joint, when known",
    "title": "Title as given by the source",
    "date": "Date issued, introduced, received or held (ISO 8601), as the source gives it",
    "updated_at": "The source's own last-modified value for the unit; a changed value is what triggers a re-fetch",
    "url": "Canonical page for the document at the source (GovInfo details page, Congress.gov bill page, or API resource)",
    "text": "Plain text; what it holds differs by collection (see below). Null when the source has none",
    "text_source": "How text was obtained: bill-xml, uslm-xml, govinfo-html, govinfo-pdf, crs-html, crs-pdf, crs-summary, resolution-text, abstract",
    "text_url": "Exact URL the text was extracted from",
    "text_sha256": "SHA-256 of the bytes fetched from text_url (before extraction)",
    "metadata": "The source record as JSON (MODS, bill status XML, or API detail with sub-lists)",
    "fetched_at": "When this row was fetched (UTC)",
}
DEFERRED = [
    "CRS HTML renditions: www.congress.gov answers them with a Cloudflare bot challenge (measured 09/23/2026), which this pipeline does not try to get past. CRS text therefore comes from the PDF's text layer and keeps its line breaks, page headers and footers.",
    "Amendment text, and committee-meeting and hearing documents linked from Congress.gov: text is only on www.congress.gov, which asks for at most 10 requests/minute and is spent on CRS reports first.",
    "Bound Congressional Record (GovInfo CRECB) and Congressional Record Index.",
    "Congress.gov committee reports that have no GovInfo CRPT package (Congress.gov lists 20,324 reports and GovInfo 19,755 CRPT packages as of 09/23/2026).",
    "Other GovInfo series filed under the same collection codes, which Congress.gov does not present as its documents: SERIALSET, GOVPUB, GPO, ERP, HMAN and SMAN packages (for example 78,633 SERIALSET packages under CDOC and 142,992 under CRPT as of 09/23/2026).",
    "Senate roll-call votes (not in the Congress.gov API) and House votes before the 118th Congress (not yet in the API).",
]


def size_category(rows):
    for limit, label in ((1_000, "n<1K"), (10_000, "1K<n<10K"), (100_000, "10K<n<100K"), (1_000_000, "100K<n<1M"), (10_000_000, "1M<n<10M"), (100_000_000, "10M<n<100M")):
        if rows < limit:
            return label
    return "100M<n<1B"


def fmt(value):
    return f"{value:,}" if isinstance(value, int) else ("" if value is None else str(value))


def render(manifests, data_files):
    """manifests: {collection: manifest dict}; data_files: repo paths under data/."""
    present = [c for c in COLLECTIONS if any(path.startswith(f"data/{c.name}/") for path in data_files)]
    total_rows = sum(entry.get("rows") or 0 for m in manifests.values() for entry in m["partitions"].values())
    lines = ["---", "pretty_name: I'm Just a Bill", "license: other", "license_name: us-government-works",
             "license_link: https://www.copyright.gov/title17/92chap1.html#105", "language:", "- en",
             "task_categories:", "- text-generation", "- summarization", "- text-classification",
             "tags:", "- legal", "- government", "- congress", "- legislation", "- united-states", "- crs-reports",
             "size_categories:", f"- {size_category(total_rows)}"]
    if present:
        lines.append("configs:")
        for collection in present:
            lines += [f"- config_name: {collection.name}", "  data_files:", "  - split: train", f"    path: data/{collection.name}/*.parquet"]
            if collection.name == "crs_reports":
                lines.append("  default: true")
    lines += ["---", "", "# I'm Just a Bill", ""]
    lines += [
        "Documents of the United States Congress as published on [Congress.gov](https://www.congress.gov) and [GovInfo](https://www.govinfo.gov), the site Congress.gov takes its document text from: CRS reports, bills and their full text, laws, committee reports, hearings, committee prints, House and Senate documents including treaty documents, the daily Congressional Record, treaties, nominations, communications, votes, amendments, members and committees. One config per collection, one uniform schema, updated automatically on a schedule; the status table shows when each collection last synced.",
        "",
        f"The pipeline, its tests and its update schedule are in [{GITHUB.removeprefix('https://')}]({GITHUB}). Nothing here is hand-edited: this card is regenerated from the manifests in `manifests/` after every run.",
        "",
        "## Status",
        "",
        f"A unit is what the source lists and dates (a package, an API item, a daily issue); most units give one row; a Congressional Record issue, or a document GovInfo publishes only in parts, gives one row per granule. *Listed* is the number of units the source listed in the partitions synced so far; *failed* units were tried {MAX_ATTEMPTS} times and are recorded with their error in the manifest. A collection is complete when every partition it has started is complete; backfill runs until every partition is.",
        "",
        "| Config | Rows | Rows with text | Units | Listed | Failed | Partitions complete | Last change (UTC) |",
        "|---|---:|---:|---:|---:|---:|---:|---|",
    ]
    for collection in COLLECTIONS:
        manifest = manifests.get(collection.name)
        if not manifest:
            lines.append(f"| {collection.name} | not started | | | | | | |")
            continue
        parts = manifest["partitions"].values()
        failed = sum(1 for f in (manifest.get("failures") or {}).values() if f.get("attempts", 0) >= MAX_ATTEMPTS)
        listed = sum(entry.get("source_count") or 0 for entry in parts)
        lines.append("| {} | {} | {} | {} | {} | {} | {} / {} | {} |".format(
            collection.name, fmt(sum(e.get("rows") or 0 for e in parts)), fmt(sum(e.get("text_rows") or 0 for e in parts)),
            fmt(sum(e.get("units") or 0 for e in parts)), fmt(listed), fmt(failed), fmt(sum(1 for e in parts if e.get("complete"))), fmt(len(manifest["partitions"])),
            (manifest.get("updated_at") or "").replace("T", " ").removesuffix("Z")))
    lines += ["", f"Total rows: {total_rows:,}.", ""]
    lines += ["## Use", "", "```python", "from datasets import load_dataset", "", 'crs = load_dataset("incrediblecrab/im-just-a-bill", "crs_reports", split="train")', "```", "",
              "Or query the Parquet files in place, without downloading a collection:", "", "```python", "import duckdb", "",
              "duckdb.sql(\"\"\"SELECT id, title, date FROM 'hf://datasets/incrediblecrab/im-just-a-bill/data/hearings/*.parquet' WHERE congress = 118 LIMIT 5\"\"\").show()",
              "```", ""]
    lines += ["## Collections", "", "| Config | Unit | Source | `text` holds |", "|---|---|---|---|"]
    for collection in COLLECTIONS:
        lines.append(f"| {collection.name} | {collection.unit} | {collection.source} | {collection.text} |")
    lines += ["", "Files are `data/{config}/{partition}.parquet`, partitioned by Congress (Congressional Record: by year; CRS reports: by product-number prefix such as R, RL, IN, IF, LSB; small collections: `all`). Rows are sorted by `id`.", ""]
    lines += ["## Schema", "", "Every config has the same columns.", "", "| Column | Type | Meaning |", "|---|---|---|"]
    for field in SCHEMA:
        lines.append(f"| `{field.name}` | {field.type} | {COLUMN_DOCS[field.name]} |")
    lines += ["",
              "## How it stays current", "",
              f"A scheduled GitHub Actions workflow ([`pipeline.yml`]({GITHUB}/blob/main/.github/workflows/pipeline.yml)) runs {len(LANES)} lanes in parallel on its schedule, split by the rate limit each one spends (api.congress.gov, www.congress.gov, www.govinfo.gov). Each lane lists its sources, compares every unit's last-modified value with the value stored in the Parquet file, fetches only new or changed units, drops units the source no longer lists, and commits each rewritten partition together with its manifest. The runner keeps one partition on disk at a time, so the full corpus is never stored locally. Writes to this repo use Hugging Face Trusted Publishing (short-lived OIDC tokens), so no write token is stored anywhere.",
              "",
              f"Each manifest records per partition the file's SHA-256, rows, rows with text, units, units listed by the source, failures and a fingerprint of the listing, plus the last {RUNS_KEPT} runs. After every run a verification job reads the `id` column of every file, checks it against its manifest, and compares complete partitions with the sources' current counts (tolerance: the larger of {TOLERANCE_MIN} units or {TOLERANCE_SHARE:.1%}).",
              "",
              f"History is squashed into one commit once it passes {SQUASH_AFTER_COMMITS:,} commits, so a pinned revision stays available only until the next squash.",
              "",
              "## Known gaps", ""]
    lines += [f"- {item}" for item in DEFERRED]
    lines += ["- During the initial backfill, which takes days because api.congress.gov allows 5,000 requests an hour, collections are incomplete; the status table shows how far each has got.", "",
              "## License and provenance", "",
              "These are works of the United States Government. [17 U.S.C. § 105](https://www.copyright.gov/title17/92chap1.html#105): \"Copyright protection under this title is not available for any work of the United States Government\". Some documents quote or reproduce third-party material (for example, statements submitted for a hearing record, or figures in a CRS report) that may still be under copyright; CRS reports carry the same caution. Only text is included, no images.",
              "",
              "Not affiliated with or endorsed by the Library of Congress, the Congressional Research Service or the Government Publishing Office. Text is extracted automatically and can differ from the official PDF in layout, hyphenation and tables; cite the official document through `url`.",
              ""]
    return "\n".join(lines)


def load_manifests(store):
    manifests = {}
    for collection in COLLECTIONS:
        raw = store.read_text(manifest_path(collection.name))
        if raw:
            manifests[collection.name] = json.loads(raw)
    return manifests
