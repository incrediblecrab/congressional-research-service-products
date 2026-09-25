"""Renders the Constitution Annotated dataset's card from its manifest alone, like card.py for the products."""

from .card import GITHUB, size_category
from .constitution import REPO_ID, SCHEMA
from .pipeline import MAX_ATTEMPTS, TEXT_RETRY_HOURS

COLUMN_DOCS = {
    "id": "GovInfo's granule id (GPO-CONAN-2022-8), or the package id for a package without granules (GPO-CONAN-2024-SUPP)",
    "package_id": "The GovInfo package: one printed edition or supplement",
    "package_title": "The package's title as GovInfo's collection listing gives it",
    "kind": "edition or supplement, from the package title",
    "title": "The granule's title, a part of the book such as \"Article I - Legislative Branch\"; for a package without granules, its title",
    "sequence": "The granule's number in its package (8 in GPO-CONAN-2022-8); null for a package without granules",
    "subsequence": "For a granule under a heading, its number there (1 in GPO-CONAN-1992-9-1); else null",
    "date_issued": "GovInfo's dateIssued (YYYY-MM-DD)",
    "pages": "Pages in the PDF, as pdftotext counts them",
    "text": "The PDF's text layer, pages separated by form feeds, so page n is `text.split(\"\\f\")[n - 1]`; null when the PDF has no text layer",
    "url": "The unit's page on govinfo.gov (the record's detailsLink)",
    "pdf_url": "The GovInfo API link the PDF was fetched from; the API needs an api.data.gov key",
    "pdf_sha256": "SHA-256 of the PDF's bytes",
    "updated_at": "The package's lastModified when this row was written. A changed value triggers a re-fetch; one that finds the unit unchanged keeps the row and records the new value in `manifest.json` instead",
    "metadata": "GovInfo's summary record for the unit, as JSON",
    "fetched_at": "When the fetch that wrote this row ran (UTC)",
}


def render(manifest):
    manifest = manifest or {}
    entries = manifest.get("partitions") or {}
    failures = manifest.get("failures") or {}
    listing = manifest.get("listing") or {}
    seen = manifest.get("seen") or listing
    rows = sum(entry.get("rows") or 0 for entry in entries.values())
    with_text = sum(entry.get("text_rows") or 0 for entry in entries.values())
    exhausted = sum(1 for f in failures.values() if f["attempts"] >= MAX_ATTEMPTS)
    count = seen.get("count")
    lines = ["---", "pretty_name: Constitution Annotated (Congressional Research Service)", "license: other", "license_name: us-government-works",
             "license_link: https://www.copyright.gov/title17/92chap1.html#105", "language:", "- en",
             "task_categories:", "- text-generation", "- question-answering",
             "tags:", "- legal", "- constitutional-law", "- government", "- congress", "- united-states", "- crs",
             "size_categories:", f"- {size_category(rows)}"]
    if entries:
        lines += ["configs:", "- config_name: default", "  data_files:", "  - split: train", "    path: data/*.parquet"]
    lines += ["---", "", "# Constitution Annotated (Congressional Research Service)", ""]
    lines += [
        "*The Constitution of the United States of America: Analysis and Interpretation*, which the Congressional Research Service (CRS) prepares and Congress prints as a Senate Document: every edition and supplement that [GovInfo](https://www.govinfo.gov) holds, with the full text of each part of the book and GovInfo's record of it.",
        "",
        f"Nothing here is edited by hand. The pipeline, its tests and its schedule are in [{GITHUB.removeprefix('https://')}]({GITHUB}), and this card is rendered from `manifest.json` in the same commit.",
        "",
        "## Status",
        "",
    ]
    if count:
        lines.append(f"**{rows:,} of {count:,} units** ({rows / count:.1%}) as of {seen.get('at')} UTC, when GovInfo last listed {count:,}. {with_text:,} rows have text. {exhausted:,} units failed {MAX_ATTEMPTS} times and are tried again by each weekly run; their errors are in `manifest.json`.")
    else:
        lines.append("The first sync has not listed GovInfo yet.")
    if listing.get("at"):
        lines += ["", f"Last complete sync: {listing['at']} UTC."]
    lines += ["", "| Package | Rows | With text | Listed | Complete |", "|---|---:|---:|---:|---|"]
    for key in sorted(entries):
        entry = entries[key]
        lines.append(f"| {key} | {entry.get('rows') or 0:,} | {entry.get('text_rows') or 0:,} | {entry.get('listed') or 0:,} | {'yes' if entry.get('complete') else 'no'} |")
    if not entries:
        lines.append("| (none yet) | 0 | 0 | 0 | no |")
    lines += [
        "",
        "## Use",
        "",
        "```python",
        "from datasets import load_dataset",
        f'conan = load_dataset("{REPO_ID}", split="train")',
        "```",
        "",
        "```sql",
        "-- DuckDB, straight from the Hub: the parts of the newest edition, in the book's order",
        f"SELECT sequence, subsequence, title, pages FROM 'hf://datasets/{REPO_ID}/data/*.parquet' WHERE kind = 'edition' AND date_issued = (SELECT max(date_issued) FROM 'hf://datasets/{REPO_ID}/data/*.parquet' WHERE kind = 'edition') ORDER BY sequence, subsequence;",
        "```",
        "",
        "## Files",
        "",
        "- `data/{package}.parquet`: one row per unit of one GovInfo package, sorted by id. A unit is a granule, GovInfo's part of a book, or a whole package that GovInfo does not split into granules (most supplements).",
        "- `manifest.json`: per package, the row count, SHA-256, rows with text and whether it is complete; failed units with their errors; the last listing; the last 20 runs.",
        "",
        "## Schema",
        "",
        "| Column | Type | Description |",
        "|---|---|---|",
    ]
    for column in SCHEMA:
        lines.append(f"| `{column.name}` | {column.type} | {COLUMN_DOCS[column.name]} |")
    lines += [
        "",
        "## How it stays current",
        "",
        "A GitHub Actions job runs once a week. It lists the GPO-CONAN packages in GovInfo's collection of additional government publications (GPO) and the granules of each, then fetches the units that are new or whose package's `lastModified` changed, removes the ones GovInfo no longer has, and commits the changed packages with this card. `lastModified` is GovInfo's stamp, not the book's: on September 25, 2026, every package's was from March 7 to 8 or August 18, 2025, long after it was printed. That is also why the job runs weekly rather than daily: GovInfo changes these packages a few times a year, and every run downloads `manifest.json`, which the Hub counts as a download of this dataset. A unit whose record and PDF did not change keeps its row, and its package file is not rewritten.",
        "",
        "The job writes with Hugging Face Trusted Publishing, so no write token is stored anywhere. `manifest.json` names each run's writer: `github-actions` for this job, `local` for the same pipeline run from a computer. GitHub starts scheduled jobs late, or drops them, when it is busy, so a new supplement can take more than a week to appear.",
        "",
        "## Known gaps",
        "",
        "- This is the printed book, not the web edition at [constitution.congress.gov](https://constitution.congress.gov), which CRS updates between printings. That site answers automated requests with a Cloudflare challenge, which this pipeline does not try to get past.",
        "- A granule that only heads others, such as GPO-CONAN-1992-9 (\"The Constitution of the United States of America (With Annotations)\") over GPO-CONAN-1992-9-1 to -9-8, has no PDF of its own and is not a row; its parts are. On September 25, 2026, 9 editions had 2 such headings each.",
        "- `text` is the PDF's text layer: it keeps running heads, page numbers and footnote markers, and has no table structure. The first page of the 2020 Supplement's errata sheet (GPO-CONAN-2020-SUPP-2) has text in its text layer that the rendered page does not show, such as \"115th\" beside the visible \"116th Congress\", so its `text` interleaves the two.",
        f"- A PDF without a text layer gives a row with null `text`, fetched again every {TEXT_RETRY_HOURS // 24} days.",
        "",
        "## License",
        "",
        "The Constitution Annotated is a work of the United States Government and is not subject to copyright in the United States ([17 U.S.C. § 105](https://www.copyright.gov/title17/92chap1.html#105)).",
        "",
    ]
    return "\n".join(lines)
