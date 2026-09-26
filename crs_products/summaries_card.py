"""Renders the bill summaries dataset's card from its manifest alone, like card.py for the products."""

import math
from collections import defaultdict

from .card import GITHUB, size_category
from .summaries import FIRST_CONGRESS, OVERLAP_HOURS, RECONCILE_HOURS, REPO_ID, RESYNC_DAYS, SCHEDULE_HOURS, SCHEMA, TYPES

COLUMN_DOCS = {
    "id": "congress-type-number-versionCode, for example 119-hr-8893-00",
    "congress": "The Congress the bill or resolution was introduced in (119 for 2025-2026)",
    "bill_type": "hr, s, hjres, sjres, hconres, sconres, hres or sres",
    "bill_number": "The bill's number",
    "version_code": "The API's versionCode: which of the bill's summaries this is (00 for the one at introduction)",
    "action_date": "The date of the action the summary describes (YYYY-MM-DD)",
    "action_desc": "The action the summary describes, such as \"Introduced in House\" or \"Passed Senate amended\"",
    "title": "The bill's title as the listing gives it",
    "origin_chamber": "House or Senate",
    "current_chamber": "The chamber the listing gives for the summary",
    "text": "The summary as plain text",
    "html": "The summary as the API gives it (HTML)",
    "summary_update_date": "The API's lastSummaryUpdateDate",
    "fetched_at": "When the run that wrote this row ran (UTC); a later read that found the summary unchanged is not recorded",
}


def years(congress):
    start = 1787 + 2 * congress
    return f"{start}-{start + 1}"


def render(manifest):
    manifest = manifest or {}
    entries = manifest.get("partitions") or {}
    listing = manifest.get("listing") or {}
    seen = manifest.get("seen") or listing
    rows = sum(entry.get("rows") or 0 for entry in entries.values())
    count = seen.get("count")
    windows = [counts for entry in entries.values() for counts in (entry.get("short") or {}).values()]
    missing = sum(entry.get("failed") or 0 for entry in entries.values() if entry.get("complete"))
    lines = ["---", "pretty_name: US Bill and Resolution Summaries (Congressional Research Service)", "license: other", "license_name: us-government-works",
             "license_link: https://www.copyright.gov/title17/92chap1.html#105", "language:", "- en",
             "task_categories:", "- summarization", "- text-generation",
             "tags:", "- legal", "- legislation", "- government", "- congress", "- united-states", "- crs",
             "size_categories:", f"- {size_category(rows)}"]
    if entries:
        lines += ["configs:", "- config_name: default", "  data_files:", "  - split: train", "    path: data/*.parquet"]
    lines += ["---", "", "# US Bill and Resolution Summaries (Congressional Research Service)", ""]
    lines += [
        f"Every summary of a bill or resolution of the United States Congress that the Congressional Research Service (CRS) wrote and the [Congress.gov API](https://api.congress.gov) lists, from the {FIRST_CONGRESS}rd Congress ({years(FIRST_CONGRESS)}) on, with its text. CRS summarizes a measure when it is introduced and again at later actions, such as passing a chamber, so a bill can have several summaries; `action_desc` names the action.",
        "",
        f"Nothing here is edited by hand. The pipeline, its tests and its schedule are in [{GITHUB.removeprefix('https://')}]({GITHUB}), and this card is rendered from `manifest.json` in the same commit.",
        "",
        "## Status",
        "",
    ]
    if count:
        complete = sum(1 for entry in entries.values() if entry.get("complete"))
        lines.append(f"**{rows:,} of {count:,} summaries** ({rows / count:.1%}) as of {seen.get('at')} UTC, when the API last listed {count:,}. {complete:,} of {len(entries):,} slices (one congress and bill type each) have been read in full.")
        if missing or windows:
            lines += ["", f"{missing:,} summaries that the API listed when their slice was last read in full are not here, and {len(windows):,} crowded seconds could not be read in full (see *Known gaps*). A slice whose count differs from its rows is read again at the next daily check."]
    else:
        lines.append("The first sync has not listed the API yet.")
    if listing.get("at"):
        lines += ["", f"Last complete sync: {listing['at']} UTC."]
    lines += ["", "| Congress | Years | Rows | Listed | Slices read in full |", "|---|---|---:|---:|---:|"]
    by_congress = defaultdict(lambda: [0, 0, 0, 0])
    for key, entry in entries.items():
        total = by_congress[int(key.split("-")[0])]
        total[0] += entry.get("rows") or 0
        total[1] += entry.get("listed") or 0
        total[2] += 1 if entry.get("complete") else 0
        total[3] += 1
    for congress in sorted(by_congress, reverse=True):
        r, n, c, p = by_congress[congress]
        lines.append(f"| {congress} | {years(congress)} | {r:,} | {n:,} | {c} of {p} |")
    if not by_congress:
        lines.append("| (none yet) | | 0 | 0 | 0 of 0 |")
    lines += [
        "",
        "*Listed* is the API's count for the slices when they were last read in full or checked.",
        "",
        "## Use",
        "",
        "```python",
        "from datasets import load_dataset",
        f'summaries = load_dataset("{REPO_ID}", split="train")',
        "```",
        "",
        "```sql",
        "-- DuckDB, straight from the Hub: every summary of H.R. 1 of the 119th Congress, oldest first",
        f"SELECT version_code, action_date, action_desc, text FROM 'hf://datasets/{REPO_ID}/data/*.parquet' WHERE congress = 119 AND bill_type = 'hr' AND bill_number = 1 ORDER BY action_date, version_code;",
        "```",
        "",
        "## Files",
        "",
        f"- `data/{{congress}}-{{type}}.parquet`: one row per summary of one congress and bill type, sorted by id, such as `data/119-hr.parquet`. The congress has three digits (`093-hr`) so the files sort in order. The {len(TYPES)} types are {', '.join(TYPES)}.",
        "- `manifest.json`: per slice, the row count, SHA-256, the API's count, when it was last read in full and any windows it could not read; the last listing; the last 20 runs.",
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
        f"A GitHub Actions job is scheduled every {SCHEDULE_HOURS} hours, at 00:00 and 12:00 UTC. It reads the summaries whose `updateDate` is at most {OVERLAP_HOURS} hours older than the newest the previous run saw, and commits the slices they changed with this card. Once a day, on the first run at least {RECONCILE_HOURS} hours after the last check, it also asks the API for each slice's count, reads again in full each slice whose count differs from its rows here, and the {math.ceil(len(entries) / RESYNC_DAYS) or 1} slices read longest ago once that was more than {RESYNC_DAYS} days ago. A slice read in full loses the summaries the API no longer lists.",
        "",
        "The listing orders summaries by `updateDate`, but summaries that share one come back in a different order from one request to the next, so paging by offset skips some and repeats others: one pass over the House bills of the 100th Congress read 6,644 distinct summaries of 6,765 on September 25, 2026. So the pipeline reads windows of `updateDate`s instead, moving each window's end down to the oldest second on its page, and pages a second that fills a whole page in several orders until it has read that second's count. That read all 6,765 of those summaries in 102 requests.",
        "",
        f"The job writes with Hugging Face Trusted Publishing, so no write token is stored anywhere. `manifest.json` names each run's writer: `github-actions` for this job, `local` for the same pipeline run from a computer. GitHub starts scheduled jobs late, or drops them, when it is busy, so a new summary can take more than {SCHEDULE_HOURS} hours to appear. Each run downloads `manifest.json` and the slices it updates, and the Hub counts a download for each 5 minutes in which a run reads files, so part of this dataset's download count is this job.",
        "",
        "## Known gaps",
        "",
        "- Only the summaries: not the bills' text, actions, cosponsors or status, which the API lists elsewhere.",
        f"- The API lists no summaries before the {FIRST_CONGRESS}rd Congress.",
        "- `title` is the one title the listing gives with the summary; a bill has several, and the listing does not say which this is.",
        "- In a second that holds more summaries than one page, every order the pipeline tries may still miss some. `manifest.json` records each such second with its count and how many were read, and each slice's `failed` is how many it lists that are not here; the status above totals both.",
        "- A summary the pipeline cannot read keeps its earlier row, if it has one, and is listed with its error under `failures` in `manifest.json`; later runs try it again.",
        "",
        "## License",
        "",
        "CRS summaries are works of the United States Government and are not subject to copyright in the United States ([17 U.S.C. § 105](https://www.copyright.gov/title17/92chap1.html#105)).",
        "",
    ]
    return "\n".join(lines)
