# sources

Adapters that turn a source's listings into units and each unit into dataset rows.

**Objective:** hide each source's paging, identifiers and formats behind four methods the sync loop calls: `plan`, `fetch_many`, `unit_of` and `live_counts`.

**Inputs:** Congress.gov API v3 and the GovInfo API (both keyed), CRS PDFs on www.congress.gov, GovInfo bulk data and package content (keyless).

**Files:**

- `congress.py`: one generic adapter configured per Congress.gov endpoint (treaties, members, committees, congresses, House requirements, nominations, committee meetings, House votes, House and Senate communications, amendments, bills before the 108th Congress), plus CRS reports. CRS text comes from each report's PDF, because www.congress.gov answers requests for the HTML with a Cloudflare challenge.
- `govinfo.py`: bulk bill status, bill text and laws; API-listed committee reports, hearings, committee prints and congressional documents, with MODS metadata and HTML or PDF text, one row per part when GovInfo publishes a document only in parts; Congressional Record issues, one row per granule.
- `__init__.py`: marks the package.
