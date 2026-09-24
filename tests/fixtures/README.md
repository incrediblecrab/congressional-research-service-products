# fixtures

Real Congress.gov API responses, recorded September 23, 2026. None contains an API key.

**Objective:** test parsing against what the API actually returns, not a format written from memory.

**Inputs:** `GET /v3/crsreport` and `GET /v3/crsreport/{id}`.

**Files:**

- `list-crsreport.json`: a two-item listing page.
- `detail-crsreport-IN12740.json`: a post with PDF and HTML renditions and no topics.
- `detail-crsreport-RL34480.json`: a report with only an HTML rendition, one topic, and its author listed twice.
