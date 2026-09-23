# fixtures

Real source responses, trimmed to what the tests need, recorded September 23, 2026.

**Objective:** test parsing against the formats the sources actually return, not formats written from memory.

**Inputs:** Congress.gov API v3 and GovInfo package content.

**Files:**

- `list-*.json`, `detail-*.json`: one list page and one item from each Congress.gov endpoint, named for the endpoint. None contains an API key.
- `BILLSTATUS-118sconres1.xml`: a bill status record.
- `BILLS-118sconres1is.xml`: the bill's text.
- `PLAW-118publ1.xml`: a public law in USLM XML.
- `CDOC-119tdoc2.htm`: a Senate Treaty Document's HTML rendition.
- `CREC-2024-01-02-mods.xml`: MODS metadata for one Congressional Record issue.
