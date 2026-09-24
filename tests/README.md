# tests

Offline tests: `pip install '.[test]'`, then `python -m pytest -q`. They use no network and no key; PDF tests are skipped without `pdftotext`.

**Objective:** pin down the behavior an unattended pipeline depends on. Each load-bearing check was also tested by planting the defect it should catch, a changed line of code or data, and confirming that a test fails.

**Inputs:** real API responses in [`fixtures/`](fixtures/README.md), and scripted fakes.

**Files:**

- `conftest.py`: a scripted CRS source, and a fetcher that serves fixtures by URL.
- `test_pipeline.py`: the sync loop: first sync, idle runs, changes and removals, retries, the text retry, the suspect-listing guard, resumption, the writer lease, and the probe's decision.
- `test_source.py`: rows from real API records, text renditions in order, bot challenges, listing pages.
- `test_http.py`: the fetcher on a mock transport: challenges, quota exhaustion, missing keys, outages.
- `test_store.py`: partition keys, Parquet round trips, and the commit fence against a fake Hub.
- `test_verify.py`: each planted data defect is named, and the command exits 1.
- `test_card.py`: the card's front matter and numbers.
- `test_cli.py`: exit codes, `$GITHUB_OUTPUT`, Trusted Publishing, and the workflow's commands, options and outputs.
