# tests

Offline tests: `pip install '.[test]'`, then `python -m pytest -q`. They use no network and no key.

**Objective:** pin down the behavior an unattended pipeline depends on. Each gate was also checked by planting the defect it should catch.

**Inputs:** real, trimmed source samples in [`fixtures/`](fixtures/README.md), and scripted fakes.

**Files:**

- `conftest.py`: a scripted source adapter, and a fetcher that serves fixtures by URL.
- `test_pipeline.py`: the sync loop: completion, idle runs, change detection, retries, the short-listing guard, resumption.
- `test_sources.py`: adapters on real samples: ids, parsing, and the unit-to-row invariant.
- `test_http.py`: the real fetcher on a mock transport: soft 404s, bot challenges, quota exhaustion, missing keys.
- `test_verify.py`: the verification gate catches each planted defect, through the function and the CLI exit code.
- `test_card.py`: the dataset card's YAML front matter and status rows.
- `test_cli.py`: the squash threshold, the Trusted Publishers resource, and a workflow lane matrix equal to the registry.
