# im-just-a-bill

This repository builds and updates [im-just-a-bill](https://huggingface.co/datasets/incrediblecrab/im-just-a-bill), a public Hugging Face dataset of documents from Congress.gov and GovInfo: CRS reports, bills and their text, laws, treaties, committee reports, hearings, committee prints, House and Senate documents, the Congressional Record, nominations, communications, votes, amendments, members and committees. Each collection is one config with a shared schema. The dataset card lists status and known gaps.

**Objective:** download, parse and publish every document as Parquet, then keep it current with no person or personal device involved. [`pipeline.yml`](.github/workflows/pipeline.yml) runs on GitHub Actions and handles one partition at a time (fetch, write, upload, delete), so the corpus is never stored locally. It writes through Hugging Face Trusted Publishers, so no Hugging Face token is stored.

**Inputs:** Congress.gov API v3 and GovInfo API, which need a free [api.data.gov](https://api.data.gov/signup/) key in `DATA_GOV_API_KEY`; GovInfo bulk data; CRS PDFs from Congress.gov.

**Files:**

- [`ijab/`](ijab/README.md): the pipeline package
- [`tests/`](tests/README.md): offline tests on real samples
- [`.github/workflows/`](.github/workflows/README.md): the schedule
- `pyproject.toml`: pinned dependencies; PDF text also needs poppler's `pdftotext`
- `LICENSE`: MIT, for the code

**Try it:** `pip install .`, then `python -m ijab run --lane congress-crs --local /tmp/out --max-units 5`.
