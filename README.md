# congressional-research-service-products

This repository builds and updates [congressional-research-service-products](https://huggingface.co/datasets/incrediblecrab/congressional-research-service-products), a public Hugging Face dataset of every Congressional Research Service (CRS) product the Congress.gov API lists: reports, posts, resources, testimony and infographics, active and archived, with full text. The API listed 14,141 on September 23, 2026; the dataset card shows how many are stored.

**Objective:** hold every CRS product and add new and updated ones automatically, with no person, personal device or local copy of the corpus. [`pipeline.yml`](.github/workflows/pipeline.yml) probes the API every 5 minutes on GitHub Actions and syncs when the listing changed. It writes through Hugging Face Trusted Publishing, so no Hugging Face token is stored.

**Inputs:** the [Congress.gov API](https://api.congress.gov) (`/v3/crsreport`), which needs a free [api.data.gov](https://api.data.gov/signup/) key in `DATA_GOV_API_KEY`, and the PDF and HTML renditions on www.congress.gov.

**Files:**

- [`crs_products/`](crs_products/README.md): the pipeline package
- [`tests/`](tests/README.md): offline tests on real samples
- [`.github/workflows/`](.github/workflows/README.md): the schedule
- `pyproject.toml`: pinned dependencies; text extraction also needs poppler's `pdftotext`
- `LICENSE`: MIT, for the code

**Try it:** `pip install .`, then `python -m crs_products run --local /tmp/out --partitions TE10 --max-units 3`.
