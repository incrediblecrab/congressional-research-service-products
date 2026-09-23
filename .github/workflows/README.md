# workflows

[`pipeline.yml`](pipeline.yml) is the only automation. It runs on its cron schedule, or by hand from the Actions tab.

**Objective:** keep the dataset current without a person or a personal device.

**Inputs:** the repository secret `DATA_GOV_API_KEY`, and each job's OIDC id token, which Hugging Face exchanges for a short-lived write token. The exchange works only once the dataset has a Trusted Publisher for this repository, branch `main` and workflow `pipeline.yml`.

**Jobs:**

- `lane`: one parallel job per lane, each spending its own rate limit and syncing its collections within the time budget.
- `publish`: regenerates the dataset card, runs `verify --live` and squashes long history. Runs that set `args` (bounded tests) skip it.
- `keepalive`: makes an empty commit once the repository has had none for 45 days, because GitHub disables schedules after 60 days without activity.

`.github/` itself has no README, because GitHub would display it in place of the repository README.
