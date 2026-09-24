# workflows

[`pipeline.yml`](pipeline.yml) is the only automation. It runs on its cron schedule, or by hand from the Actions tab.

**Objective:** keep the dataset current without a person or a personal device.

**Inputs:** the repository secret `DATA_GOV_API_KEY`, and each job's OIDC id token, which Hugging Face exchanges for a short-lived write token. The exchange works only once the dataset has a Trusted Publisher for this repository, workflow `pipeline.yml` and branch `main`; until then a sync writes nothing and ends with a warning, not a failure.

**Jobs:**

- `sync`, every 5 minutes: `probe` makes one API request and reads the manifest. Only when it says a sync is needed does `run` sync within its budget, followed by `verify --live` and `squash` if it committed. A dispatch with `args` runs a bounded test instead, without probe, verify or squash.
- `keepalive`, daily: makes an empty commit once the repository has had none for 45 days, because GitHub disables schedules after 60 days without activity.

`.github/` itself has no README, because GitHub would display it in place of the repository README.
