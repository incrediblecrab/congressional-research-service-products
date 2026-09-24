# workflows

[`pipeline.yml`](pipeline.yml) is the only automation.

**Objective:** keep the dataset current with no person or personal device.

**Inputs:** the repository secret `DATA_GOV_API_KEY`, and each job's OIDC id token, which Hugging Face exchanges for a short-lived write token. It works only once the dataset registers a Trusted Publisher for this repository, workflow `pipeline.yml` and branch `main`; until then a sync writes nothing and ends with a warning.

**Jobs:**

- `sync`, scheduled every 5 minutes and daily: `probe` makes one API request and reads the manifest's state from the card's metadata, which is not a download. Only when it says a sync is needed does `run` sync within its budget, then `verify --live` and `squash` if it committed. A dispatch with `args` runs a bounded test instead, with no probe, verify, squash or `continue`.
- `continue`: when `run` ran out of budget while still fetching, starts the next run at once, since GitHub starts scheduled runs late or [drops them](https://huggingface.co/datasets/incrediblecrab/congressional-research-service-products#how-it-stays-current). Only this job holds `actions: write`.
- `keepalive`, daily: commits once the repository has been idle 45 days, because GitHub disables schedules after 60.

`.github/` has no README: GitHub would show it instead of the repository README.
