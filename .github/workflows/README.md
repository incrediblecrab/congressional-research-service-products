# workflows

[`pipeline.yml`](pipeline.yml) is the only automation.

**Objective:** keep the dataset current with no personal device, and no person while the repository gets a commit every 60 days.

**Inputs:** the secret `DATA_GOV_API_KEY`, and each job's OIDC token, which Hugging Face exchanges for a short-lived write token once the dataset registers a Trusted Publisher for this repository, `pipeline.yml` and `main`. Until then a sync writes nothing and warns.

**Jobs:**

- `sync`, every 5 minutes and daily: `probe` makes one API request and reads the manifest's state from the card's metadata, not a download. When a sync is needed, `run` syncs within its budget, then `verify --live` and `squash` run if it committed. A dispatch with `args` runs only a bounded test.
- `continue`: when `run` ran out of budget while still fetching, starts the next run, since GitHub starts scheduled runs late or [drops them](https://huggingface.co/datasets/incrediblecrab/congressional-research-service-products#how-it-stays-current). Only this job holds `actions: write`.
- `inactivity`, on every scheduled run: fails after 50 days without a commit, so GitHub notifies the owner before disabling the schedule at 60. It never commits, as GitHub [called](https://github.com/ddev/github-action-add-on-test/issues/46) circumventing that rule a Terms violation.

`.github/` has no README: GitHub would show it instead of the repository README.
