# workflows

**Objective:** keep the three datasets current with no personal device, and no person while the repository gets a commit every 60 days.

**Inputs:** the secret `DATA_GOV_API_KEY`, and each job's OIDC token, which Hugging Face exchanges for a short-lived write token once a dataset registers a Trusted Publisher for this repository, its workflow file and `main`. Until then a run writes nothing and warns.

**Files:**

- [`pipeline.yml`](pipeline.yml), products: every 5 minutes, `probe` makes one API request and reads state from the card's metadata, not a download. When needed, `run` syncs; `verify --live` and `squash` follow a commit. `continue` starts the next run when one ran out of budget mid-fetch, as GitHub starts scheduled runs late or [drops them](https://huggingface.co/datasets/incrediblecrab/congressional-research-service-products#how-it-stays-current). `inactivity` fails after 50 days without a commit, before GitHub disables every schedule here at 60. It makes no keepalive commit: GitHub [called](https://github.com/ddev/github-action-add-on-test/issues/46) that a Terms violation.
- [`summaries.yml`](summaries.yml), bill summaries: every 6 hours, `run`, then `verify` and `squash` after a commit; `continue` as above.
- [`constitution.yml`](constitution.yml), Constitution Annotated: weekly, `run`, then `verify --live` and `squash` after a commit.

A dispatch with `args` runs only a bounded test. `.github/` has no README, which GitHub would show instead of the repository's.
