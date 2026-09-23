"""python -m ijab {run,card,verify,squash}: sync a lane, render the dataset card, check the artifacts, squash history."""

import argparse
import json
import logging
import os
import time

from .collections import BY_NAME, COLLECTIONS, LANES, for_lane
from .http import Fetcher
from .pipeline import Context, sync_collection, utcnow
from .store import SQUASH_AFTER_COMMITS

DEFAULT_REPO = "incrediblecrab/im-just-a-bill"
log = logging.getLogger("ijab")


def open_store(args):
    from .store import HubStore, LocalStore

    return LocalStore(args.local, workdir=args.workdir) if args.local else HubStore(args.repo, workdir=args.workdir)


def selected(args):
    if args.collections:
        names = [name.strip() for name in args.collections.split(",") if name.strip()]
        unknown = sorted(set(names) - set(BY_NAME))
        if unknown:
            raise SystemExit(f"unknown collections: {', '.join(unknown)}")
        return [BY_NAME[name] for name in names]
    if getattr(args, "lane", None):
        return for_lane(args.lane)
    return list(COLLECTIONS)


def cmd_run(args):
    """Each collection gets an equal share of the time still left, so time a collection does not need goes to the rest."""
    collections = selected(args)
    store, fetcher = open_store(args), Fetcher()
    only = frozenset(key.strip() for key in args.partitions.split(",")) if args.partitions else None
    started = time.monotonic()
    lane_end = started + args.budget_minutes * 60
    runs = []
    try:
        for index, collection in enumerate(collections):
            now = time.monotonic()
            if now >= lane_end:
                runs.append({"collection": collection.name, "finished": False, "stopped": "not reached"})
                continue
            deadline = now + (lane_end - now) / (len(collections) - index)
            ctx = Context(fetcher=fetcher, store=store, deadline=deadline, only=only, max_units=args.max_units)
            runs.append(sync_collection(ctx, collection))
        store.commit(f"Run records: {args.lane or ','.join(c.name for c in collections)}")
    finally:
        summary = {"lane": args.lane, "ended": utcnow(), "minutes": round((time.monotonic() - started) / 60, 1),
                   "peak_scratch_bytes": store.peak_bytes, "requests": dict(sorted(fetcher.requests.items())), "runs": runs}
        store.close()
        fetcher.close()
    print(json.dumps(summary, indent=1))
    errors = [run for run in runs if run.get("stopped") not in (None, "budget", "not reached")]
    return 1 if errors else 0


def cmd_card(args):
    from .card import load_manifests, render

    store = open_store(args)
    try:
        text = render(load_manifests(store), store.list_files("data/"))
        if store.read_text("README.md") == text:
            print("card unchanged")
        else:
            store.put_text("README.md", text, "Update dataset card")
            print("card updated")
    finally:
        store.close()
    return 0


def cmd_verify(args):
    from .verify import check

    store = open_store(args)
    fetcher = Fetcher() if args.live else None
    try:
        problems, summaries = check(store, selected(args), fetcher, args.live)
    finally:
        store.close()
        if fetcher:
            fetcher.close()
    print(json.dumps({"problems": problems, "collections": summaries}, indent=1))
    return 1 if problems else 0


def cmd_squash(args):
    """Replaces the repo's history with one commit once it is longer than --min-commits: every sync commits, and a rewritten partition's superseded chunks stay referenced by history until it is squashed."""
    from huggingface_hub import HfApi

    api = HfApi()
    commits = len(api.list_repo_commits(args.repo, repo_type="dataset"))
    if commits <= args.min_commits:
        print(f"{commits} commits; not squashed (threshold {args.min_commits})")
        return 0
    api.super_squash_history(args.repo, repo_type="dataset", commit_message=f"Squash history of {commits} commits ({utcnow()})")
    print(f"squashed {commits} commits in {args.repo}")
    return 0


def main(argv=None):
    parser = argparse.ArgumentParser(prog="ijab", description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)

    def add(name, handler, help_text):
        sub = commands.add_parser(name, help=help_text)
        target = sub.add_mutually_exclusive_group()
        target.add_argument("--repo", default=DEFAULT_REPO, help=f"Hugging Face dataset repo (default {DEFAULT_REPO})")
        target.add_argument("--local", help="use a local directory instead of the Hub (tests and dry runs)")
        sub.add_argument("--workdir", help="parent directory for scratch files (default: system temp)")
        sub.set_defaults(handler=handler)
        return sub

    run = add("run", cmd_run, "sync one lane (or named collections) within a time budget")
    run.add_argument("--lane", choices=LANES)
    run.add_argument("--collections", help="comma-separated collection names (overrides --lane)")
    run.add_argument("--budget-minutes", type=float, default=320.0)
    run.add_argument("--partitions", help="comma-separated partition keys to sync; others are skipped (smoke tests)")
    run.add_argument("--max-units", type=int, help="fetch at most this many units per partition (smoke tests)")
    add("card", cmd_card, "regenerate README.md on the Hub from the manifests")
    verify = add("verify", cmd_verify, "check files against manifests; --live also against source counts")
    verify.add_argument("--collections", help="comma-separated collection names (default: all)")
    verify.add_argument("--live", action="store_true")
    add("squash", cmd_squash, "squash the Hub repo's history into one commit").add_argument(
        "--min-commits", type=int, default=SQUASH_AFTER_COMMITS, help=f"squash only a longer history (default {SQUASH_AFTER_COMMITS})")

    args = parser.parse_args(argv)
    if args.command == "run" and not (args.lane or args.collections):
        parser.error("run needs --lane or --collections")
    if args.command == "squash" and args.local:
        parser.error("squash works on the Hub only")
    if not args.local and os.environ.get("GITHUB_ACTIONS") == "true":
        # Trusted Publishing: huggingface_hub trades the job's OIDC id token for a short-lived token scoped to this repo.
        os.environ.setdefault("HF_OIDC_RESOURCE", f"datasets/{args.repo}")
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    for noisy in ("httpx", "httpcore", "huggingface_hub"):
        logging.getLogger(noisy).setLevel(logging.WARNING)
    from huggingface_hub.utils import disable_progress_bars

    disable_progress_bars()
    return args.handler(args)
