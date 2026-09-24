"""Polite HTTP client: one pacing clock per host, bounded retries, no secrets in URLs or logs."""

import os
import threading
import time
from urllib.parse import urlsplit

import httpx

USER_AGENT = "congressional-research-service-products/0.2 (+https://github.com/incrediblecrab/congressional-research-service-products)"

# Minimum seconds between requests to each host within one process.
# api.congress.gov documents 5,000 requests/hour per key; 0.9 s keeps a run near 4,000/hour.
# www.congress.gov is a Library of Congress site: loc.gov/legal asks for at most 10 requests/minute "regardless of the number of machines", which is why only one writer runs at a time (the lease in pipeline.py).
HOST_INTERVAL = {"api.congress.gov": 0.9, "www.congress.gov": 6.0, "congress.gov": 6.0}
DEFAULT_INTERVAL = 1.0
KEYED_HOSTS = {"api.congress.gov"}


class MissingKey(RuntimeError):
    """An api.data.gov key is required for this host and DATA_GOV_API_KEY is not set."""


class QuotaExhausted(RuntimeError):
    """The host keeps answering 429; the caller should stop using it for this run."""


class Blocked(RuntimeError):
    """A bot challenge (for example Cloudflare's) answered instead of the resource."""


class Unavailable(RuntimeError):
    """Server errors or network failures outlasted every retry."""


class Fetcher:
    def __init__(self, api_key=None, intervals=None, max_retries=5, timeout=120.0):
        self.api_key = api_key if api_key is not None else os.environ.get("DATA_GOV_API_KEY") or None
        self.intervals = dict(HOST_INTERVAL, **(intervals or {}))
        self.max_retries = max_retries
        self.client = httpx.Client(headers={"User-Agent": USER_AGENT}, timeout=timeout, follow_redirects=True)
        self._next_at = {}
        self._lock = threading.Lock()
        self.requests = {}
        # A host that ran out of quota or needs a missing key stays unusable for the rest of the run. A bot challenge does not kill the host: www.congress.gov has challenged CRS HTML pages while serving the PDFs.
        self.dead = {}

    def _pace(self, host):
        interval = self.intervals.get(host, DEFAULT_INTERVAL)
        with self._lock:
            now = time.monotonic()
            at = max(now, self._next_at.get(host, 0.0))
            self._next_at[host] = at + interval
        if at > now:
            time.sleep(at - now)

    def _headers(self, host, headers):
        merged = dict(headers or {})
        if host in KEYED_HOSTS:
            if not self.api_key:
                raise self._kill(host, MissingKey(f"{host} needs DATA_GOV_API_KEY"))
            merged["X-Api-Key"] = self.api_key
        return merged

    def _kill(self, host, error):
        self.dead[host] = error
        return error

    def get(self, url, params=None, headers=None):
        """GET with pacing and retries. Returns the response; 404 is returned, not raised."""
        host = urlsplit(url).hostname or ""
        if host in self.dead:
            raise self.dead[host]
        merged = self._headers(host, headers)
        last_error = None
        for attempt in range(self.max_retries + 1):
            self._pace(host)
            self.requests[host] = self.requests.get(host, 0) + 1
            try:
                response = self.client.get(url, params=params, headers=merged)
            except httpx.TransportError as error:
                last_error = Unavailable(f"{type(error).__name__} from {host}{urlsplit(url).path}")
                time.sleep(min(300, 2 ** (attempt + 2)))
                continue
            status = response.status_code
            if status == 403 and b"Just a moment" in response.content[:4096]:
                raise Blocked(f"bot challenge at {host}{urlsplit(url).path}")
            if status == 429:
                last_error = QuotaExhausted(f"429 from {host}")
                if attempt >= 2:
                    raise self._kill(host, last_error)
                time.sleep(_retry_after(response, default=60 * (attempt + 1)))
                continue
            if status >= 500:
                last_error = Unavailable(f"HTTP {status} from {host}{urlsplit(url).path}")
                time.sleep(min(300, 2 ** (attempt + 2)))
                continue
            return response
        raise last_error

    def json(self, url, params=None):
        response = self.get(url, params=params, headers={"Accept": "application/json"})
        if response.status_code == 404:
            return None
        response.raise_for_status()
        return response.json()

    def close(self):
        self.client.close()


def _retry_after(response, default):
    value = response.headers.get("Retry-After", "")
    return min(3600, int(value)) if value.isdigit() else default
