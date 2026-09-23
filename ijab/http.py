"""Polite HTTP client: one pacing clock per host, bounded retries, no secrets in URLs or logs."""

import hashlib
import os
import threading
import time
from pathlib import Path
from urllib.parse import urlsplit

import httpx

USER_AGENT = "im-just-a-bill/0.1 (+https://github.com/incrediblecrab/im-just-a-bill)"

# Minimum seconds between requests to each host within one process (one lane).
# api.congress.gov documents 5,000 requests/hour per key; 0.9 s keeps the congress-api lane near 4,000/hour.
# www.congress.gov is a Library of Congress site: loc.gov/legal asks for at most 10 requests/minute.
# api.govinfo.gov documents 36,000 requests/hour. www.govinfo.gov publishes no limit; 0.3 s per lane keeps the three GovInfo lanes together at 10 requests/second, the same 36,000/hour.
HOST_INTERVAL = {
    "api.congress.gov": 0.9,
    "www.congress.gov": 6.0,
    "congress.gov": 6.0,
    "api.govinfo.gov": 0.2,
    "www.govinfo.gov": 0.3,
}
DEFAULT_INTERVAL = 1.0
KEYED_HOSTS = {"api.congress.gov", "api.govinfo.gov"}


class MissingKey(RuntimeError):
    """An api.data.gov key is required for this host and DATA_GOV_API_KEY is not set."""


class QuotaExhausted(RuntimeError):
    """The host keeps answering 429; the caller should stop using it for this run."""


class Blocked(RuntimeError):
    """A bot challenge (for example Cloudflare's) answered instead of the resource."""


class Fetcher:
    def __init__(self, api_key=None, intervals=None, max_retries=5, timeout=120.0):
        self.api_key = api_key if api_key is not None else os.environ.get("DATA_GOV_API_KEY") or None
        self.intervals = dict(HOST_INTERVAL, **(intervals or {}))
        self.max_retries = max_retries
        self.client = httpx.Client(headers={"User-Agent": USER_AGENT}, timeout=timeout, follow_redirects=True)
        self._next_at = {}
        self._lock = threading.Lock()
        self.requests = {}
        # A host that ran out of quota or needs a missing key stays unusable for the rest of the run. A bot challenge does not kill the host: www.congress.gov challenges CRS HTML pages but serves the PDFs.
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

    def get(self, url, params=None, headers=None, stream_to=None):
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
                if stream_to is not None:
                    return self._stream(url, params, merged, stream_to)
                response = self.client.get(url, params=params, headers=merged)
            except (httpx.TransportError, _RetryableStatus) as error:
                last_error = error
                time.sleep(min(300, 2 ** (attempt + 2)))
                continue
            if _soft_404(response):
                return httpx.Response(404, request=response.request)
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
                last_error = RuntimeError(f"HTTP {status} from {host}{urlsplit(url).path}")
                time.sleep(min(300, 2 ** (attempt + 2)))
                continue
            return response
        raise last_error

    def _stream(self, url, params, headers, dest):
        dest = Path(dest)
        digest = hashlib.sha256()
        with self.client.stream("GET", url, params=params, headers=headers) as response:
            if _soft_404(response):
                return httpx.Response(404, request=response.request)
            if response.status_code == 429 or response.status_code >= 500:
                raise _RetryableStatus(response.status_code)
            if response.status_code != 200:
                return response
            with dest.open("wb") as handle:
                for chunk in response.iter_bytes(1 << 20):
                    handle.write(chunk)
                    digest.update(chunk)
        response.sha256 = digest.hexdigest()
        return response

    def json(self, url, params=None):
        response = self.get(url, params=params, headers={"Accept": "application/json"})
        if response.status_code == 404:
            return None
        response.raise_for_status()
        return response.json()

    def close(self):
        self.client.close()


class _RetryableStatus(Exception):
    pass


def _soft_404(response):
    """www.govinfo.gov answers a missing file with a redirect to /error and a 200 "Page Not Found" page."""
    return bool(response.history) and urlsplit(str(response.url)).path.rstrip("/") == "/error"


def _retry_after(response, default):
    value = response.headers.get("Retry-After", "")
    return min(3600, int(value)) if value.isdigit() else default
