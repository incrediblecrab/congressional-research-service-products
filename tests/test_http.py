"""The real Fetcher: its pacing on a fake clock, and against a mock transport, bot challenges, quota exhaustion, missing keys and server errors."""

import httpx
import pytest

from crs_products import http as http_module
from crs_products.http import Blocked, Fetcher, MissingKey, QuotaExhausted, Unavailable

HOSTS = ("www.congress.gov", "api.congress.gov")


@pytest.fixture(autouse=True)
def no_sleep(monkeypatch):
    monkeypatch.setattr(http_module.time, "sleep", lambda seconds: None)


def fetcher_with(handler, api_key="test-key", max_retries=5):
    fetcher = Fetcher(api_key=api_key, intervals={host: 0 for host in HOSTS}, max_retries=max_retries)
    fetcher.client.close()
    fetcher.client = httpx.Client(transport=httpx.MockTransport(handler), follow_redirects=True)
    return fetcher


@pytest.mark.parametrize("host", ["www.congress.gov", "congress.gov"])
def test_eleven_requests_to_a_library_of_congress_host_span_more_than_a_minute(monkeypatch, host):
    # Requests leave a little after their slots, so ten intervals must leave a margin over 60 seconds, not merely reach it.
    clock = [1000.0]
    monkeypatch.setattr(http_module.time, "monotonic", lambda: clock[0])
    monkeypatch.setattr(http_module.time, "sleep", lambda seconds: clock.__setitem__(0, clock[0] + seconds))
    fetcher = Fetcher(api_key="test-key")
    sends = []
    for _ in range(11):
        fetcher._pace(host)
        sends.append(clock[0])
    fetcher.client.close()
    assert sends[-1] - sends[0] >= 60.5


def test_bot_challenge_raises_blocked_without_killing_the_host():
    def handler(request):
        if request.url.path.endswith(".html"):
            return httpx.Response(403, headers={"cf-mitigated": "challenge"}, text="<!DOCTYPE html><title>Just a moment...</title>")
        return httpx.Response(200, content=b"%PDF-1.7")

    fetcher = fetcher_with(handler)
    with pytest.raises(Blocked):
        fetcher.get("https://www.congress.gov/crs_external_products/IN/HTML/IN12740.html")
    assert "www.congress.gov" not in fetcher.dead
    assert fetcher.get("https://www.congress.gov/crs_external_products/IN/PDF/IN12740/IN12740.3.pdf").status_code == 200


def test_repeated_429_exhausts_the_host_and_later_calls_fail_fast():
    seen = []

    def handler(request):
        seen.append(request)
        return httpx.Response(429, headers={"Retry-After": "0"})

    fetcher = fetcher_with(handler)
    with pytest.raises(QuotaExhausted):
        fetcher.get("https://api.congress.gov/v3/crsreport")
    assert len(seen) == 3
    with pytest.raises(QuotaExhausted):
        fetcher.get("https://api.congress.gov/v3/crsreport/R40001")
    assert len(seen) == 3, "a dead host gets no further requests"
    assert seen[0].headers["X-Api-Key"] == "test-key"
    assert "test-key" not in str(seen[0].url), "the key travels in a header, never in the URL"


def test_missing_key_kills_only_the_keyed_host():
    seen = []

    def handler(request):
        seen.append(request)
        return httpx.Response(200, content=b"%PDF-1.7")

    fetcher = fetcher_with(handler, api_key="")
    with pytest.raises(MissingKey):
        fetcher.json("https://api.congress.gov/v3/crsreport")
    assert seen == []
    assert fetcher.get("https://www.congress.gov/crs_external_products/IN/PDF/IN12740/IN12740.3.pdf").status_code == 200


@pytest.mark.parametrize("failure", ["status", "transport"])
def test_server_errors_and_network_failures_become_unavailable_after_retries(failure):
    seen = []

    def handler(request):
        seen.append(request)
        if failure == "transport":
            raise httpx.ConnectError("connection refused", request=request)
        return httpx.Response(503)

    fetcher = fetcher_with(handler, max_retries=2)
    with pytest.raises(Unavailable):
        fetcher.get("https://www.congress.gov/crs_external_products/IN/PDF/IN12740/IN12740.3.pdf")
    assert len(seen) == 3
    assert "www.congress.gov" not in fetcher.dead, "an outage is not remembered: the next product tries again"


def test_a_404_is_returned_and_json_turns_it_into_none():
    fetcher = fetcher_with(lambda request: httpx.Response(404, json={"error": "not found"}))
    assert fetcher.get("https://api.congress.gov/v3/crsreport/R99999").status_code == 404
    assert fetcher.json("https://api.congress.gov/v3/crsreport/R99999") is None
