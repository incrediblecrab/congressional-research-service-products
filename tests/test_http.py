"""The real Fetcher against a mock transport: soft 404s, bot challenges, quota exhaustion and missing keys."""

import httpx
import pytest

from ijab.http import Blocked, Fetcher, MissingKey, QuotaExhausted

HOSTS = ("www.govinfo.gov", "www.congress.gov", "api.congress.gov", "api.govinfo.gov")


def fetcher_with(handler, api_key="test-key"):
    fetcher = Fetcher(api_key=api_key, intervals={host: 0 for host in HOSTS})
    fetcher.client.close()
    fetcher.client = httpx.Client(transport=httpx.MockTransport(handler), follow_redirects=True)
    return fetcher


def govinfo(request):
    if request.url.path == "/content/pkg/GONE/html/GONE.htm":
        return httpx.Response(302, headers={"Location": "https://www.govinfo.gov/error"})
    if request.url.path == "/error":
        return httpx.Response(200, text="<html><body>Page Not Found</body></html>")
    return httpx.Response(200, text="real content")


def test_redirect_to_error_page_is_a_404(tmp_path):
    fetcher = fetcher_with(govinfo)
    assert fetcher.get("https://www.govinfo.gov/content/pkg/GONE/html/GONE.htm").status_code == 404
    assert fetcher.get("https://www.govinfo.gov/content/pkg/HERE/html/HERE.htm").status_code == 200


def test_streamed_redirect_to_error_page_is_a_404_and_writes_nothing(tmp_path):
    fetcher = fetcher_with(govinfo)
    dest = tmp_path / "file.zip"
    assert fetcher.get("https://www.govinfo.gov/content/pkg/GONE/html/GONE.htm", stream_to=dest).status_code == 404
    assert not dest.exists()
    response = fetcher.get("https://www.govinfo.gov/content/pkg/HERE/html/HERE.htm", stream_to=dest)
    assert response.status_code == 200 and dest.read_bytes() == b"real content" and len(response.sha256) == 64


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
        fetcher.get("https://api.congress.gov/v3/bill")
    assert len(seen) == 3
    with pytest.raises(QuotaExhausted):
        fetcher.get("https://api.congress.gov/v3/member")
    assert len(seen) == 3, "a dead host gets no further requests"
    assert seen[0].headers["X-Api-Key"] == "test-key"
    assert "test-key" not in str(seen[0].url), "the key travels in a header, never in the URL"


def test_missing_key_kills_only_keyed_hosts():
    seen = []

    def handler(request):
        seen.append(request)
        return httpx.Response(200, json={"files": []})

    fetcher = fetcher_with(handler, api_key="")
    with pytest.raises(MissingKey):
        fetcher.get("https://api.congress.gov/v3/bill")
    assert seen == [] and "api.congress.gov" in fetcher.dead
    assert fetcher.json("https://www.govinfo.gov/bulkdata/json/BILLS") == {"files": []}
