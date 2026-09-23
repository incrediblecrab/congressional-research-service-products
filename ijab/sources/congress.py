"""Congress.gov API v3 (needs DATA_GOV_API_KEY): one generic adapter configured per endpoint, plus CRS report text."""

import json
import re
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

from .. import text as T
from ..http import Blocked
from ..pipeline import Adapter, Partition, Unit, age_hours, dumps, utcnow
from .govinfo import BILL_SLUGS, latest_summary, package_text, sha256

API = "https://api.congress.gov/v3"
PAGE = 250
OVERLAP = timedelta(hours=26)
RELIST_HOURS = 30 * 24


def current_congress(today=None):
    today = today or datetime.now(timezone.utc)
    return (today.year - 1789) // 2 + 1


def strip_query(url):
    return url.split("?", 1)[0] if url else url


def day(value):
    return value[:10] if isinstance(value, str) and value else None


def as_int(value):
    return int(value) if str(value if value is not None else "").isdigit() else None


def first_list(data):
    """The payload list of an API response: a top-level list, or the first list inside a top-level object."""
    for key, value in (data or {}).items():
        if key in ("pagination", "request"):
            continue
        if isinstance(value, list):
            return value
        if isinstance(value, dict):
            inner = next((v for v in value.values() if isinstance(v, list)), None)
            if inner is not None:
                return inner
    return []


@dataclass(frozen=True)
class Spec:
    path: str
    item_key: str
    detail_key: str | None
    unit_id: object
    row: object
    per_congress: bool = False
    first_congress: int = 93
    last_congress: int | None = None
    since: bool = False
    sub: tuple = ()
    partition: object = None


class ApiAdapter(Adapter):
    """List an endpoint (per Congress or whole), then fetch each new or changed item's detail and sub-lists.

    Per-Congress collections that support fromDateTime get an incremental pass first; every complete partition is still relisted in full once RELIST_HOURS have passed, which is what catches deletions.
    """

    spec: Spec = None

    def pages(self, path, params=None):
        offset = 0
        while True:
            data = self.ctx.fetcher.json(f"{API}{path}", params={"format": "json", "limit": PAGE, "offset": offset, **(params or {})})
            items = (data or {}).get(self.spec.item_key) or []
            yield from items
            offset += len(items)
            if not items or offset >= int(((data or {}).get("pagination") or {}).get("count") or 0):
                return

    def count(self, path):
        data = self.ctx.fetcher.json(f"{API}{path}", params={"format": "json", "limit": 1}) or {}
        return int((data.get("pagination") or {}).get("count") or 0)

    def key_of(self, item):
        if self.spec.partition:
            return self.spec.partition(item)
        return str(item["congress"]) if self.spec.per_congress else "all"

    def group(self, items):
        groups = defaultdict(dict)
        for item in items:
            uid = self.spec.unit_id(item)
            groups[self.key_of(item)][uid] = Unit(uid, item.get("updateDate"), {"item": item})
        return groups

    def congresses(self):
        now = current_congress()
        return now, min(now, self.spec.last_congress or now)

    def plan(self, manifest):
        spec = self.spec
        if not spec.per_congress:
            groups = self.group(self.pages(spec.path))
            for key in sorted(groups):
                yield Partition(key, groups[key])
            return
        started = utcnow()
        now, top = self.congresses()
        if spec.since and manifest.get("watermark"):
            since = datetime.strptime(manifest["watermark"], "%Y-%m-%dT%H:%M:%SZ") - OVERLAP
            groups = self.group(self.pages(spec.path, {"fromDateTime": since.strftime("%Y-%m-%dT%H:%M:%SZ"), "toDateTime": started}))
            for key in sorted(groups, key=int, reverse=True):
                if spec.first_congress <= int(key) <= top:
                    yield Partition(key, groups[key], complete_listing=False)
        if spec.since:
            manifest["watermark"] = started
        empty = manifest.setdefault("empty_partitions", {})
        for congress in range(top, spec.first_congress - 1, -1):
            key = str(congress)
            if not self.wanted(key):
                continue
            entry = manifest["partitions"].get(key) or {}
            recent = congress >= now - 1 and not spec.since
            if entry.get("complete") and not recent and age_hours(entry.get("listed_at")) < RELIST_HOURS:
                continue
            if key in empty and age_hours(empty[key]) < RELIST_HOURS:
                continue
            units = self.group(self.pages(f"{spec.path}/{congress}")).get(key, {})
            if not units and not entry:
                if congress < now:
                    empty[key] = utcnow()
                continue
            empty.pop(key, None)
            yield Partition(key, units)

    def live_counts(self, keys):
        """Source counts from the API's own pagination totals, for verify."""
        if self.spec.partition:
            return {"*": self.count(self.spec.path)}
        if not self.spec.per_congress:
            return {"all": self.count(self.spec.path)}
        return {key: self.count(f"{self.spec.path}/{key}") for key in keys}

    def detail(self, unit):
        item = unit.info["item"]
        url = strip_query(item["url"])
        if not self.spec.detail_key:
            return url, dict(item)
        entity = (self.ctx.fetcher.json(url, params={"format": "json"}) or {}).get(self.spec.detail_key)
        if isinstance(entity, list):
            entity = next((e for e in entity if isinstance(e, dict) and self.spec.unit_id(dict(item, **e)) == unit.id), entity[0] if entity else None)
        if not isinstance(entity, dict):
            raise RuntimeError(f"no {self.spec.detail_key} at {url.removeprefix(API)}")
        for name in self.spec.sub:
            ref = entity.get(name)
            if isinstance(ref, dict) and "url" in ref:
                entity[name] = self.sub_list(strip_query(ref["url"])) if ref.get("count") != 0 else []
            elif ref is None:
                entity[name] = self.sub_list(f"{url}/{name}")
        return url, entity

    def sub_list(self, url):
        out, offset = [], 0
        while True:
            data = self.ctx.fetcher.json(url, params={"format": "json", "limit": PAGE, "offset": offset}) or {}
            items = first_list(data)
            out.extend(items)
            offset += len(items)
            if not items or offset >= int((data.get("pagination") or {}).get("count") or 0):
                return out

    def fetch(self, unit):
        url, entity = self.detail(unit)
        row = self.spec.row(entity, unit.info["item"])
        row["chamber"] = T.chamber_of(row.get("chamber"))
        row.setdefault("url", url)
        row["metadata"] = dumps(entity)
        return [row]


def treaty_id(t):
    return f"{t.get('congressReceived')}-{t.get('number')}{t.get('suffix') or ''}"


def treaty_row(t, item):
    titles = t.get("titles") or []
    formal = next((x.get("title") for x in titles if "Formal" in (x.get("titleType") or "")), None)
    short = next((x.get("title") for x in titles if "Short" in (x.get("titleType") or "")), None)
    html = t.get("resolutionText")
    return {"id": treaty_id(t), "congress": as_int(t.get("congressReceived")), "type": "treaty",
            "number": f"{t.get('number')}{t.get('suffix') or ''}", "chamber": "Senate",
            "title": short or formal or t.get("topic"), "date": day(t.get("transmittedDate")),
            "text": T.html_text(html) if html else None, "text_source": "resolution-text" if html else None}


def nomination_title(n, item):
    """The description; list nominations (mostly military) have none, so their nominee groups stand in."""
    if n.get("description") or item.get("description"):
        return n.get("description") or item.get("description")
    groups = [", ".join(str(x) for x in (g.get("organization"), g.get("positionTitle")) if x) + (f", {g['nomineeCount']} nominee(s)" if g.get("nomineeCount") else "")
              for g in n.get("nominees") or [] if isinstance(g, dict)]
    return "; ".join(g for g in groups if g) or None


def nomination_row(n, item):
    return {"id": f"{n.get('congress')}-{n.get('citation')}", "congress": as_int(n.get("congress")),
            "type": "civilian" if (n.get("nominationType") or {}).get("isCivilian") else "military",
            "number": n.get("citation"), "version": n.get("partNumber"), "chamber": "Senate", "title": nomination_title(n, item),
            "date": n.get("receivedDate")}


def member_row(m, item):
    terms = m.get("terms") or []
    terms = terms.get("item", []) if isinstance(terms, dict) else terms
    last = max((t for t in terms if isinstance(t, dict)), key=lambda t: (t.get("congress") or 0, t.get("startYear") or 0), default={})
    return {"id": m.get("bioguideId"), "congress": as_int(last.get("congress")), "type": (last.get("memberType") or "").lower() or None,
            "chamber": last.get("chamber"),
            "title": m.get("directOrderName") or item.get("name"), "date": str(last.get("startYear")) if last.get("startYear") else None}


def committee_row(c, item):
    history = [h for h in c.get("history") or [] if isinstance(h, dict)]
    return {"id": c.get("systemCode") or item.get("systemCode"), "type": (c.get("type") or item.get("committeeTypeCode") or "").lower() or None,
            "chamber": item.get("chamber"), "title": (history[0].get("officialName") if history else None) or item.get("name"),
            "date": day(history[-1].get("startDate")) if history else None}


def meeting_row(m, item):
    return {"id": f"{m.get('congress')}-{(m.get('chamber') or '').lower()}-{m.get('eventId')}", "congress": as_int(m.get("congress")),
            "type": (m.get("type") or "").lower() or None, "number": str(m.get("eventId")), "chamber": m.get("chamber"),
            "title": m.get("title"), "date": day(m.get("date"))}


def communication_id(c):
    return f"{c.get('congress')}-{((c.get('communicationType') or {}).get('code') or '').lower()}-{c.get('number')}"


def communication_row(c, item):
    abstract = c.get("abstract")
    return {"id": communication_id(c), "congress": as_int(c.get("congress")), "type": ((c.get("communicationType") or {}).get("code") or "").lower() or None,
            "number": str(c.get("number")), "chamber": c.get("chamber"), "title": (abstract or "")[:300] or None,
            "date": c.get("congressionalRecordDate"), "text": abstract, "text_source": "abstract" if abstract else None}


def requirement_row(r, item):
    return {"id": str(r.get("number")), "type": "requirement", "number": str(r.get("number")), "chamber": "House", "title": r.get("nature")}


def vote_id(v):
    return f"{v.get('congress')}-{v.get('sessionNumber')}-{v.get('rollCallNumber')}"


def vote_row(v, item):
    measure = " ".join(str(x) for x in (v.get("legislationType"), v.get("legislationNumber")) if x)
    return {"id": vote_id(v), "congress": as_int(v.get("congress")), "type": (v.get("voteType") or "").lower() or None,
            "number": str(v.get("rollCallNumber")), "version": str(v.get("sessionNumber")), "chamber": "House",
            "title": " - ".join(x for x in (v.get("voteQuestion"), measure, v.get("result")) if x) or None, "date": day(v.get("startDate"))}


def amendment_id(a):
    return f"{a.get('congress')}-{(a.get('type') or '').lower()}-{a.get('number')}"


def amendment_row(a, item):
    amended = a.get("amendedBill") or {}
    target = " ".join(str(x) for x in (amended.get("type"), amended.get("number")) if x)
    title = a.get("purpose") or a.get("description") or (f"Amendment to {target}: {amended.get('title')}" if target else None)
    return {"id": amendment_id(a), "congress": as_int(a.get("congress")), "type": (a.get("type") or "").lower() or None,
            "number": str(a.get("number")), "chamber": a.get("chamber"), "title": title,
            "date": day(a.get("submittedDate") or a.get("proposedDate"))}


def congress_number(item):
    return strip_query(item.get("url") or "").rstrip("/").rsplit("/", 1)[-1] or re.match(r"\d+", item.get("name") or "").group(0)


def congress_row(c, item):
    number = congress_number(c)
    return {"id": number, "congress": as_int(number), "type": "congress", "title": c.get("name"),
            "date": next((s.get("startDate") for s in sorted(c.get("sessions") or [], key=lambda s: s.get("startDate") or "") if s.get("startDate")), c.get("startYear"))}


def bill_id(b):
    return f"{b.get('congress')}-{(b.get('type') or '').lower()}-{b.get('number')}"


def legacy_bill_row(b, item):
    kind = (b.get("type") or "").lower()
    congress, number = as_int(b.get("congress")), str(b.get("number"))
    summary = latest_summary(b.get("summaries"))
    return {"id": bill_id(b), "congress": congress, "type": kind, "number": number, "chamber": b.get("originChamber"),
            "title": b.get("title"), "date": b.get("introducedDate"), "url": f"https://www.congress.gov/bill/{congress}/{BILL_SLUGS.get(kind, kind)}/{number}",
            "text": summary, "text_source": "crs-summary" if summary else None}


def configured(name, **kwargs):
    return type(name, (ApiAdapter,), {"spec": Spec(**kwargs)})


Members = configured("Members", path="/member", item_key="members", detail_key="member", unit_id=lambda i: i["bioguideId"], row=member_row)
Committees = configured("Committees", path="/committee", item_key="committees", detail_key="committee", unit_id=lambda i: i["systemCode"], row=committee_row)
Congresses = configured("Congresses", path="/congress", item_key="congresses", detail_key=None, unit_id=congress_number, row=congress_row)
HouseRequirements = configured("HouseRequirements", path="/house-requirement", item_key="houseRequirements", detail_key="houseRequirement",
                               unit_id=lambda i: str(i["number"]), row=requirement_row)
Nominations = configured("Nominations", path="/nomination", item_key="nominations", detail_key="nomination", per_congress=True, since=True,
                         sub=("actions",), unit_id=lambda i: f"{i['congress']}-{i['citation']}", row=nomination_row)
CommitteeMeetings = configured("CommitteeMeetings", path="/committee-meeting", item_key="committeeMeetings", detail_key="committeeMeeting",
                               per_congress=True, since=True, unit_id=lambda i: f"{i['congress']}-{(i.get('chamber') or '').lower()}-{i['eventId']}", row=meeting_row)
HouseCommunications = configured("HouseCommunications", path="/house-communication", item_key="houseCommunications", detail_key="houseCommunication",
                                 per_congress=True, unit_id=communication_id, row=communication_row)
SenateCommunications = configured("SenateCommunications", path="/senate-communication", item_key="senateCommunications", detail_key="senateCommunication",
                                  per_congress=True, unit_id=communication_id, row=communication_row)
HouseVotes = configured("HouseVotes", path="/house-vote", item_key="houseRollCallVotes", detail_key="houseRollCallVote", per_congress=True,
                        sub=("members",), unit_id=vote_id, row=vote_row)
Amendments = configured("Amendments", path="/amendment", item_key="amendments", detail_key="amendment", per_congress=True, since=True,
                        unit_id=amendment_id, row=amendment_row)
LegacyBills = configured("LegacyBills", path="/bill", item_key="bills", detail_key="bill", per_congress=True, last_congress=107,
                         sub=("summaries",), unit_id=bill_id, row=legacy_bill_row)


class Treaties(ApiAdapter):
    """Treaty metadata from the API; text is the Senate Treaty Document on GovInfo when one exists, else the resolution of ratification."""

    spec = Spec(path="/treaty", item_key="treaties", detail_key="treaty", sub=("actions", "committees"), unit_id=treaty_id, row=treaty_row)

    def fetch(self, unit):
        rows = super().fetch(unit)
        row = rows[0]
        number = re.match(r"\d+", str(row["number"] or ""))
        if row["congress"] and number:
            package = f"CDOC-{row['congress']}tdoc{number.group(0)}"
            text, source, link, digest, _ = package_text(self.ctx.fetcher, package)
            if text:
                row.update({"text": text, "text_source": source, "text_url": link, "text_sha256": digest})
        return rows


def crs_prefix(item):
    match = re.match(r"[A-Z]+", item.get("id") or "")
    return match.group(0) if match else "other"


def crs_row(r, item):
    return {"id": r.get("id"), "type": (r.get("contentType") or "").lower() or None, "number": r.get("id"),
            "version": str(r.get("currentVersion") or item.get("version") or "") or None,
            "title": r.get("title"), "date": day(r.get("publishDate"))}


class CrsReports(ApiAdapter):
    """CRS products: metadata from the API, text from the PDF the API links to, or the HTML rendition when there is no PDF.

    PDF comes first because www.congress.gov puts its HTML renditions behind a Cloudflare bot challenge (measured 09/23/2026: the HTML answered 403 with cf-mitigated: challenge while the PDF at the API's URL answered 200).
    """

    spec = Spec(path="/crsreport", item_key="CRSReports", detail_key="CRSReport", unit_id=lambda i: i["id"], row=crs_row, partition=crs_prefix)

    def fetch(self, unit):
        rows = super().fetch(unit)
        entity = json.loads(rows[0]["metadata"])
        formats = {(f.get("format") or "").upper(): f.get("url") for f in entity.get("formats") or [] if isinstance(f, dict) and f.get("url")}
        text = source = link = digest = None
        if formats.get("PDF"):
            response = self.ctx.fetcher.get(formats["PDF"])
            if response.status_code == 200 and response.content[:5] == b"%PDF-":
                text, source, link, digest = T.pdf_text(response.content), "crs-pdf", formats["PDF"], sha256(response.content)
            elif response.status_code != 404:
                response.raise_for_status()
        if not text and formats.get("HTML"):
            try:
                response = self.ctx.fetcher.get(formats["HTML"])
            except Blocked as error:
                raise RuntimeError(f"no PDF text, and the HTML is behind a bot challenge ({error})") from None
            if response.status_code == 200:
                text, source, link, digest = T.html_text(response.content), "crs-html", formats["HTML"], sha256(response.content)
            elif response.status_code != 404:
                response.raise_for_status()
        if not text:
            source = link = digest = None
        if formats and not text:
            raise RuntimeError(f"no text in {sorted(formats)} for {unit.id}")
        rows[0].update({"text": text, "text_source": source, "text_url": link, "text_sha256": digest})
        return rows
