"""Every collection in the dataset: its lane (the GitHub Actions job that owns it), adapter, source and text meaning.

Lanes split the work by rate-limit budget, so they can run in parallel without sharing a quota: govinfo-* lanes use www.govinfo.gov (bulk data and content) plus light api.govinfo.gov listing; congress-api uses api.congress.gov at up to ~4,000 requests/hour; congress-crs uses www.congress.gov at 10 requests/minute plus about one api.congress.gov request per changed report. The two api.congress.gov lanes together stay under 5,000/hour.
"""

from dataclasses import dataclass

from .sources import congress as C
from .sources import govinfo as G


@dataclass(frozen=True)
class Collection:
    name: str
    lane: str
    adapter: type
    source: str
    text: str
    unit: str = "document"


COLLECTIONS = [
    Collection("bills", "govinfo-bulk", G.BillStatus, "GovInfo BILLSTATUS bulk XML, 108th Congress to present",
               "Latest CRS summary of the bill (HTML converted to text); full bill status record in metadata", "bill"),
    Collection("bill_text", "govinfo-bulk", G.BillTextAll, "GovInfo BILLS: bulk XML for the 113th Congress on, package HTML for the 103rd-112th",
               "Full text of one version of a bill or resolution", "bill version"),
    Collection("laws", "govinfo-bulk", G.LawsAll, "GovInfo PLAW: USLM bulk XML for the 113th Congress on, package HTML or PDF for the 104th-112th",
               "Full text of the public or private law as enacted", "law"),
    Collection("committee_reports", "govinfo-docs", G.CommitteeReports, "GovInfo CRPT packages: House, Senate and Senate executive reports",
               "Full text of the report (HTML rendition, PDF text layer when HTML is missing or short)", "report"),
    Collection("hearings", "govinfo-docs", G.Hearings, "GovInfo CHRG packages",
               "Full transcript of the hearing (HTML rendition, PDF text layer when HTML is missing or short)", "hearing"),
    Collection("committee_prints", "govinfo-docs", G.CommitteePrints, "GovInfo CPRT packages",
               "Full text of the committee print (HTML rendition, PDF text layer when HTML is missing or short)", "print"),
    Collection("congressional_documents", "govinfo-docs", G.CongressionalDocuments, "GovInfo CDOC packages: House, Senate and Senate Treaty documents",
               "Full text of the document (HTML rendition, PDF text layer when HTML is missing or short)", "document"),
    Collection("congressional_record", "govinfo-record", G.CongressionalRecord, "GovInfo CREC packages: the daily Congressional Record, one row per granule",
               "Full text of one article (granule) of a daily issue", "daily issue"),
    Collection("crs_reports", "congress-crs", C.CrsReports, "Congress.gov API /crsreport plus the report's PDF on www.congress.gov (HTML only when there is no PDF)",
               "Full text of the current version of the CRS product, from the PDF's text layer", "report"),
    Collection("treaties", "congress-api", C.Treaties, "Congress.gov API /treaty, with the Senate Treaty Document text from GovInfo CDOC",
               "Treaty Document text when GovInfo has it, else the resolution of advice and consent, else empty", "treaty"),
    Collection("members", "congress-api", C.Members, "Congress.gov API /member", "Empty; the record is in metadata", "member"),
    Collection("committees", "congress-api", C.Committees, "Congress.gov API /committee", "Empty; the record is in metadata", "committee"),
    Collection("congresses", "congress-api", C.Congresses, "Congress.gov API /congress", "Empty; sessions and dates are in metadata", "congress"),
    Collection("house_requirements", "congress-api", C.HouseRequirements, "Congress.gov API /house-requirement",
               "Empty; the requirement's nature is the title, the rest is in metadata", "requirement"),
    Collection("nominations", "congress-api", C.Nominations, "Congress.gov API /nomination with actions",
               "Empty; the nomination as received is the title, nominees and actions are in metadata", "nomination"),
    Collection("committee_meetings", "congress-api", C.CommitteeMeetings, "Congress.gov API /committee-meeting",
               "Empty; witnesses, documents and videos are in metadata", "meeting"),
    Collection("house_votes", "congress-api", C.HouseVotes, "Congress.gov API /house-vote with every member's vote (API covers the 118th Congress on)",
               "Empty; the question, result, party totals and member votes are in metadata", "roll call"),
    Collection("house_communications", "congress-api", C.HouseCommunications, "Congress.gov API /house-communication",
               "Abstract of the executive communication, memorial, petition or presidential message", "communication"),
    Collection("senate_communications", "congress-api", C.SenateCommunications, "Congress.gov API /senate-communication",
               "Abstract of the executive communication, petition or memorial", "communication"),
    Collection("amendments", "congress-api", C.Amendments, "Congress.gov API /amendment (detail record; amendment text is not included)",
               "Empty; the purpose or description is the title, sponsors and the amended bill are in metadata", "amendment"),
    Collection("bills_legacy", "congress-api", C.LegacyBills, "Congress.gov API /bill with summaries, 93rd-107th Congresses (BILLSTATUS starts at the 108th)",
               "Latest CRS summary of the bill", "bill"),
]

BY_NAME = {collection.name: collection for collection in COLLECTIONS}
LANES = sorted({collection.lane for collection in COLLECTIONS})


def for_lane(lane):
    return [collection for collection in COLLECTIONS if collection.lane == lane]
