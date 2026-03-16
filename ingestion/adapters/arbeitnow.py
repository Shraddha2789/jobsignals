"""
Arbeitnow adapter — fetches job postings from arbeitnow.com's public API.

Returns ~300 jobs per run (3 pages × 100). Mix of remote + onsite,
global locations, diverse roles (tech and non-tech). No auth required.
"""
from __future__ import annotations

import re
from datetime import datetime, timezone
from typing import Iterator

import httpx
from rich.console import Console
from tenacity import retry, stop_after_attempt, wait_exponential

from ingestion.adapters.base import BaseAdapter
from ingestion.models import RawJobPosting

console = Console()

API_URL = "https://www.arbeitnow.com/api/job-board-api"
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) JobSignals/1.0",
    "Accept": "application/json",
}

# How many pages to fetch (100 jobs/page)
DEFAULT_PAGES = 3


def _strip_html(html: str) -> str:
    text = re.sub(r"<[^>]+>", " ", html)
    for entity, char in [("&amp;", "&"), ("&lt;", "<"), ("&gt;", ">"),
                          ("&nbsp;", " "), ("&#39;", "'"), ("&quot;", '"')]:
        text = text.replace(entity, char)
    return re.sub(r"\s+", " ", text).strip()


def _parse_location(location: str, remote: bool) -> tuple[str, str, str]:
    """Return (location_raw, city, modality)."""
    if remote:
        return location or "Remote", "Remote", "remote"
    city = (location or "").split(",")[0].strip() or "Unknown"
    return location or "Unknown", city, "onsite"


def _parse_date(ts) -> datetime:
    """Parse Unix timestamp or ISO string."""
    try:
        if isinstance(ts, (int, float)):
            return datetime.fromtimestamp(ts, tz=timezone.utc)
        return datetime.fromisoformat(str(ts)).astimezone(timezone.utc)
    except Exception:
        return datetime.now(tz=timezone.utc)


def _infer_country(location: str) -> str:
    """Best-effort country extraction from location string."""
    if not location:
        return "US"
    loc = location.lower()
    country_hints = {
        # Germany — major cities + common suffixes
        "germany": "DE", "deutschland": "DE",
        "berlin": "DE", "munich": "DE", "münchen": "DE", "hamburg": "DE",
        "cologne": "DE", "köln": "DE", "koln": "DE",
        "frankfurt": "DE", "düsseldorf": "DE", "dusseldorf": "DE",
        "stuttgart": "DE", "dortmund": "DE", "essen": "DE", "bremen": "DE",
        "leipzig": "DE", "hannover": "DE", "hanover": "DE",
        "nuremberg": "DE", "nürnberg": "DE", "nurnberg": "DE",
        "bonn": "DE", "mannheim": "DE", "wiesbaden": "DE",
        "bielefeld": "DE", "münster": "DE", "munster": "DE",
        "aachen": "DE", "chemnitz": "DE", "kiel": "DE",
        "magdeburg": "DE", "rostock": "DE", "erfurt": "DE",
        "mainz": "DE", "lübeck": "DE", "lubeck": "DE",
        "heidelberg": "DE", "darmstadt": "DE", "regensburg": "DE",
        "ingolstadt": "DE", "würzburg": "DE", "wurzburg": "DE",
        "ulm": "DE", "wolfsburg": "DE", "heilbronn": "DE",
        "potsdam": "DE", "dresden": "DE", "freiburg": "DE",
        "augsburg": "DE", "karlsruhe": "DE", "kaiserslautern": "DE",
        "saarbrücken": "DE", "saarbrucken": "DE", "trier": "DE",
        "aschersleben": "DE", "aschaffenburg": "DE",
        "am main": "DE", "im breisgau": "DE", "an der ruhr": "DE",
        # UK
        "uk": "GB", "united kingdom": "GB", "england": "GB",
        "london": "GB", "manchester": "GB", "birmingham": "GB",
        "leeds": "GB", "glasgow": "GB", "edinburgh": "GB",
        "liverpool": "GB", "bristol": "GB", "sheffield": "GB",
        # Netherlands
        "netherlands": "NL", "holland": "NL",
        "amsterdam": "NL", "rotterdam": "NL", "the hague": "NL", "utrecht": "NL",
        # France
        "france": "FR", "paris": "FR", "lyon": "FR", "marseille": "FR",
        # Canada
        "canada": "CA", "toronto": "CA", "vancouver": "CA",
        "montreal": "CA", "calgary": "CA", "ottawa": "CA",
        # Australia
        "australia": "AU", "sydney": "AU", "melbourne": "AU",
        "brisbane": "AU", "perth": "AU", "adelaide": "AU",
        # India
        "india": "IN", "bangalore": "IN", "bengaluru": "IN",
        "mumbai": "IN", "delhi": "IN", "hyderabad": "IN", "pune": "IN",
        "chennai": "IN", "kolkata": "IN",
        # Singapore / Philippines
        "singapore": "SG",
        "philippines": "PH", "manila": "PH", "metro manila": "PH",
        # US — explicit only, don't default "remote" to US
        "united states": "US", "usa": "US", "new york": "US",
        "san francisco": "US", "los angeles": "US", "chicago": "US",
        "seattle": "US", "boston": "US", "austin": "US",
    }
    for hint, code in country_hints.items():
        if hint in loc:
            return code
    return "US"


class ArbeitnowAdapter(BaseAdapter):
    """
    Fetches job postings from arbeitnow.com.
    Covers remote + onsite, global, all role categories.
    """

    source_platform = "arbeitnow"

    def __init__(self, pages: int = DEFAULT_PAGES) -> None:
        self.pages = pages

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10))
    def _fetch_page(self, page: int) -> list[dict]:
        response = httpx.get(
            API_URL, headers=HEADERS, params={"page": page}, timeout=20
        )
        response.raise_for_status()
        return response.json().get("data", [])

    def fetch(self) -> Iterator[RawJobPosting]:
        console.print(f"  [dim]Fetching from Arbeitnow API ({self.pages} pages)...[/]")
        total_raw = 0
        yielded = 0

        for page in range(1, self.pages + 1):
            try:
                raw_jobs = self._fetch_page(page)
            except Exception as e:
                console.print(f"  [red]✗ Arbeitnow page {page} failed: {e}[/]")
                break

            total_raw += len(raw_jobs)
            if not raw_jobs:
                break

            for job in raw_jobs:
                description_raw = _strip_html(job.get("description") or "")
                if not description_raw:
                    continue

                remote   = bool(job.get("remote"))
                location = job.get("location") or ""
                loc_raw, city, modality = _parse_location(location, remote)
                country  = _infer_country(location)

                # job_types is a list like ["Full-time"] or []
                job_types = [t.lower().replace("-", "_").replace(" ", "_")
                             for t in (job.get("job_types") or [])]
                emp_type = "full_time"
                for jt in job_types:
                    if "part" in jt:
                        emp_type = "part_time"
                    elif "contract" in jt or "freelance" in jt:
                        emp_type = "contract"
                    elif "intern" in jt:
                        emp_type = "internship"

                yield RawJobPosting(
                    source_id        = job["slug"],
                    source_platform  = "arbeitnow",
                    source_url       = job.get("url"),
                    title_raw        = job["title"],
                    company_name     = (job.get("company_name") or "Unknown").strip(),
                    company_domain   = None,
                    location_raw     = loc_raw,
                    location_city    = city,
                    location_country = country,
                    work_modality    = modality,
                    employment_type  = emp_type,
                    seniority_level  = None,
                    description_raw  = description_raw,
                    salary_min       = None,
                    salary_max       = None,
                    salary_currency  = "USD",
                    salary_source    = None,
                    posted_at        = _parse_date(job.get("created_at")),
                )
                yielded += 1

        console.print(f"  [dim]Received {total_raw} raw jobs from Arbeitnow[/]")
        console.print(f"  [green]✓[/] {yielded} Arbeitnow jobs yielded")
