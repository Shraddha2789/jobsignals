"""
Jobicy adapter — fetches remote job postings from jobicy.com public API.

Completely free, no API key required.
Returns up to 50 remote jobs per request across all countries.

Docs: https://jobicy.com/jobs-rss-feed
API:  https://jobicy.com/api/v2/remote-jobs
"""

from __future__ import annotations

import re
import time
from datetime import datetime, timezone
from typing import Iterator

import httpx
from rich.console import Console
from tenacity import retry, stop_after_attempt, wait_exponential

from ingestion.adapters.base import BaseAdapter
from ingestion.models import RawJobPosting

console = Console()

API_URL = "https://jobicy.com/api/v2/remote-jobs"
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) JobSignals/1.0",
    "Accept": "application/json",
}

# Job categories to fetch — Jobicy supports category filtering
CATEGORIES = [
    "engineering",
    "product",
    "data-science",
    "design",
    "marketing",
    "devops",
]

COUNTRY_MAP = {
    "india": "IN",
    "united states": "US",
    "us": "US",
    "usa": "US",
    "united kingdom": "GB",
    "uk": "GB",
    "germany": "DE",
    "canada": "CA",
    "australia": "AU",
    "singapore": "SG",
    "netherlands": "NL",
    "france": "FR",
    "brazil": "BR",
    "worldwide": "US",
    "global": "US",
    "anywhere": "US",
}


def _strip_html(html: str) -> str:
    text = re.sub(r"<[^>]+>", " ", html or "")
    text = re.sub(r"&amp;", "&", text)
    text = re.sub(r"&lt;", "<", text)
    text = re.sub(r"&gt;", ">", text)
    text = re.sub(r"&nbsp;", " ", text)
    return re.sub(r"\s+", " ", text).strip()


def _parse_date(date_str: str | None) -> datetime:
    if not date_str:
        return datetime.now(tz=timezone.utc)
    try:
        return datetime.fromisoformat(date_str.replace("Z", "+00:00")).astimezone(timezone.utc)
    except Exception:
        return datetime.now(tz=timezone.utc)


def _infer_country(region: str) -> str:
    if not region:
        return "US"
    return COUNTRY_MAP.get(region.lower().strip(), "US")


class JobicyAdapter(BaseAdapter):
    """
    Fetches remote jobs from jobicy.com — completely free, no auth.
    Covers global remote roles across all tech categories.
    """

    source_platform = "other"

    def __init__(self, count: int = 50) -> None:
        self.count = min(count, 50)  # API max is 50

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=10),
    )
    def _fetch(self, tag: str) -> list[dict]:
        params = {"count": self.count, "tag": tag}
        resp = httpx.get(API_URL, headers=HEADERS, params=params, timeout=20)
        resp.raise_for_status()
        return resp.json().get("jobs", [])

    def fetch(self) -> Iterator[RawJobPosting]:
        console.print("  [dim]Fetching from Jobicy API...[/]")
        total = 0
        seen_ids: set[str] = set()

        for category in CATEGORIES:
            try:
                jobs = self._fetch(tag=category)
            except Exception as e:
                console.print(f"  [red]✗ Jobicy '{category}' failed: {e}[/]")
                continue

            for job in jobs:
                source_id = str(job.get("id") or "")
                if not source_id or source_id in seen_ids:
                    continue
                seen_ids.add(source_id)

                description = _strip_html(job.get("jobDescription") or job.get("description") or "")
                if not description:
                    continue

                region = job.get("jobGeo") or ""
                country = _infer_country(region)

                yield RawJobPosting(
                    source_id=f"jobicy_{source_id}",
                    source_platform="other",
                    source_url=job.get("url") or job.get("jobUri"),
                    title_raw=job.get("jobTitle") or "",
                    company_name=job.get("companyName") or "Unknown",
                    company_domain=job.get("companyUrl"),
                    location_raw=region or "Remote",
                    location_city=region or "Remote",
                    location_country=country,
                    work_modality="remote",
                    employment_type="full_time",
                    seniority_level=None,
                    description_raw=description,
                    salary_min=None,
                    salary_max=None,
                    salary_currency="USD",
                    salary_source=None,
                    posted_at=_parse_date(job.get("pubDate")),
                )
                total += 1

            time.sleep(1)

        console.print(f"  [green]✓[/] Jobicy: {total} jobs fetched")
