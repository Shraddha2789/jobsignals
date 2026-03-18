"""
The Muse adapter — fetches job postings from themuse.com public API.

Completely free, no API key required.
Strong coverage of US, UK, Canada, Australia, Germany and other global markets.
Focuses on tech, product, design, and data roles.

API docs: https://www.themuse.com/developers/api/v2
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

API_URL = "https://www.themuse.com/api/public/jobs"
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) JobSignals/1.0",
    "Accept": "application/json",
}

# Categories that map well to tech/product roles
CATEGORIES = [
    "Software Engineer",
    "Data Science",
    "Product Management",
    "Engineering",
    "Design",
    "DevOps",
    "QA",
    "Data and Analytics",
]

COUNTRY_MAP = {
    "us": "US",
    "usa": "US",
    "united states": "US",
    "uk": "GB",
    "united kingdom": "GB",
    "england": "GB",
    "germany": "DE",
    "canada": "CA",
    "australia": "AU",
    "india": "IN",
    "singapore": "SG",
    "netherlands": "NL",
    "france": "FR",
    "remote": "US",
}

MAX_PAGES = 3  # 3 pages × ~20 results = ~60 jobs per category


def _strip_html(html: str) -> str:
    text = re.sub(r"<[^>]+>", " ", html or "")
    text = re.sub(r"&amp;", "&", text)
    text = re.sub(r"&lt;", "<", text)
    text = re.sub(r"&gt;", ">", text)
    text = re.sub(r"&nbsp;", " ", text)
    return re.sub(r"\s+", " ", text).strip()


def _infer_country(locations: list[dict]) -> str:
    for loc in locations:
        name = (loc.get("name") or "").lower()
        for key, code in COUNTRY_MAP.items():
            if key in name:
                return code
    return "US"


def _parse_date(date_str: str | None) -> datetime:
    if not date_str:
        return datetime.now(tz=timezone.utc)
    try:
        return datetime.fromisoformat(date_str.replace("Z", "+00:00")).astimezone(timezone.utc)
    except Exception:
        return datetime.now(tz=timezone.utc)


class TheMuseAdapter(BaseAdapter):
    """
    Fetches jobs from The Muse — free public API, no auth required.
    Strong US/UK/Canada/Australia/Germany coverage.
    """

    source_platform = "other"

    def __init__(self, max_pages: int = MAX_PAGES) -> None:
        self.max_pages = max_pages

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=10),
    )
    def _fetch_page(self, category: str, page: int) -> dict:
        params = {"category": category, "page": page, "descended": "true"}
        resp = httpx.get(API_URL, headers=HEADERS, params=params, timeout=20)
        resp.raise_for_status()
        return resp.json()

    def fetch(self) -> Iterator[RawJobPosting]:
        console.print("  [dim]Fetching from The Muse API...[/]")
        total = 0
        seen_ids: set[str] = set()

        for category in CATEGORIES:
            for page in range(self.max_pages):
                try:
                    data = self._fetch_page(category, page)
                except Exception as e:
                    console.print(f"  [red]✗ The Muse '{category}' p{page} failed: {e}[/]")
                    break

                results = data.get("results", [])
                if not results:
                    break

                for job in results:
                    source_id = str(job.get("id") or "")
                    if not source_id or source_id in seen_ids:
                        continue
                    seen_ids.add(source_id)

                    contents = _strip_html(job.get("contents") or "")
                    if not contents:
                        continue

                    locations = job.get("locations") or []
                    loc_names = [loc.get("name", "") for loc in locations]
                    location_raw = ", ".join(loc_names) if loc_names else "Remote"
                    country = _infer_country(locations)

                    company = job.get("company") or {}

                    yield RawJobPosting(
                        source_id=f"muse_{source_id}",
                        source_platform="other",
                        source_url=job.get("refs", {}).get("landing_page"),
                        title_raw=job.get("name") or "",
                        company_name=company.get("name") or "Unknown",
                        company_domain=None,
                        location_raw=location_raw,
                        location_city=loc_names[0] if loc_names else "Remote",
                        location_country=country,
                        work_modality="remote" if "remote" in location_raw.lower() else "onsite",
                        employment_type="full_time",
                        seniority_level=None,
                        description_raw=contents,
                        salary_min=None,
                        salary_max=None,
                        salary_currency="USD",
                        salary_source=None,
                        posted_at=_parse_date(job.get("publication_date")),
                    )
                    total += 1

                time.sleep(0.5)

        console.print(f"  [green]✓[/] The Muse: {total} jobs fetched")
