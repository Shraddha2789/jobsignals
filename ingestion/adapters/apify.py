"""
Apify adapter — scrapes LinkedIn Jobs and Indeed via Apify cloud actors.

Actors used:
  LinkedIn: bebity/linkedin-jobs-scraper
  Indeed:   misceres/indeed-scraper

Sign up:  https://apify.com/
API key:  https://console.apify.com/settings/integrations
Set env:  APIFY_API_TOKEN=apify_api_...

Free tier: $5/month credit (~5,000 LinkedIn results or ~25,000 Indeed results).
Both actors run synchronously — results stream back in one HTTP call.
"""

from __future__ import annotations

import hashlib
import os
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

APIFY_BASE_URL = "https://api.apify.com/v2"

# ── Default search targets ─────────────────────────────────────────────────────

LINKEDIN_DEFAULT_QUERIES: list[dict] = [
    {"query": "software engineer", "location": "United States"},
    {"query": "data scientist", "location": "United States"},
    {"query": "data engineer", "location": "United States"},
    {"query": "machine learning engineer", "location": "United States"},
    {"query": "product manager", "location": "United States"},
]

INDEED_DEFAULT_QUERIES: list[dict] = [
    {"position": "software engineer", "country": "US", "location": ""},
    {"position": "data scientist", "country": "US", "location": ""},
    {"position": "data engineer", "country": "US", "location": ""},
    {"position": "machine learning engineer", "country": "US", "location": ""},
    {"position": "product manager", "country": "US", "location": ""},
]


# ── Helpers ───────────────────────────────────────────────────────────────────


def _strip_html(text: str) -> str:
    text = re.sub(r"<[^>]+>", " ", text or "")
    return re.sub(r"\s+", " ", text).strip()


def _parse_date(date_str: str | None) -> datetime:
    if not date_str:
        return datetime.now(tz=timezone.utc)
    try:
        return datetime.fromisoformat(date_str.replace("Z", "+00:00")).astimezone(timezone.utc)
    except Exception:
        return datetime.now(tz=timezone.utc)


def _parse_salary(salary_str: str | None) -> tuple[int | None, int | None, str]:
    """
    Parse salary strings like '$120,000 - $150,000/yr' or '£50k'.
    Returns (sal_min, sal_max, currency).
    """
    if not salary_str:
        return None, None, "USD"

    currency = "USD"
    if "£" in salary_str:
        currency = "GBP"
    elif "€" in salary_str:
        currency = "EUR"
    elif "₹" in salary_str or "INR" in salary_str.upper():
        currency = "INR"

    # Expand shorthand: 120k → 120000
    normalized = re.sub(
        r"(\d+(?:\.\d+)?)k", lambda m: str(int(float(m.group(1)) * 1000)), salary_str, flags=re.I
    )

    numbers = [
        int(n.replace(",", ""))
        for n in re.findall(r"[\d,]+", normalized)
        if n.replace(",", "").isdigit()
    ]

    # Convert hourly/monthly to annual
    lower = salary_str.lower()
    if "hour" in lower or "/hr" in lower:
        numbers = [n * 2080 for n in numbers]  # 40 hr × 52 wk
    elif "month" in lower or "/mo" in lower:
        numbers = [n * 12 for n in numbers]

    if len(numbers) >= 2:
        return numbers[0], numbers[1], currency
    if len(numbers) == 1:
        return numbers[0], numbers[0], currency
    return None, None, currency


def _detect_modality(text: str) -> str:
    lower = (text or "").lower()
    if "remote" in lower:
        return "remote"
    if "hybrid" in lower:
        return "hybrid"
    if "on-site" in lower or "onsite" in lower or "in office" in lower or "in-office" in lower:
        return "onsite"
    return "unspecified"


def _detect_country(location: str) -> str:
    lower = (location or "").lower()
    mapping = {
        "united states": "US",
        " usa": "US",
        ", us": "US",
        "united kingdom": "GB",
        " uk": "GB",
        "india": "IN",
        "canada": "CA",
        "germany": "DE",
        "australia": "AU",
        "singapore": "SG",
        "netherlands": "NL",
        "france": "FR",
    }
    for key, code in mapping.items():
        if key in lower:
            return code
    return "US"


# ── Shared Apify HTTP layer ───────────────────────────────────────────────────


class _ApifyBaseAdapter(BaseAdapter):
    """Handles all Apify API communication; subclasses provide actor logic."""

    source_platform = "other"

    def __init__(self) -> None:
        self.api_token = os.environ.get("APIFY_API_TOKEN", "")

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=5, max=30),
    )
    def _run_actor_sync(
        self,
        actor_id: str,
        actor_input: dict,
        wait_secs: int = 300,
    ) -> list[dict]:
        """
        Run an Apify actor synchronously and return its dataset items.
        Uses the run-sync-get-dataset-items endpoint (max wait: 300 s).
        Falls back to async polling if the sync run times out.
        """
        url = f"{APIFY_BASE_URL}/acts/{actor_id}/run-sync-get-dataset-items"
        headers = {"Authorization": f"Bearer {self.api_token}"}
        params = {"waitForFinish": wait_secs, "memory": 512}

        resp = httpx.post(
            url,
            json=actor_input,
            headers=headers,
            params=params,
            timeout=wait_secs + 60,
        )
        resp.raise_for_status()

        body = resp.json()
        return body if isinstance(body, list) else []

    def _run_actor_async_poll(
        self,
        actor_id: str,
        actor_input: dict,
        poll_interval: int = 10,
        max_wait: int = 600,
    ) -> list[dict]:
        """
        Start an actor run asynchronously, poll until finished, then fetch dataset.
        Used as fallback when sync mode times out.
        """
        headers = {"Authorization": f"Bearer {self.api_token}"}

        # Start run
        run_resp = httpx.post(
            f"{APIFY_BASE_URL}/acts/{actor_id}/runs",
            json=actor_input,
            headers=headers,
            params={"memory": 512},
            timeout=30,
        )
        run_resp.raise_for_status()
        run_id = run_resp.json()["data"]["id"]

        # Poll until finished
        elapsed = 0
        while elapsed < max_wait:
            time.sleep(poll_interval)
            elapsed += poll_interval
            status_resp = httpx.get(
                f"{APIFY_BASE_URL}/actor-runs/{run_id}",
                headers=headers,
                timeout=15,
            )
            status_resp.raise_for_status()
            status = status_resp.json()["data"]["status"]
            if status in ("SUCCEEDED", "FAILED", "ABORTED", "TIMED-OUT"):
                break

        if status != "SUCCEEDED":
            console.print(f"  [yellow]⚠ Apify run {run_id} ended with status: {status}[/]")
            return []

        # Fetch dataset
        dataset_id = run_resp.json()["data"]["defaultDatasetId"]
        items_resp = httpx.get(
            f"{APIFY_BASE_URL}/datasets/{dataset_id}/items",
            headers=headers,
            params={"format": "json", "clean": True},
            timeout=60,
        )
        items_resp.raise_for_status()
        return items_resp.json() if isinstance(items_resp.json(), list) else []


# ── LinkedIn adapter ──────────────────────────────────────────────────────────


class ApifyLinkedInAdapter(_ApifyBaseAdapter):
    """
    Scrapes LinkedIn Jobs via Apify actor bebity/linkedin-jobs-scraper.

    Actor cost: ~$1 per 1,000 results on Apify free tier.

    Args:
        queries: list of dicts with 'query' and 'location' keys.
                 Defaults to LINKEDIN_DEFAULT_QUERIES.
        rows_per_query: max job postings to fetch per query (default 50).
    """

    source_platform = "linkedin"
    ACTOR_ID = "bebity/linkedin-jobs-scraper"

    def __init__(
        self,
        queries: list[dict] | None = None,
        rows_per_query: int = 50,
    ) -> None:
        super().__init__()
        self.queries = queries or LINKEDIN_DEFAULT_QUERIES
        self.rows_per_query = rows_per_query

    def fetch(self) -> Iterator[RawJobPosting]:
        if not self.api_token:
            console.print("  [yellow]⚠ APIFY_API_TOKEN not set — skipping LinkedIn (Apify)[/]")
            return

        console.print(
            f"  [dim]Fetching LinkedIn jobs via Apify ({len(self.queries)} queries, "
            f"{self.rows_per_query} rows each)...[/]"
        )
        total = 0

        for target in self.queries:
            actor_input = {
                "title": target.get("query", "software engineer"),
                "location": target.get("location", "United States"),
                "rows": self.rows_per_query,
                "publishedAt": "r86400",  # past 24 hours
            }
            try:
                items = self._run_actor_sync(self.ACTOR_ID, actor_input)
            except Exception as e:
                console.print(f"  [red]✗ Apify LinkedIn '{target.get('query')}' failed: {e}[/]")
                continue

            console.print(
                f"  [dim]  LinkedIn '{target.get('query')}' @ "
                f"'{target.get('location')}': {len(items)} results[/]"
            )

            for item in items:
                posting = self._map_item(item)
                if posting:
                    yield posting
                    total += 1

            time.sleep(3)  # be polite between actor runs

        console.print(f"  [green]✓[/] Apify LinkedIn: {total} jobs fetched")

    def _map_item(self, item: dict) -> RawJobPosting | None:
        description = _strip_html(item.get("descriptionHtml") or item.get("description") or "")
        if not description:
            return None

        title = item.get("title") or ""
        company = item.get("companyName") or item.get("company") or "Unknown"
        location_raw = item.get("location") or ""
        url = item.get("url") or item.get("applyUrl") or ""

        raw_id = f"li_{title}{company}{location_raw}"
        source_id = "apify_li_" + hashlib.md5(raw_id.encode()).hexdigest()[:12]

        sal_str = item.get("salary") or item.get("salaryRange") or ""
        sal_min, sal_max, sal_cur = _parse_salary(sal_str)

        modality = _detect_modality((item.get("workType") or "") + " " + location_raw)

        contract = (item.get("contractType") or "").lower()
        emp_type = "full_time"
        if "part" in contract:
            emp_type = "part_time"
        elif "contract" in contract or "freelan" in contract:
            emp_type = "contract"
        elif "intern" in contract:
            emp_type = "internship"

        return RawJobPosting(
            source_id=source_id,
            source_platform="linkedin",
            source_url=url or None,
            title_raw=title,
            company_name=company,
            company_domain=None,
            location_raw=location_raw,
            location_city=location_raw.split(",")[0].strip() if location_raw else None,
            location_country=_detect_country(location_raw),
            work_modality=modality,
            employment_type=emp_type,
            seniority_level=None,
            description_raw=description,
            salary_min=sal_min,
            salary_max=sal_max,
            salary_currency=sal_cur,
            salary_source="posted" if sal_min else None,
            posted_at=_parse_date(item.get("postedAt") or item.get("publishedAt")),
        )


# ── Indeed adapter ────────────────────────────────────────────────────────────


class ApifyIndeedAdapter(_ApifyBaseAdapter):
    """
    Scrapes Indeed via Apify actor misceres/indeed-scraper.

    Actor cost: ~$0.20 per 1,000 results on Apify free tier.

    Args:
        queries: list of dicts with 'position', 'country', and optional 'location' keys.
                 Defaults to INDEED_DEFAULT_QUERIES.
        max_items_per_query: max job postings to fetch per query (default 50).
    """

    source_platform = "indeed"
    ACTOR_ID = "misceres~indeed-scraper"

    def __init__(
        self,
        queries: list[dict] | None = None,
        max_items_per_query: int = 50,
    ) -> None:
        super().__init__()
        self.queries = queries or INDEED_DEFAULT_QUERIES
        self.max_items_per_query = max_items_per_query

    def fetch(self) -> Iterator[RawJobPosting]:
        if not self.api_token:
            console.print("  [yellow]⚠ APIFY_API_TOKEN not set — skipping Indeed (Apify)[/]")
            return

        console.print(
            f"  [dim]Fetching Indeed jobs via Apify ({len(self.queries)} queries, "
            f"{self.max_items_per_query} items each)...[/]"
        )
        total = 0

        for target in self.queries:
            actor_input = {
                "position": target.get("position", "software engineer"),
                "country": target.get("country", "US"),
                "location": target.get("location", ""),
                "maxItems": self.max_items_per_query,
                "saveOnlyUniqueItems": True,
            }
            try:
                items = self._run_actor_sync(self.ACTOR_ID, actor_input)
            except Exception as e:
                console.print(f"  [red]✗ Apify Indeed '{target.get('position')}' failed: {e}[/]")
                continue

            console.print(
                f"  [dim]  Indeed '{target.get('position')}' "
                f"({target.get('country', 'US')}): {len(items)} results[/]"
            )

            for item in items:
                posting = self._map_item(item, target.get("country", "US"))
                if posting:
                    yield posting
                    total += 1

            time.sleep(3)

        console.print(f"  [green]✓[/] Apify Indeed: {total} jobs fetched")

    def _map_item(self, item: dict, country: str = "US") -> RawJobPosting | None:
        description = _strip_html(item.get("description") or item.get("descriptionHtml") or "")
        if not description:
            return None

        title = item.get("positionName") or item.get("title") or ""
        company = item.get("company") or item.get("companyName") or "Unknown"
        location_raw = item.get("location") or ""
        url = item.get("url") or item.get("jobUrl") or ""

        raw_id = f"in_{title}{company}{location_raw}"
        source_id = "apify_in_" + hashlib.md5(raw_id.encode()).hexdigest()[:12]

        sal_str = item.get("salary") or item.get("salaryRange") or ""
        sal_min, sal_max, sal_cur = _parse_salary(sal_str)

        modality = _detect_modality(location_raw + " " + description[:300])

        job_type = (item.get("jobType") or "").lower()
        emp_type = "full_time"
        if "part" in job_type:
            emp_type = "part_time"
        elif "contract" in job_type or "temp" in job_type:
            emp_type = "contract"
        elif "intern" in job_type:
            emp_type = "internship"

        return RawJobPosting(
            source_id=source_id,
            source_platform="indeed",
            source_url=url or None,
            title_raw=title,
            company_name=company,
            company_domain=None,
            location_raw=location_raw,
            location_city=location_raw.split(",")[0].strip() if location_raw else None,
            location_country=country,
            work_modality=modality,
            employment_type=emp_type,
            seniority_level=None,
            description_raw=description,
            salary_min=sal_min,
            salary_max=sal_max,
            salary_currency=sal_cur,
            salary_source="posted" if sal_min else None,
            posted_at=_parse_date(item.get("postedAt") or item.get("datePosted")),
        )
