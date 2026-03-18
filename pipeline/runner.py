"""
Ingestion + normalization runner.

Orchestrates: fetch → deduplicate → normalize → persist → aggregate.
One run processes all registered adapters sequentially.
"""

from __future__ import annotations

import os

from rich.console import Console
from rich.progress import Progress, SpinnerColumn, TextColumn
from sqlalchemy import text

from db import get_connection
from ingestion.adapters.adzuna import AdzunaAdapter
from ingestion.adapters.jooble import JoobleAdapter
from ingestion.adapters.arbeitnow import ArbeitnowAdapter
from ingestion.adapters.base import BaseAdapter
from ingestion.adapters.himalayas import HimalayasAdapter
from ingestion.adapters.jobicy import JobicyAdapter
from ingestion.adapters.remoteok import RemoteOKAdapter
from ingestion.adapters.remotive import RemotiveAdapter
from ingestion.adapters.serpapi import SerpAPIAdapter
from ingestion.adapters.seed import COMPANIES, SeedAdapter
from ingestion.models import RawJobPosting
from pipeline.aggregations import run_all_aggregations
from pipeline.deduplication import compute_content_hash, hash_exists, is_duplicate
from pipeline.normalization import extract_skills, normalize_title

console = Console()

DATASET_VERSION = os.environ.get("DATASET_VERSION", "1.0.0")


# ── Company bootstrap ──────────────────────────────────────────────────────────


def _ensure_companies() -> dict[str, str]:
    """
    Insert all seed companies if not present.
    Returns a mapping of {domain: company_id}.
    """
    mapping: dict[str, str] = {}
    with get_connection() as conn:
        for c in COMPANIES:
            result = conn.execute(
                text("SELECT company_id FROM companies WHERE domain = :domain"),
                {"domain": c["domain"]},
            ).fetchone()

            if result:
                mapping[c["domain"]] = str(result[0])
            else:
                row = conn.execute(
                    text(
                        """
                        INSERT INTO companies
                            (company_name, domain, industry, company_stage,
                             employee_count_range, hq_country)
                        VALUES
                            (:name, :domain, :industry, :stage, :employees, :country)
                        RETURNING company_id
                        """
                    ),
                    {
                        "name": c["name"],
                        "domain": c["domain"],
                        "industry": c["industry"],
                        "stage": c["stage"],
                        "employees": c["employees"],
                        "country": c["country"],
                    },
                ).fetchone()
                mapping[c["domain"]] = str(row[0])
    return mapping


# ── Single posting persistence ─────────────────────────────────────────────────


def _persist_posting(
    raw: RawJobPosting,
    company_id: str | None,
    title_normalized: str,
    title_family: str | None,
    seniority_level: str | None,
    description_cleaned: str,
    content_hash: str,
) -> str | None:
    """Insert one job posting and return its job_id, or None if skipped."""

    if hash_exists(content_hash):
        return None

    with get_connection() as conn:
        result = conn.execute(
            text(
                """
                INSERT INTO job_postings (
                    source_id, source_platform, source_url,
                    title_raw, title_normalized, title_family,
                    company_id,
                    location_raw, location_city, location_country,
                    work_modality, employment_type, seniority_level,
                    description_raw, description_cleaned,
                    salary_min, salary_max, salary_currency, salary_source,
                    posted_at, content_hash, dataset_version
                ) VALUES (
                    :src_id, :platform, :url,
                    :title_raw, :title_norm, :family,
                    :company_id,
                    :loc_raw, :city, :country,
                    :modality, :emp_type, :seniority,
                    :desc_raw, :desc_clean,
                    :sal_min, :sal_max, :sal_cur, :sal_src,
                    :posted_at, :hash, :version
                )
                ON CONFLICT (source_platform, source_id) DO NOTHING
                RETURNING job_id
                """
            ),
            {
                "src_id": raw.source_id,
                "platform": raw.source_platform,
                "url": raw.source_url,
                "title_raw": raw.title_raw,
                "title_norm": title_normalized,
                "family": title_family,
                "company_id": company_id,
                "loc_raw": raw.location_raw,
                "city": raw.location_city,
                "country": raw.location_country,
                "modality": raw.work_modality,
                "emp_type": raw.employment_type,
                "seniority": seniority_level,
                "desc_raw": raw.description_raw,
                "desc_clean": description_cleaned,
                "sal_min": raw.salary_min,
                "sal_max": raw.salary_max,
                "sal_cur": raw.salary_currency,
                "sal_src": raw.salary_source,
                "posted_at": raw.posted_at,
                "hash": content_hash,
                "version": DATASET_VERSION,
            },
        ).fetchone()

        if result is None:
            return None
        return str(result[0])


def _persist_skills(job_id: str, raw_desc: str) -> int:
    skills = extract_skills(raw_desc)
    if not skills:
        return 0
    with get_connection() as conn:
        for skill in skills:
            conn.execute(
                text(
                    """
                    INSERT INTO job_skills
                        (job_id, skill_name, skill_category, skill_raw,
                         is_required, extraction_method, confidence_score)
                    VALUES
                        (:job_id, :name, :cat, :raw, :req, :method, :conf)
                    ON CONFLICT (job_id, skill_name) DO NOTHING
                    """
                ),
                {
                    "job_id": job_id,
                    "name": skill.skill_name,
                    "cat": skill.skill_category,
                    "raw": skill.skill_raw,
                    "req": skill.is_required,
                    "method": skill.extraction_method,
                    "conf": skill.confidence_score,
                },
            )
    return len(skills)


# ── Auto-insert unknown companies ─────────────────────────────────────────────


def _upsert_company_by_name(company_name: str, company_map: dict[str, str]) -> str | None:
    """
    Insert a company by name if it doesn't exist (for sources without domain info).
    Uses normalised company_name as the cache key. Returns company_id.
    """
    if not company_name or company_name.lower() == "unknown":
        return None
    cache_key = f"__name__{company_name.lower().strip()}"
    if cache_key in company_map:
        return company_map[cache_key]
    with get_connection() as conn:
        # Check by name first
        existing = conn.execute(
            text("SELECT company_id FROM companies WHERE LOWER(company_name) = :name"),
            {"name": company_name.lower().strip()},
        ).fetchone()
        if existing:
            company_id = str(existing[0])
        else:
            row = conn.execute(
                text("""
                    INSERT INTO companies (company_name)
                    VALUES (:name)
                    RETURNING company_id
                """),
                {"name": company_name.strip()},
            ).fetchone()
            company_id = str(row[0])
    company_map[cache_key] = company_id
    return company_id


# ── Shared adapter loop ────────────────────────────────────────────────────────


def _run_adapter(
    adapter: BaseAdapter,
    company_map: dict[str, str],
    stats: dict[str, int],
    label: str = "postings",
) -> None:
    """Process every posting from an adapter, updating stats in place."""
    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        console=console,
    ) as progress:
        task = progress.add_task(f"Processing {label}...", total=None)

        for raw in adapter.fetch():
            stats["processed"] += 1

            if is_duplicate(raw.source_platform, raw.source_id):
                stats["skipped"] += 1
                progress.advance(task)
                continue

            company_id = company_map.get(raw.company_domain or "") or _upsert_company_by_name(
                raw.company_name, company_map
            )
            city = raw.location_city or ""
            posted_date = str(raw.posted_at.date()) if raw.posted_at else "2026-01-01"

            title_norm, family, seniority = normalize_title(raw.title_raw)
            seniority = raw.seniority_level or seniority

            content_hash = compute_content_hash(
                company_id or "unknown", title_norm, city, posted_date
            )
            desc_cleaned = " ".join(raw.description_raw.split())

            job_id = _persist_posting(
                raw, company_id, title_norm, family, seniority, desc_cleaned, content_hash
            )

            if job_id is None:
                stats["skipped"] += 1
            else:
                stats["inserted"] += 1
                skill_count = _persist_skills(job_id, raw.description_raw)
                stats["skills"] += skill_count

            progress.advance(task)


# ── Main runners ───────────────────────────────────────────────────────────────


def run_ingestion(n_seed_postings: int = 400) -> dict[str, int]:
    """Run seed data ingestion (development / demo)."""
    console.print("[bold cyan]▶ JobSignals ingestion pipeline starting...[/]")

    console.print("  [dim]Bootstrapping companies...[/]")
    company_map = _ensure_companies()
    console.print(f"  [green]✓[/] {len(company_map)} companies ready")

    stats = {"processed": 0, "inserted": 0, "skipped": 0, "skills": 0}
    _run_adapter(
        SeedAdapter(n_postings=n_seed_postings),
        company_map,
        stats,
        f"{n_seed_postings} seed postings",
    )

    console.print(
        f"  [green]✓[/] Ingestion complete: "
        f"{stats['inserted']} inserted, {stats['skipped']} skipped, "
        f"{stats['skills']} skills extracted"
    )
    console.print()
    run_all_aggregations()
    return stats


def run_all_sources() -> dict[str, int]:
    """Run all live data sources in sequence: RemoteOK + Remotive + Arbeitnow + Adzuna."""
    console.print("[bold cyan]▶ JobSignals — multi-source ingestion[/]")
    console.print("  [dim]Bootstrapping companies...[/]")
    company_map = _ensure_companies()
    console.print(f"  [green]✓[/] {len(company_map)} companies ready\n")

    total_stats = {"processed": 0, "inserted": 0, "skipped": 0, "skills": 0}

    sources = [
        # ── Free, unlimited — runs every 3 hours ──────────────────────────────
        (RemoteOKAdapter(), "RemoteOK"),
        (RemotiveAdapter(), "Remotive"),
        (ArbeitnowAdapter(), "Arbeitnow"),
        (HimalayasAdapter(), "Himalayas"),
        (JobicyAdapter(), "Jobicy"),
        # ── API key required ──────────────────────────────────────────────────
        (AdzunaAdapter(country="us"), "Adzuna US"),
        (AdzunaAdapter(country="gb"), "Adzuna GB"),
        (AdzunaAdapter(country="in"), "Adzuna IN"),
        (AdzunaAdapter(country="au"), "Adzuna AU"),
        (AdzunaAdapter(country="de"), "Adzuna DE"),
        (AdzunaAdapter(country="ca"), "Adzuna CA"),
        (JoobleAdapter(), "Jooble"),
        # ── Google Jobs via SerpAPI — rotates daily (100 free/month) ─────────
        (SerpAPIAdapter(), "Google Jobs (SerpAPI)"),
    ]

    for adapter, name in sources:
        stats = {"processed": 0, "inserted": 0, "skipped": 0, "skills": 0}
        _run_adapter(adapter, company_map, stats, f"{name} postings")
        console.print(
            f"  [green]✓[/] {name}: {stats['inserted']} inserted, "
            f"{stats['skipped']} skipped, {stats['skills']} skills\n"
        )
        for k in total_stats:
            total_stats[k] += stats[k]

    console.print(
        f"[bold green]✓ All sources done:[/] "
        f"{total_stats['inserted']} inserted, {total_stats['skipped']} skipped, "
        f"{total_stats['skills']} skills"
    )
    console.print()
    run_all_aggregations()
    return total_stats


def run_remoteok_ingestion(min_relevance_tags: int = 0) -> dict[str, int]:
    """Fetch real job postings from RemoteOK and run them through the full pipeline."""
    console.print("[bold cyan]▶ JobSignals — RemoteOK live ingestion[/]")

    console.print("  [dim]Bootstrapping companies...[/]")
    company_map = _ensure_companies()
    console.print(f"  [green]✓[/] {len(company_map)} companies ready")

    stats = {"processed": 0, "inserted": 0, "skipped": 0, "skills": 0}
    _run_adapter(
        RemoteOKAdapter(min_relevance_tags=min_relevance_tags),
        company_map,
        stats,
        "RemoteOK live postings",
    )

    console.print(
        f"  [green]✓[/] RemoteOK ingestion complete: "
        f"{stats['inserted']} inserted, {stats['skipped']} skipped, "
        f"{stats['skills']} skills extracted"
    )
    console.print()
    run_all_aggregations()
    return stats
