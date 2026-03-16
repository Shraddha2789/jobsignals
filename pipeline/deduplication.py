"""
Deduplication layer.

Strategy:
  1. Exact match on content_hash = SHA-256(company_id + title_normalized + city + date)
  2. Cross-source: if (source_platform, source_id) pair already exists, skip insert.

Phase 2: add LSH near-duplicate detection on description_cleaned.
"""
from __future__ import annotations

import hashlib

from sqlalchemy import text

from db import get_connection


def compute_content_hash(company_id: str, title_normalized: str, city: str, date: str) -> str:
    raw = f"{company_id}|{title_normalized.lower()}|{city.lower()}|{date}"
    return hashlib.sha256(raw.encode()).hexdigest()


def is_duplicate(source_platform: str, source_id: str) -> bool:
    """Return True if this (platform, source_id) pair is already in the database."""
    with get_connection() as conn:
        result = conn.execute(
            text(
                "SELECT 1 FROM job_postings "
                "WHERE source_platform = :platform AND source_id = :sid LIMIT 1"
            ),
            {"platform": source_platform, "sid": source_id},
        ).fetchone()
    return result is not None


def hash_exists(content_hash: str) -> bool:
    """Return True if a posting with this content hash already exists."""
    with get_connection() as conn:
        result = conn.execute(
            text("SELECT 1 FROM job_postings WHERE content_hash = :h LIMIT 1"),
            {"h": content_hash},
        ).fetchone()
    return result is not None
