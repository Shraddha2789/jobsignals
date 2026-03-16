"""
Aggregation pipeline — computes skill_trends and company_signals.
Runs after every ingestion cycle. Designed to be idempotent (UPSERT).
"""
from __future__ import annotations

from datetime import date, timedelta

from rich.console import Console
from sqlalchemy import text

from db import get_connection

console = Console()

WINDOWS = [7, 30, 90, 365]


# ── Skill Trends ──────────────────────────────────────────────────────────────

def compute_skill_trends(reference_date: date | None = None) -> int:
    """
    For each (skill, title_family, window), compute posting_count and posting_share.
    Returns the number of trend rows upserted.
    """
    if reference_date is None:
        reference_date = date.today()

    rows_written = 0

    with get_connection() as conn:
        for window in WINDOWS:
            since = reference_date - timedelta(days=window)

            # Total postings in window (denominator) — exclude synthetic seed data
            total_result = conn.execute(
                text(
                    "SELECT COUNT(*) FROM job_postings "
                    "WHERE is_active = TRUE AND posted_at >= :since "
                    "AND source_platform != 'seed'"
                ),
                {"since": since},
            ).scalar()
            total = total_result or 0
            if total == 0:
                continue

            # Skill counts grouped by title_family
            rows = conn.execute(
                text(
                    """
                    SELECT
                        js.skill_name,
                        jp.title_family,
                        jp.location_country,
                        COUNT(DISTINCT jp.job_id) AS posting_count
                    FROM job_skills js
                    JOIN job_postings jp ON jp.job_id = js.job_id
                    WHERE jp.is_active = TRUE
                      AND jp.posted_at >= :since
                      AND jp.source_platform != 'seed'
                    GROUP BY js.skill_name, jp.title_family, jp.location_country
                    """
                ),
                {"since": since},
            ).fetchall()

            for row in rows:
                skill_name, title_family, country, count = row
                share = round(count / total, 6) if total > 0 else 0.0

                conn.execute(
                    text(
                        """
                        INSERT INTO skill_trends
                            (skill_name, title_family, location_country,
                             period, window_days, posting_count, posting_share, computed_at)
                        VALUES
                            (:skill, :family, :country,
                             :period, :window, :count, :share, NOW())
                        ON CONFLICT (skill_name, title_family, location_country, period, window_days)
                        DO UPDATE SET
                            posting_count = EXCLUDED.posting_count,
                            posting_share = EXCLUDED.posting_share,
                            computed_at   = NOW()
                        """
                    ),
                    {
                        "skill":   skill_name,
                        "family":  title_family,
                        "country": country,
                        "period":  reference_date,
                        "window":  window,
                        "count":   count,
                        "share":   share,
                    },
                )
                rows_written += 1

    # ── MOM / YoY delta pass ───────────────────────────────────────────────
    # After all UPSERTs, back-fill mom_change and yoy_change for the latest period.
    # Tolerance: MOM → find period within ±10 days of reference_date - 30d
    #            YoY → find period within ±30 days of reference_date - 365d
    with get_connection() as conn:
        for window in WINDOWS:
            conn.execute(
                text(
                    """
                    UPDATE skill_trends AS cur
                    SET mom_change = cur.posting_share - prev.posting_share
                    FROM skill_trends AS prev
                    WHERE cur.period       = :ref_date
                      AND cur.window_days  = :window
                      AND prev.window_days = :window
                      AND prev.skill_name        = cur.skill_name
                      AND prev.title_family       IS NOT DISTINCT FROM cur.title_family
                      AND prev.location_country   IS NOT DISTINCT FROM cur.location_country
                      AND prev.period BETWEEN (:ref_date - INTERVAL '40 days')
                                          AND (:ref_date - INTERVAL '20 days')
                    """
                ),
                {"ref_date": reference_date, "window": window},
            )
            conn.execute(
                text(
                    """
                    UPDATE skill_trends AS cur
                    SET yoy_change = cur.posting_share - prev.posting_share
                    FROM skill_trends AS prev
                    WHERE cur.period       = :ref_date
                      AND cur.window_days  = :window
                      AND prev.window_days = :window
                      AND prev.skill_name        = cur.skill_name
                      AND prev.title_family       IS NOT DISTINCT FROM cur.title_family
                      AND prev.location_country   IS NOT DISTINCT FROM cur.location_country
                      AND prev.period BETWEEN (:ref_date - INTERVAL '395 days')
                                          AND (:ref_date - INTERVAL '335 days')
                    """
                ),
                {"ref_date": reference_date, "window": window},
            )

    return rows_written


# ── Company Signals ───────────────────────────────────────────────────────────

def compute_company_signals(reference_date: date | None = None) -> int:
    """
    Compute hiring velocity and top signals per company.
    Returns number of signal rows upserted.
    """
    if reference_date is None:
        reference_date = date.today()

    rows_written = 0

    with get_connection() as conn:
        companies = conn.execute(text("SELECT company_id FROM companies")).fetchall()

        for (company_id,) in companies:
            for window in [30, 90, 365]:
                since = reference_date - timedelta(days=window)

                stats = conn.execute(
                    text(
                        """
                        SELECT
                            COUNT(*)                          AS total_postings,
                            SUM(CASE WHEN is_active THEN 1 ELSE 0 END) AS active_postings,
                            PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY salary_min) AS med_sal_min,
                            PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY salary_max) AS med_sal_max
                        FROM job_postings
                        WHERE company_id = :cid
                          AND posted_at >= :since
                          AND source_platform != 'seed'
                        """
                    ),
                    {"cid": company_id, "since": since},
                ).fetchone()

                if not stats or stats[0] == 0:
                    continue

                total, active, med_min, med_max = stats

                # Hiring velocity: normalize by employee count assumption
                velocity = min(100.0, round((total / window) * 30 * 5, 1))

                # Top skills
                top_skills_rows = conn.execute(
                    text(
                        """
                        SELECT js.skill_name, COUNT(*) AS cnt
                        FROM job_skills js
                        JOIN job_postings jp ON jp.job_id = js.job_id
                        WHERE jp.company_id = :cid AND jp.posted_at >= :since
                          AND jp.source_platform != 'seed'
                        GROUP BY js.skill_name
                        ORDER BY cnt DESC
                        LIMIT 10
                        """
                    ),
                    {"cid": company_id, "since": since},
                ).fetchall()
                top_skills = [r[0] for r in top_skills_rows]

                # Top role families
                top_roles_rows = conn.execute(
                    text(
                        """
                        SELECT title_family, COUNT(*) AS cnt
                        FROM job_postings
                        WHERE company_id = :cid AND posted_at >= :since
                          AND source_platform != 'seed'
                          AND title_family IS NOT NULL
                        GROUP BY title_family
                        ORDER BY cnt DESC
                        LIMIT 5
                        """
                    ),
                    {"cid": company_id, "since": since},
                ).fetchall()
                top_roles = [r[0] for r in top_roles_rows]

                import json
                conn.execute(
                    text(
                        """
                        INSERT INTO company_signals
                            (company_id, period, window_days, total_postings, active_postings,
                             hiring_velocity_score, top_skills, top_roles,
                             median_salary_min, median_salary_max, computed_at)
                        VALUES
                            (:cid, :period, :window, :total, :active,
                             :velocity, :skills, :roles, :med_min, :med_max, NOW())
                        ON CONFLICT (company_id, period, window_days)
                        DO UPDATE SET
                            total_postings        = EXCLUDED.total_postings,
                            active_postings       = EXCLUDED.active_postings,
                            hiring_velocity_score = EXCLUDED.hiring_velocity_score,
                            top_skills            = EXCLUDED.top_skills,
                            top_roles             = EXCLUDED.top_roles,
                            median_salary_min     = EXCLUDED.median_salary_min,
                            median_salary_max     = EXCLUDED.median_salary_max,
                            computed_at           = NOW()
                        """
                    ),
                    {
                        "cid":      company_id,
                        "period":   reference_date,
                        "window":   window,
                        "total":    total,
                        "active":   active,
                        "velocity": velocity,
                        "skills":   json.dumps(top_skills),
                        "roles":    json.dumps(top_roles),
                        "med_min":  int(med_min) if med_min else None,
                        "med_max":  int(med_max) if med_max else None,
                    },
                )
                rows_written += 1

    return rows_written


def run_all_aggregations() -> dict[str, int]:
    console.print("[bold cyan]▶ Running aggregations...[/]")
    trend_rows  = compute_skill_trends()
    signal_rows = compute_company_signals()
    console.print(f"  [green]✓[/] skill_trends: {trend_rows} rows upserted")
    console.print(f"  [green]✓[/] company_signals: {signal_rows} rows upserted")
    return {"skill_trends": trend_rows, "company_signals": signal_rows}
