"""GET/POST /v1/insights — AI-powered job market analysis with real tool calling."""
from __future__ import annotations

import json
import os
import time
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from openai import OpenAI
from sqlalchemy import text
from sqlalchemy.engine import Connection

from api.deps import get_db
from api.schemas.responses import APIResponse, InsightOut, InsightRequest
from pipeline.normalization.skill_extractor import SKILLS_TAXONOMY

router = APIRouter(prefix="/insights", tags=["Insights"])

GROQ_BASE_URL = "https://api.groq.com/openai/v1"
DEFAULT_MODEL  = "llama-3.1-8b-instant"
MAX_TOOL_ROUNDS = 3   # max agentic loop iterations before forcing a final answer

CACHE_TTL = 600  # 10 minutes

_insight_cache: dict[str, tuple[float, object]] = {}


# ── Tool schemas ───────────────────────────────────────────────────────────────

TOOLS = [
    {
        "type": "function",
        "function": {
            "name": "get_skill_trends",
            "description": (
                "Get the most in-demand skills from recent job postings. "
                "Returns skill name, category, posting share %, MoM change, and posting count. "
                "Call this multiple times with different country or title_family values to compare markets."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "title_family": {
                        "type": "string",
                        "description": (
                            "Filter to a role family: 'Data Engineering', 'Data Science', "
                            "'ML Engineering', 'Software Engineering', 'Product Management', "
                            "'Design', 'Marketing', 'Operations', 'Sales', 'Finance', 'HR'"
                        ),
                    },
                    "country": {
                        "type": "string",
                        "description": (
                            "ISO 2-letter country code to scope the query: "
                            "US, IN (India), GB (UK), DE (Germany), CA (Canada), AU (Australia). "
                            "Omit for global data across all countries."
                        ),
                    },
                    "window_days": {
                        "type": "integer",
                        "description": "Lookback window in days. One of: 7, 30, 90, 365. Default 30.",
                    },
                    "limit": {
                        "type": "integer",
                        "description": "Max skills to return. Default 15, max 25.",
                    },
                },
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "get_job_summary",
            "description": (
                "Get aggregate job market statistics: total active postings, modality breakdown "
                "(remote/hybrid/onsite), seniority distribution, top role families, and top hiring companies. "
                "Call with different country values to compare markets side-by-side."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "title_family": {
                        "type": "string",
                        "description": "Filter by role family",
                    },
                    "country": {
                        "type": "string",
                        "description": "ISO 2-letter code. Omit for global.",
                    },
                    "window_days": {
                        "type": "integer",
                        "description": "Lookback window in days. One of: 7, 30, 90, 365.",
                    },
                },
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "get_salary_benchmark",
            "description": (
                "Get salary percentiles (p25 / median / p75) broken down by seniority level for a role family. "
                "title_family is required. Best results for US, GB, CA, AU where salaries are posted. "
                "IN and DE have sparse salary data."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "title_family": {"type": "string"},
                    "country": {
                        "type": "string",
                        "description": "ISO 2-letter code. Defaults to US if omitted.",
                    },
                    "window_days": {"type": "integer", "description": "Lookback window: 30, 90, or 365 days."},
                },
                "required": ["title_family"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "get_company_signals",
            "description": (
                "Get hiring velocity, top skills demanded, and top role families for a specific company. "
                "Use when the user asks about a particular company's hiring activity."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "company_name": {
                        "type": "string",
                        "description": "Partial or full company name (case-insensitive search)",
                    },
                    "window_days": {"type": "integer", "description": "Lookback window: 30, 90, or 365 days."},
                },
                "required": ["company_name"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "get_market_overview",
            "description": (
                "Get a high-level market snapshot: total postings, active postings, unique skills count, "
                "companies hiring, top role family, salary coverage %, and data source breakdown. "
                "Good for overview and summary questions."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "country": {
                        "type": "string",
                        "description": "ISO 2-letter code. Omit for global.",
                    },
                },
            },
        },
    },
]


# ── Tool implementations ───────────────────────────────────────────────────────

def _tool_skill_trends(db: Connection, args: dict, context_country: Optional[str] = None) -> dict:
    title_family = args.get("title_family")
    raw_country  = args.get("country") or context_country
    country      = raw_country.upper() if raw_country else None
    window       = int(args.get("window_days", 30))
    limit        = min(int(args.get("limit", 15)), 25)

    base_conds = ["window_days = :window"]
    params: dict = {"window": window, "limit": limit}
    if title_family:
        base_conds.append("title_family = :family")
        params["family"] = title_family
    if country:
        base_conds.append("location_country = :country")
        params["country"] = country

    latest = db.execute(
        text(f"SELECT MAX(period) FROM skill_trends WHERE {' AND '.join(base_conds)}"),
        params,
    ).scalar()

    if not latest:
        return {
            "available": False,
            "message": (
                f"No skill trend data found for "
                f"country={country or 'global'}, family={title_family or 'all'}, window={window}d."
            ),
        }

    base_conds.append("period = :period")
    params["period"] = latest

    if title_family:
        rows = db.execute(
            text(
                f"""
                SELECT skill_name, posting_count, posting_share, mom_change
                FROM skill_trends
                WHERE {' AND '.join(base_conds)}
                ORDER BY posting_share DESC NULLS LAST
                LIMIT :limit
                """
            ),
            params,
        ).fetchall()
    else:
        # Aggregate across all families to avoid duplicates
        country_cond = "AND st.location_country = :country" if country else ""
        rows = db.execute(
            text(
                f"""
                WITH totals AS (
                    SELECT COUNT(*) AS n FROM job_postings
                    WHERE is_active = TRUE AND source_platform != 'seed'
                    {'AND location_country = :country' if country else ''}
                )
                SELECT st.skill_name,
                       SUM(st.posting_count)                          AS posting_count,
                       SUM(st.posting_count)::float / NULLIF(t.n, 0) AS posting_share,
                       NULL::float                                    AS mom_change
                FROM skill_trends st, totals t
                WHERE st.window_days = :window
                  AND st.period = :period
                  {country_cond}
                GROUP BY st.skill_name, t.n
                ORDER BY posting_count DESC NULLS LAST
                LIMIT :limit
                """
            ),
            params,
        ).fetchall()

    return {
        "scope": f"country={country or 'GLOBAL'} | family={title_family or 'all'} | window={window}d",
        "period": str(latest),
        "skills": [
            {
                "skill": r[0],
                "category": SKILLS_TAXONOMY.get(r[0], {}).get("category") or "other",
                "posting_count": int(r[1]),
                "posting_share_pct": round(float(r[2] or 0) * 100, 2),
                **({"mom_change_pct": round(float(r[3]) * 100, 1)} if r[3] is not None else {}),
            }
            for r in rows
        ],
    }


def _tool_job_summary(db: Connection, args: dict, context_country: Optional[str] = None) -> dict:
    title_family = args.get("title_family")
    raw_country  = args.get("country") or context_country
    country      = raw_country.upper() if raw_country else None
    window       = int(args.get("window_days", 30))

    conds  = [
        "is_active = TRUE",
        f"posted_at >= NOW() - INTERVAL '{window} days'",
        "source_platform != 'seed'",
    ]
    params: dict = {}
    if title_family:
        conds.append("title_family = :family")
        params["family"] = title_family
    if country:
        conds.append("location_country = :country")
        params["country"] = country

    where = " AND ".join(conds)

    total    = db.execute(text(f"SELECT COUNT(*) FROM job_postings WHERE {where}"), params).scalar() or 0
    modality = {
        r[0] or "unspecified": r[1]
        for r in db.execute(
            text(f"SELECT work_modality, COUNT(*) FROM job_postings WHERE {where} GROUP BY 1 ORDER BY 2 DESC"),
            params,
        ).fetchall()
    }
    seniority = {
        r[0] or "unspecified": r[1]
        for r in db.execute(
            text(
                f"SELECT seniority_level, COUNT(*) FROM job_postings "
                f"WHERE {where} GROUP BY 1 ORDER BY 2 DESC LIMIT 6"
            ),
            params,
        ).fetchall()
    }
    top_families = [
        {"family": r[0], "postings": r[1]}
        for r in db.execute(
            text(
                f"""
                SELECT title_family, COUNT(*) AS cnt FROM job_postings
                WHERE {where} AND title_family IS NOT NULL AND title_family != 'Other'
                GROUP BY title_family ORDER BY cnt DESC LIMIT 8
                """
            ),
            params,
        ).fetchall()
    ]
    top_companies = [
        {"company": r[0], "postings": r[1]}
        for r in db.execute(
            text(
                f"""
                SELECT c.company_name, COUNT(*) AS cnt
                FROM job_postings jp
                JOIN companies c ON c.company_id = jp.company_id
                WHERE {where}
                GROUP BY c.company_name ORDER BY cnt DESC LIMIT 10
                """
            ),
            params,
        ).fetchall()
    ]

    return {
        "scope": f"country={country or 'GLOBAL'} | family={title_family or 'all'} | last {window}d",
        "total_active_postings": total,
        "modality_breakdown": modality,
        "seniority_distribution": seniority,
        "top_role_families": top_families,
        "top_hiring_companies": top_companies,
    }


def _tool_salary_benchmark(db: Connection, args: dict, context_country: Optional[str] = None) -> dict:
    title_family = args.get("title_family")
    raw_country  = args.get("country") or context_country
    country      = raw_country.upper() if raw_country else "US"
    window       = int(args.get("window_days", 90))

    if not title_family:
        return {"error": "title_family is required for salary benchmarks."}

    rows = db.execute(
        text(
            """
            SELECT seniority_level,
                   COUNT(*)                                                       AS n,
                   PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY salary_min)::int AS p25,
                   PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY salary_min)::int AS p50,
                   PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY salary_max)::int AS p75
            FROM job_postings
            WHERE title_family     = :family
              AND location_country = :country
              AND salary_min IS NOT NULL
              AND posted_at >= NOW() - INTERVAL '1 day' * :window
            GROUP BY seniority_level HAVING COUNT(*) >= 3
            ORDER BY MIN(salary_min)
            """
        ),
        {"family": title_family, "country": country, "window": window},
    ).fetchall()

    if not rows:
        return {
            "available": False,
            "message": (
                f"Insufficient salary data for {title_family} in {country} "
                f"(last {window}d). Salary data is primarily available for US, GB, CA, AU."
            ),
        }

    currency_map = {
        "US": "USD", "GB": "GBP", "CA": "CAD", "AU": "AUD",
        "DE": "EUR", "IN": "INR", "SG": "SGD",
    }
    return {
        "title_family": title_family,
        "country": country,
        "currency": currency_map.get(country, "USD"),
        "by_seniority": [
            {
                "seniority": r[0] or "unspecified",
                "sample_size": r[1],
                "p25": r[2],
                "median": r[3],
                "p75": r[4],
            }
            for r in rows
        ],
    }


def _tool_company_signals(db: Connection, args: dict, context_country: Optional[str] = None) -> dict:
    company_name = args.get("company_name", "")
    window       = int(args.get("window_days", 90))

    company = db.execute(
        text(
            "SELECT company_id, company_name, industry FROM companies "
            "WHERE company_name ILIKE :name LIMIT 1"
        ),
        {"name": f"%{company_name}%"},
    ).fetchone()

    if not company:
        return {"error": f"Company '{company_name}' not found in the database."}

    cid = str(company[0])
    signals = db.execute(
        text(
            """
            SELECT total_postings, active_postings, hiring_velocity_score, top_skills, top_roles
            FROM company_signals
            WHERE company_id = :cid AND window_days = :window
            ORDER BY period DESC LIMIT 1
            """
        ),
        {"cid": cid, "window": window},
    ).fetchone()

    if not signals:
        count = db.execute(
            text(
                f"SELECT COUNT(*) FROM job_postings "
                f"WHERE company_id = :cid AND posted_at >= NOW() - INTERVAL '{window} days'"
            ),
            {"cid": cid},
        ).scalar() or 0
        return {
            "company": company[1], "industry": company[2],
            "window_days": window, "total_postings": count,
        }

    top_skills = signals[3] if isinstance(signals[3], list) else (json.loads(signals[3]) if signals[3] else [])
    top_roles  = signals[4] if isinstance(signals[4], list) else (json.loads(signals[4]) if signals[4] else [])

    return {
        "company": company[1],
        "industry": company[2],
        "window_days": window,
        "total_postings": signals[0],
        "active_postings": signals[1],
        "hiring_velocity_score": round(float(signals[2] or 0), 1),
        "top_skills": top_skills[:8],
        "top_roles": top_roles[:5],
    }


def _tool_market_overview(db: Connection, args: dict, context_country: Optional[str] = None) -> dict:
    raw_country = args.get("country") or context_country
    country     = raw_country.upper() if raw_country else None

    p: dict = {}
    cc = "AND location_country = :country" if country else ""
    if country:
        p["country"] = country

    total    = db.execute(text(f"SELECT COUNT(*) FROM job_postings WHERE source_platform != 'seed' {cc}"), p).scalar() or 0
    active   = db.execute(text(f"SELECT COUNT(*) FROM job_postings WHERE is_active = TRUE AND source_platform != 'seed' {cc}"), p).scalar() or 0
    n_skills = db.execute(
        text(
            f"SELECT COUNT(DISTINCT js.skill_name) FROM job_skills js "
            f"JOIN job_postings jp ON jp.job_id = js.job_id "
            f"WHERE jp.source_platform != 'seed' {cc}"
        ),
        p,
    ).scalar() or 0
    n_companies = db.execute(
        text(
            f"SELECT COUNT(DISTINCT company_id) FROM job_postings "
            f"WHERE is_active = TRUE AND source_platform != 'seed' {cc} AND company_id IS NOT NULL"
        ),
        p,
    ).scalar() or 0
    top_family = db.execute(
        text(
            f"SELECT title_family, COUNT(*) AS cnt FROM job_postings "
            f"WHERE source_platform != 'seed' {cc} AND title_family IS NOT NULL AND title_family != 'Other' "
            f"GROUP BY title_family ORDER BY cnt DESC LIMIT 1"
        ),
        p,
    ).fetchone()
    sources = {
        r[0]: r[1]
        for r in db.execute(
            text(
                f"SELECT source_platform, COUNT(*) FROM job_postings "
                f"WHERE source_platform != 'seed' {cc} GROUP BY source_platform ORDER BY 2 DESC"
            ),
            p,
        ).fetchall()
    }
    salary_pct = 0.0
    if total:
        with_salary = db.execute(
            text(f"SELECT COUNT(*) FROM job_postings WHERE source_platform != 'seed' {cc} AND salary_min IS NOT NULL"),
            p,
        ).scalar() or 0
        salary_pct = round(with_salary / total * 100, 1)

    return {
        "scope": country or "GLOBAL",
        "total_postings": total,
        "active_postings": active,
        "unique_skills_tracked": n_skills,
        "companies_hiring": n_companies,
        "top_role_family": top_family[0] if top_family else None,
        "salary_data_coverage_pct": salary_pct,
        "data_sources": sources,
    }


# ── Tool dispatcher ────────────────────────────────────────────────────────────

def _execute_tool(name: str, args: dict, db: Connection, context_country: Optional[str] = None) -> str:
    # Groq's llama model sometimes serialises integer args as strings — coerce them.
    _INT_ARGS = {"window_days", "limit", "window", "top_n"}
    def _to_int(v):
        try: return int(float(v))
        except (ValueError, TypeError): return v
    args = {k: (_to_int(v) if k in _INT_ARGS and isinstance(v, str) else v) for k, v in (args or {}).items()}

    dispatch = {
        "get_skill_trends":    _tool_skill_trends,
        "get_job_summary":     _tool_job_summary,
        "get_salary_benchmark": _tool_salary_benchmark,
        "get_company_signals": _tool_company_signals,
        "get_market_overview": _tool_market_overview,
    }
    fn = dispatch.get(name)
    if not fn:
        return json.dumps({"error": f"Unknown tool: {name}"})
    try:
        return json.dumps(fn(db, args, context_country))
    except Exception as exc:
        return json.dumps({"error": str(exc)})


# ── LLM client ─────────────────────────────────────────────────────────────────

def _get_client() -> OpenAI:
    api_key = os.environ.get("GROQ_API_KEY")
    if not api_key:
        raise HTTPException(
            status_code=503,
            detail="GROQ_API_KEY not configured. Set it in your .env file.",
        )
    return OpenAI(api_key=api_key, base_url=GROQ_BASE_URL)


# ── System prompt ──────────────────────────────────────────────────────────────

_SYSTEM_PROMPT = """\
You are JobSignals AI — a professional job market intelligence analyst with access \
to a real-time database of job postings across multiple countries.

## Data available
- **Countries**: US, IN (India), GB (UK), DE (Germany), CA (Canada), AU (Australia)
- **Role families**: Data Engineering, Data Science, ML Engineering, Software Engineering, \
Product Management, Design, Marketing, Operations, Sales, Finance, HR
- **Signals**: skill demand (posting share, MoM change), job volume, modality split, \
seniority distribution, top hiring companies, salary benchmarks (US/GB/CA/AU)
- **Time windows**: 7d, 30d, 90d, 365d

## How to answer
1. **Always call tools first** — never answer from memory or make up numbers.
2. **Call multiple tools** when comparison or breadth is needed:
   - Country comparison → call the same tool twice with different country codes.
   - Full picture → call get_market_overview then get_skill_trends then get_job_summary.
   - Salary question → call get_salary_benchmark (title_family required).
3. **Be comprehensive and specific**:
   - Cite exact numbers, percentages, company names.
   - Use **bold** for key figures and skill names.
   - Structure with headers (##), bullet lists, and ranked tables where helpful.
   - Highlight trends, anomalies, and actionable takeaways.
4. **Response length**: 350–700 words. Short answers for simple questions, thorough for complex ones.
5. If data is unavailable (e.g., sparse salary data for India), say so and provide the \
   best available alternative.

## Important
- The user's country filter is a **context hint**, not a restriction.
  You have full access to all country data — use it to answer any comparison question.
- For "compare X and Y" questions, fetch data for both and present a structured comparison.
"""


# ── Agentic runner ─────────────────────────────────────────────────────────────

def _run_agentic_insight(
    question: str,
    title_family: Optional[str],
    context_country: Optional[str],
    window: int,
    db: Connection,
    client: OpenAI,
    model: str,
) -> tuple[str, list[str]]:
    """
    Agentic tool-calling loop.
    The LLM decides which tools to call and in what order.
    Runs up to MAX_TOOL_ROUNDS; then forces a final synthesis.
    """
    # Build context suffix for the system prompt
    ctx_parts = []
    if context_country:
        ctx_parts.append(f"User is currently viewing the **{context_country}** market.")
    if title_family:
        ctx_parts.append(f"Suggested role focus: {title_family}.")
    ctx_parts.append(f"Default lookback window: {window}d.")
    context_note = "\n\n**Current session context**: " + "  ".join(ctx_parts) if ctx_parts else ""

    messages: list[dict] = [
        {"role": "system", "content": _SYSTEM_PROMPT + context_note},
        {"role": "user",   "content": question},
    ]
    sources_used: set[str] = set()

    for _round in range(MAX_TOOL_ROUNDS):
        response = client.chat.completions.create(
            model=model,
            max_tokens=1200,
            messages=messages,
            tools=TOOLS,
            tool_choice="auto",
        )
        msg = response.choices[0].message

        # No tool calls → LLM produced its final answer
        if not msg.tool_calls:
            return msg.content or "No analysis returned.", list(sources_used)

        # Append the assistant turn (with tool call requests)
        messages.append({
            "role": "assistant",
            "content": msg.content or "",
            "tool_calls": [
                {
                    "id": tc.id,
                    "type": "function",
                    "function": {
                        "name": tc.function.name,
                        "arguments": tc.function.arguments,
                    },
                }
                for tc in msg.tool_calls
            ],
        })

        # Execute every tool call and feed results back
        for tc in msg.tool_calls:
            fn_name = tc.function.name
            try:
                fn_args = json.loads(tc.function.arguments) or {}
            except (json.JSONDecodeError, TypeError):
                fn_args = {}

            result = _execute_tool(fn_name, fn_args, db, context_country)
            sources_used.add(fn_name)

            messages.append({
                "role": "tool",
                "tool_call_id": tc.id,
                "content": result,
            })

    # Exhausted rounds — force a final synthesis
    messages.append({
        "role": "user",
        "content": (
            "Based on all the data you have gathered, please provide your "
            "comprehensive final analysis now."
        ),
    })
    final = client.chat.completions.create(
        model=model,
        max_tokens=2048,
        messages=messages,
    )
    return final.choices[0].message.content or "Analysis complete.", list(sources_used)


# ── Shared entry point ─────────────────────────────────────────────────────────

def _run_insight(
    question: str,
    title_family: Optional[str],
    context_country: Optional[str],
    window: int,
    db: Connection,
) -> APIResponse:
    _cache_key = f"{question}|{title_family or ''}|{context_country or ''}|{window}"
    _now = time.time()
    if _cache_key in _insight_cache and _now - _insight_cache[_cache_key][0] < CACHE_TTL:
        return _insight_cache[_cache_key][1]

    client = _get_client()
    model  = os.environ.get("INSIGHTS_MODEL", DEFAULT_MODEL)

    try:
        analysis, sources = _run_agentic_insight(
            question, title_family, context_country, window, db, client, model
        )
    except HTTPException:
        raise
    except Exception as e:
        cls = type(e).__name__
        if "AuthenticationError" in cls or "401" in str(e):
            raise HTTPException(status_code=503, detail="Invalid GROQ_API_KEY.")
        if "RateLimitError" in cls or "429" in str(e):
            raise HTTPException(status_code=429, detail="Rate limit reached. Try again shortly.")
        raise HTTPException(status_code=502, detail=f"LLM API error: {e}")

    result = APIResponse(
        data=InsightOut(question=question, analysis=analysis, sources=sources, model=model)
    )
    _insight_cache[_cache_key] = (time.time(), result)
    return result


# ── Endpoints ──────────────────────────────────────────────────────────────────

@router.get("", response_model=APIResponse[InsightOut])
def get_insight_get(
    q:            str            = Query(..., description="Your job market question"),
    title_family: Optional[str]  = Query(None, description="Scope to a role family"),
    country:      Optional[str]  = Query(
        None,
        description=(
            "2-letter country context hint (e.g. US, IN, GB). "
            "Omit for global scope. The AI can still access all countries regardless."
        ),
    ),
    window:       int            = Query(30, enum=[7, 30, 90, 365]),
    db:           Connection     = Depends(get_db),
):
    """GET version — used by the dashboard."""
    return _run_insight(q, title_family, country.upper() if country else None, window, db)


@router.post("", response_model=APIResponse[InsightOut])
def get_insight_post(req: InsightRequest, db: Connection = Depends(get_db)):
    """POST version — used by API clients / docs."""
    return _run_insight(
        req.question,
        req.title_family,
        req.country.upper() if req.country else None,
        req.window or 30,
        db,
    )
