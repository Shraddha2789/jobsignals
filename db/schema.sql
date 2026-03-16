-- JobSignals v1.0.0 — PostgreSQL Schema
-- Designed for local Postgres; column types chosen to be BigQuery/Snowflake compatible.
-- UUID primary keys, JSONB for arrays, TIMESTAMPTZ for portability.

-- ── Extensions ────────────────────────────────────────────────────────────────
CREATE EXTENSION IF NOT EXISTS "pgcrypto";

-- ── companies ─────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS companies (
    company_id          UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    company_name        VARCHAR(255) NOT NULL,
    company_aliases     JSONB        DEFAULT '[]',
    domain              VARCHAR(255),
    industry            VARCHAR(100),
    company_stage       VARCHAR(50)
        CHECK (company_stage IN ('startup','series_a','series_b','series_c','growth','public','enterprise')),
    employee_count_range VARCHAR(20)
        CHECK (employee_count_range IN ('1-10','11-50','51-200','201-1000','1001-5000','5000+')),
    hq_country          CHAR(2),
    crunchbase_id       VARCHAR(255),
    created_at          TIMESTAMPTZ  DEFAULT NOW(),
    updated_at          TIMESTAMPTZ  DEFAULT NOW(),
    UNIQUE (domain)
);

-- ── job_postings ──────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS job_postings (
    job_id              UUID         PRIMARY KEY DEFAULT gen_random_uuid(),
    source_id           VARCHAR(255) NOT NULL,
    source_platform     VARCHAR(50)  NOT NULL
        CHECK (source_platform IN ('linkedin','indeed','remoteok','remotive','arbeitnow','adzuna','yc_jobs','career_page','seed','other')),
    source_url          TEXT,

    -- Title
    title_raw           VARCHAR(500) NOT NULL,
    title_normalized    VARCHAR(255),
    title_family        VARCHAR(100)
        CHECK (title_family IN (
            'Data Engineering','Data Science','ML Engineering',
            'Software Engineering','Product Management',
            'Design','Marketing','Operations','Sales','Finance','HR',
            'Other'
        )),

    -- Company
    company_id          UUID         REFERENCES companies(company_id) ON DELETE SET NULL,

    -- Location
    location_raw        VARCHAR(255),
    location_city       VARCHAR(100),
    location_country    CHAR(2)      DEFAULT 'US',
    location_region     VARCHAR(100),
    work_modality       VARCHAR(50)  NOT NULL DEFAULT 'unspecified'
        CHECK (work_modality IN ('remote','hybrid','onsite','unspecified')),

    -- Role metadata
    employment_type     VARCHAR(50)  DEFAULT 'full_time'
        CHECK (employment_type IN ('full_time','part_time','contract','internship')),
    seniority_level     VARCHAR(50)
        CHECK (seniority_level IN ('intern','junior','mid','senior','staff','principal','executive')),

    -- Content
    description_raw     TEXT,
    description_cleaned TEXT,

    -- Salary
    salary_min          INTEGER,
    salary_max          INTEGER,
    salary_currency     CHAR(3)      DEFAULT 'USD',
    salary_source       VARCHAR(50)
        CHECK (salary_source IN ('posted','inferred','glassdoor_enrichment')),

    -- Pipeline metadata
    posted_at           TIMESTAMPTZ,
    ingested_at         TIMESTAMPTZ  DEFAULT NOW(),
    last_seen_at        TIMESTAMPTZ  DEFAULT NOW(),
    is_active           BOOLEAN      DEFAULT TRUE,
    is_deduplicated     BOOLEAN      DEFAULT FALSE,
    content_hash        VARCHAR(64),             -- SHA-256 of (company+title+location+date)
    dataset_version     VARCHAR(20)  DEFAULT '1.0.0',

    UNIQUE (source_platform, source_id)
);

CREATE INDEX IF NOT EXISTS idx_job_postings_company   ON job_postings(company_id);
CREATE INDEX IF NOT EXISTS idx_job_postings_family    ON job_postings(title_family);
CREATE INDEX IF NOT EXISTS idx_job_postings_posted    ON job_postings(posted_at DESC);
CREATE INDEX IF NOT EXISTS idx_job_postings_active    ON job_postings(is_active);
CREATE INDEX IF NOT EXISTS idx_job_postings_modality  ON job_postings(work_modality);
CREATE INDEX IF NOT EXISTS idx_job_postings_seniority ON job_postings(seniority_level);
CREATE INDEX IF NOT EXISTS idx_job_postings_hash      ON job_postings(content_hash);

-- ── job_skills ────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS job_skills (
    skill_id            UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    job_id              UUID        NOT NULL REFERENCES job_postings(job_id) ON DELETE CASCADE,
    skill_name          VARCHAR(255) NOT NULL,
    skill_category      VARCHAR(50)
        CHECK (skill_category IN ('technical','soft','domain','tool','certification')),
    skill_raw           VARCHAR(255),
    is_required         BOOLEAN     DEFAULT TRUE,
    extraction_method   VARCHAR(50) DEFAULT 'rule_based'
        CHECK (extraction_method IN ('rule_based','llm_extracted','manual')),
    confidence_score    FLOAT       DEFAULT 1.0 CHECK (confidence_score BETWEEN 0.0 AND 1.0),
    UNIQUE (job_id, skill_name)
);

CREATE INDEX IF NOT EXISTS idx_job_skills_job   ON job_skills(job_id);
CREATE INDEX IF NOT EXISTS idx_job_skills_skill ON job_skills(skill_name);

-- ── skill_trends ──────────────────────────────────────────────────────────────
-- Pre-aggregated. Refreshed daily by the pipeline.
CREATE TABLE IF NOT EXISTS skill_trends (
    trend_id            UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    skill_name          VARCHAR(255) NOT NULL,
    title_family        VARCHAR(100),           -- NULL = across all families
    location_country    CHAR(2),                -- NULL = global
    period              DATE        NOT NULL,   -- start of aggregation window
    window_days         SMALLINT    NOT NULL    -- 7, 30, 90, 365
        CHECK (window_days IN (7, 30, 90, 365)),
    posting_count       INTEGER     DEFAULT 0,
    posting_share       FLOAT       DEFAULT 0.0,
    mom_change          FLOAT,                  -- month-over-month delta
    yoy_change          FLOAT,                  -- year-over-year delta
    computed_at         TIMESTAMPTZ DEFAULT NOW(),

    UNIQUE (skill_name, title_family, location_country, period, window_days)
);

CREATE INDEX IF NOT EXISTS idx_skill_trends_skill  ON skill_trends(skill_name);
CREATE INDEX IF NOT EXISTS idx_skill_trends_period ON skill_trends(period DESC);
CREATE INDEX IF NOT EXISTS idx_skill_trends_family ON skill_trends(title_family);

-- ── company_signals ───────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS company_signals (
    signal_id           UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    company_id          UUID        NOT NULL REFERENCES companies(company_id) ON DELETE CASCADE,
    period              DATE        NOT NULL,
    window_days         SMALLINT    NOT NULL CHECK (window_days IN (30, 90, 365)),
    total_postings      INTEGER     DEFAULT 0,
    active_postings     INTEGER     DEFAULT 0,
    hiring_velocity_score FLOAT,               -- 0–100 normalized vs company size
    top_skills          JSONB       DEFAULT '[]',
    top_roles           JSONB       DEFAULT '[]',
    median_salary_min   INTEGER,
    median_salary_max   INTEGER,
    computed_at         TIMESTAMPTZ DEFAULT NOW(),

    UNIQUE (company_id, period, window_days)
);

CREATE INDEX IF NOT EXISTS idx_company_signals_company ON company_signals(company_id);
CREATE INDEX IF NOT EXISTS idx_company_signals_period  ON company_signals(period DESC);
