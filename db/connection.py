"""
Database connection layer.

Uses SQLAlchemy Core with a connection-string abstraction so that
swapping DATABASE_URL to BigQuery/Snowflake/Neon requires zero code changes.
"""
from __future__ import annotations

import os
from contextlib import contextmanager
from typing import Generator

from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Connection, Engine

load_dotenv()

_DATABASE_URL: str = os.environ.get(
    "DATABASE_URL",
    "postgresql://jobsignals:jobsignals_dev@localhost:5432/jobsignals",
)

_engine: Engine | None = None


def get_engine() -> Engine:
    global _engine
    if _engine is None:
        _engine = create_engine(
            _DATABASE_URL,
            pool_size=5,
            max_overflow=10,
            pool_pre_ping=True,   # validates connections before use
            echo=False,
        )
    return _engine


@contextmanager
def get_connection() -> Generator[Connection, None, None]:
    """Yield a transactional connection. Auto-commits on clean exit, rolls back on exception."""
    engine = get_engine()
    with engine.begin() as conn:
        yield conn


def check_connection() -> bool:
    """Return True if the database is reachable."""
    try:
        with get_connection() as conn:
            conn.execute(text("SELECT 1"))
        return True
    except Exception:
        return False
