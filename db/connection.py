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
from sqlalchemy.pool import NullPool, QueuePool

load_dotenv()

_DATABASE_URL: str = os.environ.get(
    "DATABASE_URL",
    "postgresql://jobsignals:jobsignals_dev@localhost:5432/jobsignals",
)

# Vercel sets VERCEL=1; serverless containers can't share a connection pool
# across invocations, so NullPool (one connection per request) is safer.
_IS_SERVERLESS = bool(os.environ.get("VERCEL"))

_engine: Engine | None = None


def get_engine() -> Engine:
    global _engine
    if _engine is None:
        if _IS_SERVERLESS:
            _engine = create_engine(
                _DATABASE_URL,
                poolclass=NullPool,
                connect_args={"connect_timeout": 5},
                echo=False,
            )
        else:
            # TCP keepalives prevent remote hosts (e.g. Neon) from dropping
            # long-running batch connections due to idle timeout.
            _is_remote = "localhost" not in _DATABASE_URL and "127.0.0.1" not in _DATABASE_URL
            connect_args = (
                {
                    "keepalives": 1,
                    "keepalives_idle": 30,
                    "keepalives_interval": 10,
                    "keepalives_count": 5,
                }
                if _is_remote
                else {}
            )
            _engine = create_engine(
                _DATABASE_URL,
                poolclass=QueuePool,
                pool_size=5,
                max_overflow=10,
                pool_pre_ping=True,
                pool_recycle=300,
                connect_args=connect_args,
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
