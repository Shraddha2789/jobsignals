"""FastAPI dependency injection."""
from __future__ import annotations

from typing import Generator

from sqlalchemy.engine import Connection

from db import get_connection


def get_db() -> Generator[Connection, None, None]:
    with get_connection() as conn:
        yield conn
