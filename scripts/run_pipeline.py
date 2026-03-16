"""
CLI script: run the ingestion + aggregation pipeline once.
Run: python -m scripts.run_pipeline
"""
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from rich.console import Console
from db import check_connection
from pipeline.runner import run_ingestion

console = Console()


def ingest():
    if not check_connection():
        console.print("[red]✗ Database unreachable.[/]")
        sys.exit(1)
    run_ingestion()


if __name__ == "__main__":
    ingest()
