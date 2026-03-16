"""
Simple cron scheduler using the `schedule` library.

Runs:
  - Ingestion pipeline   every 6 hours  (configurable via INGESTION_INTERVAL_SECONDS)
  - Aggregation refresh  every 24 hours (configurable via AGGREGATION_INTERVAL_SECONDS)

Graduate to Airflow/Prefect in Phase 2 by replacing this with proper DAGs.
Run: python -m scripts.scheduler
"""
import logging
import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import schedule
from rich.console import Console
from rich.logging import RichHandler

from db import check_connection
from pipeline.aggregations import run_all_aggregations
from pipeline.runner import run_all_sources

logging.basicConfig(
    level=logging.INFO,
    format="%(message)s",
    handlers=[RichHandler(rich_tracebacks=True)],
)
log = logging.getLogger("scheduler")
console = Console()

INGESTION_INTERVAL  = int(os.environ.get("INGESTION_INTERVAL_SECONDS",  21600))   # 6h
AGGREGATION_INTERVAL = int(os.environ.get("AGGREGATION_INTERVAL_SECONDS", 86400)) # 24h


def _run_ingestion():
    log.info("▶ Scheduled ingestion starting (all sources)...")
    try:
        stats = run_all_sources()
        log.info(f"✓ Ingestion done — {stats['inserted']} inserted, {stats['skipped']} skipped, {stats['skills']} skills")
    except Exception as e:
        log.error(f"✗ Ingestion failed: {e}", exc_info=True)


def _run_aggregations():
    log.info("▶ Scheduled aggregation starting...")
    try:
        result = run_all_aggregations()
        log.info(f"✓ Aggregations done — {result}")
    except Exception as e:
        log.error(f"✗ Aggregation failed: {e}", exc_info=True)


def main():
    console.print("\n[bold cyan]JobSignals Scheduler[/bold cyan]")
    console.print(f"  Ingestion  every {INGESTION_INTERVAL // 3600}h")
    console.print(f"  Aggregation every {AGGREGATION_INTERVAL // 3600}h")
    console.print("  [dim]Ctrl+C to stop[/dim]\n")

    if not check_connection():
        console.print("[red]✗ Database unreachable. Is Docker running?[/]")
        sys.exit(1)

    # Run once immediately on startup
    _run_ingestion()

    # Schedule recurring runs
    schedule.every(INGESTION_INTERVAL).seconds.do(_run_ingestion)
    schedule.every(AGGREGATION_INTERVAL).seconds.do(_run_aggregations)

    while True:
        schedule.run_pending()
        time.sleep(60)


if __name__ == "__main__":
    main()
