"""
CLI script: fetch real job postings from all sources and run them through the pipeline.
Sources: RemoteOK + Remotive + Arbeitnow
Run: python -m scripts.ingest_real
"""
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from rich.console import Console
from db import check_connection
from pipeline.runner import run_all_sources

console = Console()


def main():
    console.print()
    console.print("[bold]JobSignals — Multi-Source Ingestion[/bold]")
    console.print("  Sources: RemoteOK · Remotive · Arbeitnow · Adzuna (US/GB/IN/AU/DE/CA)")
    console.print("─" * 45)

    if not check_connection():
        console.print("[red]✗ Cannot reach database. Is Docker running?[/]")
        console.print("  Run: [cyan]make db-up[/]")
        sys.exit(1)

    stats = run_all_sources()

    console.print()
    console.print("[bold green]✓ Done![/]")
    console.print(f"  Postings fetched  : {stats['processed']}")
    console.print(f"  Postings inserted : {stats['inserted']}")
    console.print(f"  Postings skipped  : {stats['skipped']}")
    console.print(f"  Skills extracted  : {stats['skills']}")
    console.print()
    console.print("  [cyan]make api[/]  → start the API and explore live data")


if __name__ == "__main__":
    main()
