"""
CLI script: seed the database with development data.
Run: python -m scripts.seed_data
"""
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from rich.console import Console
from db import check_connection
from pipeline.runner import run_ingestion

console = Console()


def main(n: int = 400):
    console.print("\n[bold]JobSignals — Seed Data[/bold]")
    console.print("─" * 40)

    if not check_connection():
        console.print("[red]✗ Cannot reach database. Is Docker running?[/]")
        console.print("  Run: [cyan]make db-up[/]")
        sys.exit(1)

    console.print(f"[dim]Generating {n} realistic job postings...[/]")
    stats = run_ingestion(n_seed_postings=n)

    console.print()
    console.print("[bold green]✓ Seed complete![/]")
    console.print(f"  Postings inserted : {stats['inserted']}")
    console.print(f"  Postings skipped  : {stats['skipped']}")
    console.print(f"  Skills extracted  : {stats['skills']}")
    console.print()
    console.print("Next steps:")
    console.print("  [cyan]make api[/]          → start the API server")
    console.print("  [cyan]open http://localhost:8000/docs[/]  → explore endpoints")


if __name__ == "__main__":
    import sys
    n = int(sys.argv[1]) if len(sys.argv) > 1 else 400
    main(n)
