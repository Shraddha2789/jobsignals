"""
LLM-powered title classifier.

Queries all job_postings where title_family = 'Other' or IS NULL (excluding seed data),
sends batches of titles to Claude, maps the responses back, updates the DB, then
re-runs aggregations.

Run:
    python -m scripts.classify_titles
    python -m scripts.classify_titles --dry-run        # preview only, no DB writes
    python -m scripts.classify_titles --batch-size 30  # default 25
"""
from __future__ import annotations

import argparse
import json
import os
import re
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from rich.console import Console
from rich.progress import Progress, SpinnerColumn, TextColumn
from sqlalchemy import text

from db import check_connection, get_connection
from pipeline.aggregations import run_all_aggregations

console = Console()

VALID_FAMILIES = [
    "Data Engineering",
    "Data Science",
    "ML Engineering",
    "Software Engineering",
    "Product Management",
    "Design",
    "Marketing",
    "Operations",
    "Sales",
    "Finance",
    "HR",
    "Other",
]

SYSTEM_PROMPT = """\
You are a job title classifier. Given a list of job titles, classify each into exactly one
of these role families:

  Data Engineering | Data Science | ML Engineering | Software Engineering |
  Product Management | Design | Marketing | Operations | Sales | Finance | HR | Other

Rules:
- "Software Engineering" covers backend, frontend, fullstack, mobile, DevOps, cloud, QA, security, embedded.
- "Operations" covers project managers, program managers, business analysts, IT ops, RevOps, support ops.
- "Other" only if the title genuinely fits none of the above.
- Respond ONLY with a JSON array of objects: [{"id": <original_id>, "family": "<family>"}]
- No explanation. No markdown fences. Pure JSON array.
"""


def _get_client():
    try:
        import anthropic
        api_key = os.environ.get("ANTHROPIC_API_KEY")
        if not api_key:
            raise RuntimeError("ANTHROPIC_API_KEY not set")
        return anthropic.Anthropic(api_key=api_key)
    except ImportError:
        console.print("[red]✗ anthropic package not installed. Run: pip install anthropic[/]")
        sys.exit(1)


def _classify_batch(client, batch: list[dict]) -> list[dict]:
    """
    Send a batch of {id, title} dicts to Claude and return [{id, family}] results.
    Falls back to 'Other' for any titles that fail to parse.
    """
    user_msg = json.dumps([{"id": item["id"], "title": item["title"]} for item in batch])

    import anthropic
    response = client.messages.create(
        model="claude-opus-4-6",
        max_tokens=1024,
        system=SYSTEM_PROMPT,
        messages=[{"role": "user", "content": user_msg}],
    )

    raw = response.content[0].text.strip()
    # Strip markdown fences if present
    if raw.startswith("```"):
        raw = re.sub(r"^```[a-z]*\n?", "", raw)
        raw = re.sub(r"\n?```$", "", raw)

    results = json.loads(raw)
    # Validate families
    id_to_family = {}
    for r in results:
        fam = r.get("family", "Other")
        if fam not in VALID_FAMILIES:
            fam = "Other"
        id_to_family[r["id"]] = fam

    # Fill in any missing with 'Other'
    return [{"id": item["id"], "family": id_to_family.get(item["id"], "Other")} for item in batch]


def main():
    parser = argparse.ArgumentParser(description="LLM title classifier for 'Other' job postings")
    parser.add_argument("--dry-run", action="store_true", help="Preview without writing to DB")
    parser.add_argument("--batch-size", type=int, default=25, help="Titles per Claude call (default 25)")
    parser.add_argument("--limit", type=int, default=0, help="Max rows to process (0=all)")
    args = parser.parse_args()

    console.print()
    console.print("[bold]JobSignals — LLM Title Classifier[/bold]")
    console.print(f"  Batch size: {args.batch_size}  |  Dry run: {args.dry_run}")
    console.print("─" * 45)

    if not check_connection():
        console.print("[red]✗ Cannot reach database. Is Docker running?[/]")
        sys.exit(1)

    client = _get_client()

    # Fetch all unclassified / 'Other' titles (real data only)
    with get_connection() as conn:
        query = """
            SELECT job_id, title_raw, title_normalized
            FROM job_postings
            WHERE source_platform != 'seed'
              AND (title_family IS NULL OR title_family = 'Other')
            ORDER BY ingested_at DESC
        """
        if args.limit:
            query += f" LIMIT {args.limit}"
        rows = conn.execute(text(query)).fetchall()

    if not rows:
        console.print("[green]✓ No unclassified titles found — nothing to do.[/]")
        return

    console.print(f"  Found [bold]{len(rows)}[/] unclassified titles to process\n")

    # Build batches
    items = [
        {"id": str(r[0]), "title": r[2] or r[1]}   # prefer normalized, fall back to raw
        for r in rows
    ]
    batches = [items[i:i + args.batch_size] for i in range(0, len(items), args.batch_size)]

    updates: list[dict] = []
    errors = 0

    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        console=console,
    ) as progress:
        task = progress.add_task(f"Classifying {len(rows)} titles in {len(batches)} batches...", total=len(batches))

        for batch in batches:
            try:
                results = _classify_batch(client, batch)
                updates.extend(results)
            except Exception as e:
                console.print(f"  [red]✗ Batch failed: {e}[/]")
                errors += 1
                # Mark all in batch as 'Other' so we still update
                updates.extend([{"id": item["id"], "family": "Other"} for item in batch])
            progress.advance(task)

    # Tally
    from collections import Counter
    tally = Counter(u["family"] for u in updates)
    console.print("\n  [bold]Classification results:[/]")
    for fam, cnt in sorted(tally.items(), key=lambda x: -x[1]):
        bar = "█" * min(cnt, 40)
        console.print(f"  {fam:<25} {cnt:>4}  {bar}")

    if args.dry_run:
        console.print("\n  [yellow]Dry run — no DB writes.[/]")
        return

    # Apply updates
    console.print(f"\n  Writing {len(updates)} classifications to DB...")
    with get_connection() as conn:
        for u in updates:
            conn.execute(
                text("UPDATE job_postings SET title_family = :fam WHERE job_id = CAST(:id AS uuid)"),
                {"fam": u["family"], "id": u["id"]},
            )

    console.print(f"  [green]✓[/] {len(updates)} rows updated  ({errors} batch errors)")

    if errors:
        console.print(f"  [yellow]⚠ {errors} batches failed — those rows left as 'Other'[/]")

    # Re-run aggregations with fresh classifications
    console.print()
    run_all_aggregations()
    console.print()
    console.print("[bold green]✓ Classification complete![/]")


if __name__ == "__main__":
    main()
