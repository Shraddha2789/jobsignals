# ── JobSignals Makefile ───────────────────────────────────────────────────────
PYTHON := .venv/bin/python
PIP    := .venv/bin/pip

.PHONY: help setup db-up db-down db-reset seed ingest api scheduler dashboard classify test lint cron-install cron-uninstall cron-classify cron-classify-uninstall launchd-install launchd-uninstall automate git-setup push status

help:
	@echo ""
	@echo "  JobSignals — available commands"
	@echo "  ──────────────────────────────────────────"
	@echo "  make setup      Install Python deps + copy .env"
	@echo "  make db-up      Start PostgreSQL + pgAdmin via Docker"
	@echo "  make db-down    Stop containers"
	@echo "  make db-reset   Wipe and recreate the database"
	@echo "  make seed       Seed 400 job postings + run aggregations"
	@echo "  make ingest     Fetch real jobs from RemoteOK + run aggregations"
	@echo "  make api        Start the FastAPI server (port 8000)"
	@echo "  make scheduler  Start the background cron scheduler"
	@echo "  make test       Run pytest"
	@echo "  make dashboard  Serve the analytics dashboard (port 3000)"
	@echo "  make classify   Run LLM title classifier on unclassified postings"
	@echo "  make lint       Run ruff linter"
	@echo ""
	@echo "  ── Git / Deploy ──────────────────────────────"
	@echo "  make git-setup            Install pre-commit hooks (run once)"
	@echo "  make push MSG='message'   Stage all → commit → push to GitHub"
	@echo "  make status               Show git status + recent commits"
	@echo ""
	@echo "  ── Automation ───────────────────────────────"
	@echo "  make automate          Install ALL automation in one command"
	@echo "  make cron-install      Daily ingestion cron (06:00)"
	@echo "  make cron-uninstall    Remove daily cron"
	@echo "  make cron-classify     Weekly LLM classify cron (Sun 07:00)"
	@echo "  make launchd-install   API as macOS service (auto-start + restart)"
	@echo "  make launchd-uninstall Remove API service"
	@echo ""

setup:
	@echo "→ Creating Python 3.11 virtual environment..."
	/opt/homebrew/bin/python3.11 -m venv .venv
	@echo "→ Installing dependencies..."
	$(PIP) install -q pydantic pydantic-settings faker rich schedule sqlalchemy \
		psycopg2-binary fastapi "uvicorn[standard]" python-dotenv tenacity httpx click pytest httpx
	@[ -f .env ] || cp .env.example .env
	@echo "✓ Setup complete. Run 'make db-up' next."

db-up:
	docker compose up -d
	@echo "✓ PostgreSQL running on :5432"
	@echo "  pgAdmin at http://localhost:5050  (admin / admin)"

db-down:
	docker compose down

db-reset:
	docker compose down -v
	docker compose up -d
	@echo "✓ Database wiped and recreated. Run 'make seed' to reload data."

seed:
	$(PYTHON) -m scripts.seed_data

ingest:
	$(PYTHON) -m scripts.ingest_real

api:
	$(PYTHON) -m api.main

scheduler:
	$(PYTHON) -m scripts.scheduler

dashboard:
	@echo "→ Dashboard at http://localhost:3000"
	@echo "  (API must also be running: make api)"
	python3 -m http.server 3000 --directory dashboard

classify:
	$(PYTHON) -m scripts.classify_titles

test:
	$(PYTHON) -m pytest tests/ -v

lint:
	$(PYTHON) -m ruff check .

cron-install:
	@chmod +x scripts/daily_cron.sh
	@CRON_HOUR=$${CRON_HOUR:-6}; \
	CRON_LINE="0 $$CRON_HOUR * * * $(shell pwd)/scripts/daily_cron.sh"; \
	( crontab -l 2>/dev/null | grep -v "daily_cron.sh"; echo "$$CRON_LINE" ) | crontab -; \
	echo "✓ Cron installed: daily at $$CRON_HOUR:00"

cron-uninstall:
	( crontab -l 2>/dev/null | grep -v "daily_cron.sh" ) | crontab -
	@echo "✓ Daily ingest cron removed"

cron-classify:
	@bash scripts/install_classify_cron.sh install

cron-classify-uninstall:
	@bash scripts/install_classify_cron.sh uninstall

launchd-install:
	@bash scripts/install_launchd.sh install

launchd-uninstall:
	@bash scripts/install_launchd.sh uninstall

# ── Git workflow ──────────────────────────────────────────────────────────────

git-setup:
	@echo "→ Installing pre-commit hooks..."
	$(PIP) install -q pre-commit
	$(PYTHON) -m pre_commit install
	@echo "✓ Pre-commit hooks installed."
	@echo "  Hooks run automatically before every commit."
	@echo "  To run manually: pre-commit run --all-files"

# Usage: make push MSG="your commit message"
push:
	@[ "$(MSG)" ] || (echo "❌  Usage: make push MSG='your commit message'" && exit 1)
	@echo "→ Staging all changes..."
	git add -A
	@echo "→ Committing: $(MSG)"
	git commit -m "$(MSG)"
	@echo "→ Pushing to GitHub (main)..."
	git push
	@echo ""
	@echo "✓ Pushed. GitHub Actions will now:"
	@echo "  1. Run lint + tests"
	@echo "  2. Deploy to Railway if tests pass"
	@echo "  Track progress: https://github.com/Shraddha2789/jobsignals/actions"

status:
	@git status --short
	@echo ""
	@git log --oneline -5

# ─────────────────────────────────────────────────────────────────────────────

# One-shot: install everything — daily ingest + weekly classify + API as service
automate: cron-install cron-classify launchd-install
	@echo ""
	@echo "✓ Full automation installed:"
	@echo "  → API server    : starts at login, restarts on crash (launchd)"
	@echo "  → Daily ingest  : every day at 06:00 (cron)"
	@echo "  → Weekly classify: every Sunday at 07:00 (cron)"
	@echo ""
	@echo "  Logs: logs/api.log · logs/cron.log · logs/classify.log"
	@echo "  Check status: launchctl list | grep jobsignals"
	@echo "  View crons:   crontab -l"
