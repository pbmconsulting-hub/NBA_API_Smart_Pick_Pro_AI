# ============================================================
# SmartPicksProAI — Makefile
# ============================================================
# Convenience targets to set up and run the full application.
#
# Usage:
#   make setup      — install deps + create DB schema
#   make seed       — one-time historical data seed (NBA API)
#   make train      — train ML models (requires seeded data + pipeline)
#   make pipeline   — run the full 6-step ML pipeline
#   make backend    — start the FastAPI backend
#   make frontend   — start the Streamlit frontend
#   make run        — start backend + frontend together
#   make all        — setup → seed → pipeline → train → run
# ============================================================

.PHONY: setup seed train pipeline backend frontend run all help clean

PYTHON  ?= python
PIP     ?= pip
BACKEND  = SmartPicksProAI/backend
FRONTEND = SmartPicksProAI/frontend
PKG_ROOT = SmartPicksProAI

help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-15s\033[0m %s\n", $$1, $$2}'

# ── Setup ─────────────────────────────────────────────────────

setup: ## Install dependencies and create the database schema
	$(PIP) install -r requirements.txt
	cd $(BACKEND) && $(PYTHON) setup_db.py

seed: ## One-time historical data seed from NBA API (~5 min)
	cd $(BACKEND) && $(PYTHON) initial_pull.py

# ── ML Pipeline & Training ───────────────────────────────────

pipeline: ## Run the full 6-step ML pipeline (ingest → export)
	cd $(PKG_ROOT) && $(PYTHON) -m engine.pipeline.run_pipeline

train: ## Train all ML models (Ridge + Ensemble × 8 stat types)
	cd $(PKG_ROOT) && $(PYTHON) -m engine.models.train

# ── Run Servers ───────────────────────────────────────────────

backend: ## Start the FastAPI backend on port 8098
	cd $(BACKEND) && $(PYTHON) -m uvicorn api:app --host 127.0.0.1 --port 8098

frontend: ## Start the Streamlit frontend on port 8501
	cd $(FRONTEND) && streamlit run app.py --server.port 8501

run: ## Start backend (background) + frontend (foreground)
	@echo "Starting backend on http://127.0.0.1:8098 ..."
	cd $(BACKEND) && $(PYTHON) -m uvicorn api:app --host 127.0.0.1 --port 8098 &
	@sleep 2
	@echo "Starting frontend on http://localhost:8501 ..."
	cd $(FRONTEND) && streamlit run app.py --server.port 8501

# ── Full Workflow ─────────────────────────────────────────────

all: setup seed pipeline train run ## Full setup → seed → pipeline → train → run

# ── Clean ─────────────────────────────────────────────────────

clean: ## Remove generated DB, model artifacts, and data caches
	rm -f $(BACKEND)/smartpicks.db
	rm -f $(PKG_ROOT)/engine/models/saved/*.joblib
	rm -f $(PKG_ROOT)/data/raw/*.parquet
	find $(PKG_ROOT)/data/raw -name '*.csv' ! -name 'sample_*.csv' -delete 2>/dev/null || true
	rm -f $(PKG_ROOT)/data/processed/*.parquet
	find $(PKG_ROOT)/data/processed -name '*.csv' ! -name 'sample_*.csv' -delete 2>/dev/null || true
	rm -f $(PKG_ROOT)/data/ml_ready/*.parquet
	find $(PKG_ROOT)/data/ml_ready -name '*.csv' ! -name 'sample_*.csv' -delete 2>/dev/null || true
	@echo "Cleaned generated files. Sample fixtures preserved."
