#!/usr/bin/env bash
# ============================================================
# SmartPicksProAI — One-Command Launcher
# ============================================================
# Usage:
#   ./start.sh              — setup + backend + frontend
#   ./start.sh --full       — setup + seed + pipeline + train + run
#   ./start.sh --backend    — backend only
#   ./start.sh --frontend   — frontend only
# ============================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BACKEND="$SCRIPT_DIR/SmartPicksProAI/backend"
FRONTEND="$SCRIPT_DIR/SmartPicksProAI/frontend"
PKG_ROOT="$SCRIPT_DIR/SmartPicksProAI"

PYTHON="${PYTHON:-python}"
PIP="${PIP:-pip}"

# ── Helpers ───────────────────────────────────────────────────

info()  { echo -e "\033[36m▸ $*\033[0m"; }
ok()    { echo -e "\033[32m✔ $*\033[0m"; }
fail()  { echo -e "\033[31m✖ $*\033[0m"; exit 1; }

install_deps() {
    info "Installing Python dependencies …"
    $PIP install -r "$SCRIPT_DIR/requirements.txt" --quiet
    ok "Dependencies installed"
}

create_db() {
    info "Creating database schema …"
    (cd "$BACKEND" && $PYTHON setup_db.py)
    ok "Database ready"
}

seed_data() {
    info "Seeding historical data from NBA API (this may take several minutes) …"
    (cd "$BACKEND" && $PYTHON initial_pull.py)
    ok "Data seeded"
}

run_pipeline() {
    info "Running ML pipeline (6 steps) …"
    (cd "$PKG_ROOT" && $PYTHON -m engine.pipeline.run_pipeline)
    ok "Pipeline complete"
}

train_models() {
    info "Training ML models …"
    (cd "$PKG_ROOT" && $PYTHON -m engine.models.train)
    ok "Models trained"
}

start_backend() {
    info "Starting FastAPI backend on http://127.0.0.1:8098 …"
    (cd "$BACKEND" && $PYTHON -m uvicorn api:app --host 127.0.0.1 --port 8098) &
    BACKEND_PID=$!
    sleep 2
    if kill -0 "$BACKEND_PID" 2>/dev/null; then
        ok "Backend running (PID $BACKEND_PID)"
    else
        fail "Backend failed to start"
    fi
}

start_frontend() {
    info "Starting Streamlit frontend on http://localhost:8501 …"
    (cd "$FRONTEND" && streamlit run app.py --server.port 8501)
}

# ── Cleanup on exit ──────────────────────────────────────────

cleanup() {
    if [[ -n "${BACKEND_PID:-}" ]]; then
        info "Stopping backend (PID $BACKEND_PID) …"
        kill "$BACKEND_PID" 2>/dev/null || true
    fi
}
trap cleanup EXIT

# ── Main ─────────────────────────────────────────────────────

MODE="${1:-}"

case "$MODE" in
    --full)
        install_deps
        create_db
        seed_data
        run_pipeline
        train_models
        start_backend
        start_frontend
        ;;
    --backend)
        install_deps
        create_db
        (cd "$BACKEND" && $PYTHON -m uvicorn api:app --host 127.0.0.1 --port 8098)
        ;;
    --frontend)
        (cd "$FRONTEND" && streamlit run app.py --server.port 8501)
        ;;
    "")
        install_deps
        create_db
        start_backend
        start_frontend
        ;;
    *)
        echo "Usage: $0 [--full | --backend | --frontend]"
        exit 1
        ;;
esac
