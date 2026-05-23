#!/usr/bin/env bash
set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$PROJECT_ROOT"

source "$PROJECT_ROOT/venv/bin/activate"

export FUTURES_LIVE_TRADING_ENABLED="${FUTURES_LIVE_TRADING_ENABLED:-false}"
export BACKEND_SCHEDULER_ENABLED="${BACKEND_SCHEDULER_ENABLED:-true}"
export BACKEND_JOB_TIMEOUT_SECONDS="${BACKEND_JOB_TIMEOUT_SECONDS:-50}"
export FASTAPI_BACKEND_URL="${FASTAPI_BACKEND_URL:-http://127.0.0.1:8000}"

streamlit run app.py --server.port 8501 --server.address 0.0.0.0
