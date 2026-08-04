#!/usr/bin/env bash
set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$PROJECT_ROOT"

source "$PROJECT_ROOT/venv/bin/activate"

export BACKEND_SCHEDULER_ENABLED="${BACKEND_SCHEDULER_ENABLED:-true}"
export BACKEND_JOB_TIMEOUT_SECONDS="${BACKEND_JOB_TIMEOUT_SECONDS:-50}"

python -m uvicorn backend.main:app --host 0.0.0.0 --port 8000
