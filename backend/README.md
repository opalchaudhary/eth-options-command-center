# FastAPI Backend

This backend moves market-data fetching, analytics refreshes, Supabase writes, and paper/futures trading cycles out of Streamlit.

## Run Locally

From the project root:

```bash
pip install -r backend/requirements.txt
python -m uvicorn backend.main:app --reload --port 8000
```

Required environment variables:

```bash
DELTA_API_KEY=...
DELTA_API_SECRET=...
SUPABASE_URL=...
SUPABASE_KEY=...
```

You can copy `backend/.env.example` to either `.env` in the project root or `backend/.env` for local development. System environment variables take priority over file values.

The current Delta read endpoints still work with public market data, but authenticated actions should use the Delta variables.

## Test Health

Open this URL after starting FastAPI:

```text
http://localhost:8000/health
```

Expected response shape:

```json
{
  "ok": true,
  "service": "eth-options-command-center-api",
  "delta": {
    "api_key_configured": true,
    "api_secret_configured": true
  },
  "supabase": {
    "url_configured": true,
    "key_configured": true
  }
}
```

## Scheduler

FastAPI starts APScheduler automatically when the API process starts.

Default background jobs:

- market refresh every 60 seconds
- paper trading cycle every 60 seconds
- futures simulation cycle every 60 seconds

Each job has an execution lock, APScheduler `max_instances=1`, coalescing, timeout status tracking, and structured logs. Check scheduler state here:

```text
http://localhost:8000/system/status
```

Scheduler controls:

```bash
BACKEND_SCHEDULER_ENABLED=true
BACKEND_JOB_TIMEOUT_SECONDS=50
```

## Streamlit Connection

Streamlit reads the backend URL from:

```bash
FASTAPI_BACKEND_URL=http://localhost:8000
```

If the variable is missing, the frontend defaults to `http://localhost:8000`.

The frontend should call:

- `GET /market/eth`
- `GET /option-chain`
- `GET /insights`
- `GET /paper-trading/status`
- `POST /paper-trading/run-cycle`
- `GET /futures-trading/status`
- `POST /futures-trading/run-cycle`

## DigitalOcean Path

A later deployment can run FastAPI as its own service on a Droplet or App Platform container, then set `FASTAPI_BACKEND_URL` in Streamlit to the public backend URL. Keep Supabase tables unchanged; this backend reuses the existing storage modules and migrations.
