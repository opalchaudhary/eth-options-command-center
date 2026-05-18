# Futures Trading Daemon

The Futures Trading paper engine runs without a button or page action.

When the Streamlit app is awake, `app.py` and `pages/Futures_Trading.py` start one process-local background worker with:

```bash
futures_trading_runtime.start_streamlit_futures_trading_worker()
```

That worker calls `futures_trading_daemon.run_cycle()` every 60 seconds by default. Each cycle marks open futures positions to market, updates stops, closes invalidated trades, evaluates the latest setup, opens at most one ETH futures paper position, writes wallet snapshots, and records a heartbeat in `futures_trading_engine_runs`.

Streamlit Cloud may sleep after inactivity. When it wakes again, the worker restarts automatically. To keep futures trading running outside the website process, run this on always-on compute:

```bash
python futures_trading_daemon.py
```

Environment variable:

```bash
FUTURES_TRADING_INTERVAL_SECONDS=60
```

The included `Procfile` defines a separate process type:

```bash
futuresworker: python futures_trading_daemon.py
```

This is paper trading only. It never sends real orders.
