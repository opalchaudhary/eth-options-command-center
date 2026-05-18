import threading

import futures_trading_daemon


_lock = threading.Lock()
_stop_event = threading.Event()
_thread = None


def _worker_loop():
    print("Streamlit-hosted futures trading worker started.")

    while not _stop_event.is_set():
        interval_seconds = futures_trading_daemon.daemon_config()

        try:
            evaluation = futures_trading_daemon.run_cycle(interval_seconds)
            print(f"Futures trading cycle: {evaluation.get('action')}")
        except Exception as exc:
            print(f"Futures trading cycle failed: {exc}")

        _stop_event.wait(interval_seconds)


def start_streamlit_futures_trading_worker():
    global _thread

    with _lock:
        if _thread and _thread.is_alive():
            return {
                "running": True,
                "mode": "streamlit-hosted",
            }

        _stop_event.clear()
        _thread = threading.Thread(
            target=_worker_loop,
            name="streamlit-futures-trading-worker",
            daemon=True,
        )
        _thread.start()
        return {
            "running": True,
            "mode": "streamlit-hosted",
        }


def streamlit_futures_trading_worker_status():
    return {
        "running": bool(_thread and _thread.is_alive()),
        "mode": "streamlit-hosted",
    }
