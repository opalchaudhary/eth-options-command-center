from .repository import repository


def run_gridbot_heartbeat_job():
    active = repository.active_run()
    return {
        "ok": True,
        "mode": "testnet",
        "active_run_id": active.run_id if active else None,
        "active_status": active.status.value if active else None,
        "bot_count": len(repository.bots),
        "run_count": len(repository.runs),
    }

