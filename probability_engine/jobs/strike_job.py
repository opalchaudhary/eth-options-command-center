import logging


logger = logging.getLogger(__name__)


def run_probability_strike_scan_job():
    logger.info("probability.strike.no_trade", extra={"reason": "V1 scheduled strike persistence is not activated."})
    return {"ok": True, "action": "NO_OP", "reason": "Strike scan persistence is available through services but disabled in scheduler V1."}

