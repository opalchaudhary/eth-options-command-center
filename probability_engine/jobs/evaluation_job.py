import logging


logger = logging.getLogger(__name__)


def run_probability_performance_job():
    logger.info("probability.model.scored", extra={"action": "NO_OP"})
    return {"ok": True, "action": "NO_OP", "reason": "Performance scaffold is present; scoring requires stored predictions and outcomes."}

