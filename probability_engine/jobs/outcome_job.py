import logging


logger = logging.getLogger(__name__)


def run_probability_outcome_job():
    logger.info("probability.outcome.evaluated", extra={"action": "NO_OP"})
    return {"ok": True, "action": "NO_OP", "reason": "Outcome labeling scaffold is present; live evaluator remains read-only until migration is deployed."}

