import logging

from probability_engine.services.outcome_evaluator import LiveOutcomeEvaluator


logger = logging.getLogger(__name__)


def run_probability_outcome_job():
    result = LiveOutcomeEvaluator().run()
    logger.info("probability.outcome.evaluated", extra=result)
    return result
