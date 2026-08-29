import logging

from probability_engine.services.v2_shadow_outcome import V2ShadowOutcomeEvaluator


logger = logging.getLogger(__name__)


def run_probability_v2_shadow_outcome_job():
    result = V2ShadowOutcomeEvaluator().run()
    logger.info("probability.v2.shadow.outcome.evaluated", extra=result)
    return result
