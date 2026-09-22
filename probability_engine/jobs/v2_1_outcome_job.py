import logging

from probability_engine.services.v2_1_shadow_outcome import V21ShadowOutcomeEvaluator


logger = logging.getLogger(__name__)


def run_probability_v2_1_shadow_outcome_job():
    result = V21ShadowOutcomeEvaluator().run()
    logger.info("probability.v2_1.shadow.outcome.evaluated", extra=result)
    return result
