import logging

from probability_engine.services.v2_shadow_service import V2ShadowEngine


logger = logging.getLogger(__name__)


def run_probability_v2_shadow_job():
    result = V2ShadowEngine().run_shadow_prediction()
    logger.info("probability.v2.shadow.completed", extra=result)
    return result
