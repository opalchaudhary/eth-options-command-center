import logging

from probability_engine.services.v2_1_shadow_service import V21ShadowEngine


logger = logging.getLogger(__name__)


def run_probability_v2_1_shadow_job():
    result = V21ShadowEngine().run_shadow_prediction()
    logger.info("probability.v2_1.shadow.completed", extra=result)
    return result
