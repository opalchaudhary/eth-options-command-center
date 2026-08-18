from probability_engine.repositories.base_repository import SupabaseRepository


class PerformanceRepository(SupabaseRepository):
    table_name = "probability_model_performance"


class CalibrationRepository(SupabaseRepository):
    table_name = "probability_calibration"


class StrikeRecommendationRepository(SupabaseRepository):
    table_name = "option_strike_recommendations"


class StrikeOutcomeRepository(SupabaseRepository):
    table_name = "option_strike_outcomes"

