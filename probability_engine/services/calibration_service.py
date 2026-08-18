from probability_engine.services.performance_service import calibration_buckets, calibration_error


class CalibrationService:
    def summarize(self, predictions, outcomes):
        return {
            "calibration_error": calibration_error(predictions, outcomes),
            "buckets": calibration_buckets(predictions, outcomes),
        }

