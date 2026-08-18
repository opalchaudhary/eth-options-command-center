from dataclasses import dataclass


@dataclass(frozen=True)
class ModelDefinition:
    model_version: str
    feature_version: str
    regime_version: str
    range_model_version: str
    status: str = "CHAMPION"


class ModelRegistry:
    def __init__(self, config):
        self.config = config

    def champion(self):
        return ModelDefinition(
            model_version=self.config.model_version,
            feature_version=self.config.feature_version,
            regime_version=self.config.regime_version,
            range_model_version=self.config.range_model_version,
            status="CHAMPION",
        )

    def challengers(self):
        return []

