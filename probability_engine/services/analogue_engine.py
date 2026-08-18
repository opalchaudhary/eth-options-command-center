import math


DEFAULT_FEATURES = ["return_1h", "return_4h", "vwap_zscore", "atr_pct", "volume_zscore", "iv_rv_spread", "cvd_slope", "book_imbalance"]


class AnalogueEngine:
    def __init__(self, feature_names=None, recency_lambda=0.002):
        self.feature_names = feature_names or DEFAULT_FEATURES
        self.recency_lambda = recency_lambda

    def vector(self, snapshot):
        return [getattr(snapshot, name, None) for name in self.feature_names]

    def similarity(self, left, right):
        pairs = [(a, b) for a, b in zip(left, right) if a is not None and b is not None]
        if not pairs:
            return 0.0
        distance = math.sqrt(sum((float(a) - float(b)) ** 2 for a, b in pairs) / len(pairs))
        return 1 / (1 + distance)

    def select(self, current_snapshot, historical_snapshots, count=300):
        current = self.vector(current_snapshot)
        scored = []
        for index, snapshot in enumerate(historical_snapshots or []):
            score = self.similarity(current, self.vector(snapshot))
            age_weight = math.exp(-self.recency_lambda * index)
            scored.append((score * age_weight, snapshot))
        return [snapshot for _, snapshot in sorted(scored, key=lambda item: item[0], reverse=True)[:count]]

