import math


def clamp(value, low=0.0, high=1.0):
    if value is None:
        return None
    return max(low, min(high, float(value)))


def safe_div(numerator, denominator, default=None):
    try:
        if denominator in [None, 0]:
            return default
        return numerator / denominator
    except Exception:
        return default


def sigmoid(value):
    try:
        return 1 / (1 + math.exp(-value))
    except OverflowError:
        return 0.0 if value < 0 else 1.0

