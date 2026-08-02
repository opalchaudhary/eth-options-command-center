import logging
import time
from functools import wraps


logger = logging.getLogger(__name__)
_CACHE = {}


def _freeze(value):
    if isinstance(value, dict):
        return tuple(sorted((key, _freeze(item)) for key, item in value.items()))
    if isinstance(value, (list, tuple)):
        return tuple(_freeze(item) for item in value)
    return value


def cache_key(name, args, kwargs):
    return name, _freeze(args), _freeze(kwargs)


def ttl_cache(ttl_seconds):
    def decorator(fn):
        @wraps(fn)
        def wrapper(*args, **kwargs):
            key = cache_key(fn.__name__, args, kwargs)
            now = time.monotonic()
            cached = _CACHE.get(key)
            if cached and now - cached["created_at"] <= ttl_seconds:
                age = now - cached["created_at"]
                logger.info("cache_hit endpoint=%s age_seconds=%.2f", fn.__name__, age)
                value = cached["value"]
                if isinstance(value, dict):
                    return {**value, "cache": {"status": "hit", "age_seconds": round(age, 2)}}
                return value

            logger.info("cache_miss endpoint=%s", fn.__name__)
            value = fn(*args, **kwargs)
            _CACHE[key] = {"created_at": now, "value": value}
            if isinstance(value, dict):
                return {**value, "cache": {"status": "miss", "age_seconds": 0}}
            return value

        return wrapper

    return decorator


def clear_cache():
    _CACHE.clear()
