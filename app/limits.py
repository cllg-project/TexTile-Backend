"""
Cap how many heavy requests run at once, so a few statistics or exports cannot starve the API.

The limit is per process: with `WEB_CONCURRENCY` gunicorn workers the whole server allows
`workers * limit`. A request over the limit is refused at once (HTTP 503 + Retry-After) instead of
queueing behind work that can take minutes.
"""
import os
import threading


class ConcurrencyLimit:
    def __init__(self, name: str, maximum: int):
        self.name = name
        self.maximum = maximum
        self._slots = threading.BoundedSemaphore(maximum)

    def try_acquire(self) -> bool:
        return self._slots.acquire(blocking=False)

    def release(self) -> None:
        self._slots.release()


def _env_int(name: str, default: int) -> int:
    try:
        return max(1, int(os.getenv(name, default)))
    except ValueError:
        return default


STATS_LIMIT = ConcurrencyLimit("statistics", _env_int("STATS_MAX_CONCURRENT", 2))
EXPORT_LIMIT = ConcurrencyLimit("exports", _env_int("EXPORT_MAX_CONCURRENT", 4))

RETRY_AFTER_SECONDS = 5


def busy_response(limit: ConcurrencyLimit):
    """The answer for a request refused because `limit` is reached."""
    from flask import jsonify
    response = jsonify({"error": f"The server is busy computing other {limit.name}; try again in a few seconds"})
    response.status_code = 503
    response.headers["Retry-After"] = str(RETRY_AFTER_SECONDS)
    return response
