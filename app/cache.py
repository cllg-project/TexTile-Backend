"""
Small caches for the API.

DiskCache
    JSON files in a directory. Shared by every gunicorn worker (and surviving restarts), which an
    in-process dict would not be. For results that are expensive and stable, such as statistics.
    Entries expire after `ttl` seconds and the oldest are evicted beyond `max_entries`.

ttl_cache
    In-process memoisation with an expiry, for cheap-to-hold data that is read on every request
    (the collection hierarchy, the search facets). Each worker has its own copy; a stale entry
    lives at most `seconds`.

Keys include the index "generation" (see search_routes/generation.py), so re-indexing invalidates
cached results without any explicit step.
"""
import functools
import hashlib
import json
import logging
import os
import tempfile
import threading
import time
from typing import Any, Callable, List, Optional

log = logging.getLogger(__name__)


class DiskCache:
    def __init__(self, directory: str, ttl: int = 24 * 3600, max_entries: int = 2000):
        self.directory = directory
        self.ttl = ttl
        self.max_entries = max_entries

    def _path(self, namespace: str, key: Any) -> str:
        digest = hashlib.sha256(json.dumps(key, sort_keys=True, default=str).encode()).hexdigest()[:40]
        return os.path.join(self.directory, f"{namespace}-{digest}.json")

    def get(self, namespace: str, key: Any) -> Optional[Any]:
        path = self._path(namespace, key)
        try:
            if time.time() - os.stat(path).st_mtime > self.ttl:
                os.remove(path)
                return None
            with open(path, encoding="utf-8") as handle:
                return json.load(handle)
        except FileNotFoundError:
            return None
        except (ValueError, OSError):  # unreadable or half-written by something else: a miss
            log.warning("ignoring unreadable cache entry %s", path, exc_info=True)
            return None

    def set(self, namespace: str, key: Any, value: Any) -> None:
        try:
            os.makedirs(self.directory, exist_ok=True)
            # Write elsewhere then rename: a concurrent reader never sees a partial file
            fd, tmp = tempfile.mkstemp(dir=self.directory, suffix=".tmp")
            with os.fdopen(fd, "w", encoding="utf-8") as handle:
                json.dump(value, handle, ensure_ascii=False)
            os.replace(tmp, self._path(namespace, key))
            self._evict()
        except OSError:  # a cache that cannot write is just a cache that misses
            log.warning("could not write cache entry", exc_info=True)

    def _evict(self) -> None:
        entries = [e for e in os.scandir(self.directory) if e.name.endswith(".json")]
        excess = len(entries) - self.max_entries
        if excess > 0:
            for entry in sorted(entries, key=lambda e: e.stat().st_mtime)[:excess]:
                try:
                    os.remove(entry.path)
                except OSError:
                    pass

    def clear(self) -> None:
        if os.path.isdir(self.directory):
            for entry in os.scandir(self.directory):
                if entry.name.endswith(".json"):
                    os.remove(entry.path)


_registry: List[Callable] = []


def ttl_cache(seconds: float):
    """Memoise a function of hashable arguments for `seconds`. `.cache_clear()` empties it."""
    def decorator(func):
        store = {}
        lock = threading.Lock()

        @functools.wraps(func)
        def wrapper(*args):
            now = time.monotonic()
            with lock:
                hit = store.get(args)
                if hit and hit[0] > now:
                    return hit[1]
            value = func(*args)  # computed outside the lock: two threads may both compute, which is harmless
            with lock:
                store[args] = (time.monotonic() + seconds, value)
            return value

        def cache_clear():
            with lock:
                store.clear()

        wrapper.cache_clear = cache_clear
        _registry.append(wrapper)
        return wrapper
    return decorator


def clear_memory_caches() -> None:
    for func in _registry:
        func.cache_clear()


def default_cache() -> DiskCache:
    base = os.getenv("API_CACHE_DIR") or os.path.join(os.getenv("PRERENDER_DIR") or os.getcwd(), "api-cache")
    return DiskCache(
        base,
        ttl=int(os.getenv("API_CACHE_TTL", str(24 * 3600))),
        max_entries=int(os.getenv("API_CACHE_MAX_ENTRIES", "2000")),
    )


disk_cache = default_cache()
