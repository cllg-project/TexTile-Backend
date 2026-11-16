"""
gunicorn settings for the API (`gunicorn -c gunicorn.conf.py app:app`, see Dockerfile.api).

Why these values:

- `gthread` workers: a statistics stream or a CSV export keeps a request open for seconds or
  minutes. With the default single sync worker that blocks every other request; here only one of
  the worker's threads is busy. (Heavy requests are also capped, see app/limits.py.)
- the request timeout does not cut a long stream: for threaded workers it is a heartbeat,
  not a limit on how long a request may run.
- workers are recycled after a while, which bounds memory growth.

Everything can be overridden from the environment, e.g. `WEB_CONCURRENCY=8 GUNICORN_THREADS=16`.
"""
import multiprocessing
import os


def _int(name, default):
    try:
        return int(os.environ[name])
    except (KeyError, ValueError):
        return default


bind = os.getenv("BIND", "0.0.0.0:5000")

workers = _int("WEB_CONCURRENCY", min(4, multiprocessing.cpu_count() * 2 + 1))
threads = _int("GUNICORN_THREADS", 8)
worker_class = "gthread"

timeout = _int("GUNICORN_TIMEOUT", 120)
graceful_timeout = _int("GUNICORN_GRACEFUL_TIMEOUT", 30)
keepalive = _int("GUNICORN_KEEPALIVE", 5)

max_requests = _int("GUNICORN_MAX_REQUESTS", 2000)
max_requests_jitter = _int("GUNICORN_MAX_REQUESTS_JITTER", 200)

# Heartbeat files in memory: in a container the default (/tmp on the overlay disk) can stall workers
worker_tmp_dir = "/dev/shm" if os.path.isdir("/dev/shm") else None

loglevel = os.getenv("LOG_LEVEL", "info")
accesslog = "-"
errorlog = "-"
capture_output = True
