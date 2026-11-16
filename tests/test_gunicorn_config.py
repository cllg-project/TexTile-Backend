"""gunicorn.conf.py: the settings the streaming endpoints rely on."""
import os
import runpy

import pytest

CONFIG = os.path.join(os.path.dirname(os.path.dirname(__file__)), "gunicorn.conf.py")
KEYS = ("WEB_CONCURRENCY", "GUNICORN_THREADS", "GUNICORN_TIMEOUT", "LOG_LEVEL", "BIND", "GUNICORN_MAX_REQUESTS")


@pytest.fixture
def load(monkeypatch):
    def run(**env):
        for key in KEYS:
            monkeypatch.delenv(key, raising=False)
        for key, value in env.items():
            monkeypatch.setenv(key, value)
        return runpy.run_path(CONFIG)
    return run


def test_defaults_serve_streams_without_blocking_everything(load):
    conf = load()
    assert conf["worker_class"] == "gthread"          # a long stream occupies one thread, not the worker
    assert conf["threads"] >= 2 and 1 <= conf["workers"] <= 4
    assert conf["timeout"] >= 60                      # not the 30 s default
    assert conf["max_requests"] > 0                   # workers are recycled
    assert conf["loglevel"] == "info"                 # not the debug level the image used to run with
    assert conf["accesslog"] == "-" and conf["errorlog"] == "-"
    assert conf["bind"] == "0.0.0.0:5000"


def test_everything_can_be_overridden_from_the_environment(load):
    conf = load(WEB_CONCURRENCY="7", GUNICORN_THREADS="3", GUNICORN_TIMEOUT="300", LOG_LEVEL="warning",
                BIND="127.0.0.1:9000", GUNICORN_MAX_REQUESTS="10")
    assert (conf["workers"], conf["threads"], conf["timeout"]) == (7, 3, 300)
    assert (conf["loglevel"], conf["bind"], conf["max_requests"]) == ("warning", "127.0.0.1:9000", 10)


def test_a_bad_number_falls_back_to_the_default(load):
    assert load(GUNICORN_THREADS="lots")["threads"] == 8


def test_the_image_starts_gunicorn_with_this_file():
    dockerfile = open(os.path.join(os.path.dirname(CONFIG), "Dockerfile.api")).read()
    assert '"gunicorn", "-c", "gunicorn.conf.py"' in dockerfile
    assert "COPY gunicorn.conf.py" in dockerfile
    assert "--log-level" not in dockerfile.split("CMD [")[-1]
