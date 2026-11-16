import os
import sys

import pytest

# The app builds an Elasticsearch client at import time (it does not connect until used)
os.environ.setdefault("ELASTICSEARCH_HOST", "http://localhost:9200")
os.environ.setdefault("DATABASE_PATH", ":memory:")
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))


@pytest.fixture(scope="session")
def client():
    from app import app
    app.config["TESTING"] = True
    return app.test_client()


@pytest.fixture
def hierarchy():
    """The real app with a small hierarchy in a throwaway database: root > parent > child1..3."""
    from app import app
    from dapytains.app.database import Collection, db

    with app.app_context():
        db.drop_all()
        db.create_all()
        root, parent = Collection(identifier="root", title="Root"), Collection(identifier="parent", title="Parent")
        parent.parents.append(root)
        children = [Collection(identifier=f"child{i}", title=f"Child {i}") for i in (1, 2, 3)]
        for child in children:
            child.parents.append(parent)
        db.session.add_all([root, parent, *children])
        db.session.commit()
    yield app
    with app.app_context():
        db.drop_all()


@pytest.fixture(autouse=True)
def isolated_caches(tmp_path, monkeypatch):
    """Every test gets an empty disk cache of its own and empty in-process caches."""
    from app import cache
    monkeypatch.setattr(cache.disk_cache, "directory", str(tmp_path / "api-cache"))
    cache.clear_memory_caches()
    yield
    cache.clear_memory_caches()
