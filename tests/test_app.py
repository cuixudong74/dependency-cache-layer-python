"""End-to-end tests for the FastAPI read path converged onto the cache layer."""

from __future__ import annotations

from fastapi.testclient import TestClient

import main


def test_read_path_through_cache_layer():
    with TestClient(main.app) as client:
        r = client.get("/")
        assert r.status_code == 200
        assert r.json()["service"] == "dependency-cache-layer"

        # First read: miss -> loaded from upstream.
        r = client.get("/users/1/summary")
        assert r.status_code == 200
        body = r.json()
        assert body["summary"] == {"name": "alice", "title": "engineer"}
        assert body["status"] == "hit"
        assert body["freshness"] == "fresh"

        # Second read: served from cache.
        r = client.get("/users/1/summary")
        assert r.status_code == 200
        assert r.json()["status"] == "hit"


def test_not_found_returns_404():
    with TestClient(main.app) as client:
        r = client.get("/users/999/summary")
        assert r.status_code == 404
        assert "not found" in r.json()["detail"]


def test_admin_update_invalidates_dependents():
    with TestClient(main.app) as client:
        client.get("/users/1/summary")  # populate cache

        r = client.put("/admin/users/1", json={"name": "alice2"})
        assert r.status_code == 200
        assert r.json()["invalidated_entries"] == 1

        r = client.get("/users/1/summary")
        assert r.status_code == 200
        assert r.json()["summary"]["name"] == "alice2"


def test_admin_profile_update_invalidates():
    with TestClient(main.app) as client:
        client.get("/users/2/summary")
        r = client.put("/admin/profiles/2", json={"title": "senior operator"})
        assert r.status_code == 200
        assert r.json()["invalidated_entries"] == 1

        r = client.get("/users/2/summary")
        assert r.status_code == 200
        assert r.json()["summary"]["title"] == "senior operator"


def test_metrics_endpoint():
    with TestClient(main.app) as client:
        client.get("/users/1/summary")
        r = client.get("/admin/metrics")
        assert r.status_code == 200
        metrics = r.json()["metrics"]
        assert metrics["misses"] >= 1
        assert metrics["refresh_success"] >= 1
        assert r.json()["cached_entries"] >= 1
