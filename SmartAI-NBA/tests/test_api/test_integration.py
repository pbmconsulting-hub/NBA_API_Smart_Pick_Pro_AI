"""tests/test_api/test_integration.py – Integration tests for the FastAPI API."""
import pytest

pytestmark = pytest.mark.asyncio


@pytest.fixture
def anyio_backend():
    return "asyncio"


def _skip_if_no_fastapi():
    try:
        import fastapi  # noqa: F401
        import httpx  # noqa: F401
    except ImportError:
        pytest.skip("fastapi or httpx not installed")


class TestHealthEndpoint:
    def test_health_schema(self):
        _skip_if_no_fastapi()
        import httpx
        from api.main import app
        with httpx.Client(app=app, base_url="http://test") as client:
            resp = client.get("/health")
        assert resp.status_code == 200
        data = resp.json()
        assert "status" in data
        assert data["status"] in ("healthy", "degraded")
        assert "timestamp" in data

    def test_health_has_version_when_healthy(self):
        _skip_if_no_fastapi()
        import httpx
        from api.main import app
        with httpx.Client(app=app, base_url="http://test") as client:
            resp = client.get("/health")
        data = resp.json()
        if data["status"] == "healthy":
            assert "version" in data


class TestPredictionsEndpoint:
    def test_today_predictions_returns_list(self):
        _skip_if_no_fastapi()
        import httpx
        from api.main import app
        with httpx.Client(app=app, base_url="http://test") as client:
            resp = client.get("/predictions/today")
        assert resp.status_code == 200
        data = resp.json()
        assert "date" in data
        assert "predictions" in data
        assert isinstance(data["predictions"], list)

    def test_predictions_by_date_valid(self):
        _skip_if_no_fastapi()
        import httpx
        from api.main import app
        with httpx.Client(app=app, base_url="http://test") as client:
            resp = client.get("/predictions/2025-01-15")
        assert resp.status_code == 200

    def test_predictions_by_date_invalid(self):
        _skip_if_no_fastapi()
        import httpx
        from api.main import app
        with httpx.Client(app=app, base_url="http://test") as client:
            resp = client.get("/predictions/not-a-date")
        assert resp.status_code == 400


class TestPlayersEndpoint:
    def test_player_stats_not_found(self):
        _skip_if_no_fastapi()
        import httpx
        from api.main import app
        with httpx.Client(app=app, base_url="http://test") as client:
            resp = client.get("/players/Nonexistent%20Player%20XYZ/stats")
        # Either 404 or 200 with empty data is acceptable
        assert resp.status_code in (200, 404)
