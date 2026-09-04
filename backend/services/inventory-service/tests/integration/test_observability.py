import pytest
from httpx import AsyncClient, ASGITransport
from app.main import app

@pytest.mark.asyncio
async def test_inventory_metrics_endpoint():
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.get("/metrics")
        assert response.status_code == 200
        assert "http_requests_total" in response.text

@pytest.mark.asyncio
async def test_inventory_health_probes():
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        live = await client.get("/health/live")
        assert live.status_code == 200
        assert live.json() == {"status": "ok"}

        ready = await client.get("/health/ready")
        assert ready.status_code == 200
        data = ready.json()
        assert data["status"] == "ready"
        assert data["database"] == "up"
