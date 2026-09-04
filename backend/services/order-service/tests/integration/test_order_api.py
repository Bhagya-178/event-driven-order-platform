import pytest
from uuid import uuid4

@pytest.mark.asyncio
async def test_create_order_success(async_client):
    customer_id = str(uuid4())
    product_id = str(uuid4())
    payload = {
        "customer_id": customer_id,
        "items": [
            {
                "product_id": product_id,
                "quantity": 2
            }
        ]
    }

    response = await async_client.post("/orders/", json=payload)
    
    assert response.status_code == 201
    data = response.json()
    assert data["customer_id"] == customer_id
    assert data["status"] == "CREATED"
    assert len(data["items"]) == 1
    # 2 quantity * 10.00 mock price = 20.00
    assert data["total_amount"] == "20.00"

@pytest.mark.asyncio
async def test_create_order_invalid_input(async_client):
    payload = {
        "customer_id": "not-a-uuid",
        "items": []
    }
    response = await async_client.post("/orders/", json=payload)
    assert response.status_code == 422

@pytest.mark.asyncio
async def test_get_order_not_found(async_client):
    random_id = str(uuid4())
    response = await async_client.get(f"/orders/{random_id}")
    assert response.status_code == 404

@pytest.mark.asyncio
async def test_create_and_get_order(async_client):
    payload = {
        "customer_id": str(uuid4()),
        "items": [
            {"product_id": str(uuid4()), "quantity": 1}
        ]
    }
    create_resp = await async_client.post("/orders/", json=payload)
    assert create_resp.status_code == 201
    order_id = create_resp.json()["id"]

    get_resp = await async_client.get(f"/orders/{order_id}")
    assert get_resp.status_code == 200
    assert get_resp.json()["id"] == order_id

@pytest.mark.asyncio
async def test_create_order_idempotency_key(async_client):
    idempotency_key = str(uuid4())
    payload = {
        "customer_id": str(uuid4()),
        "items": [
            {"product_id": str(uuid4()), "quantity": 3}
        ]
    }

    # First request
    resp1 = await async_client.post(
        "/orders/",
        json=payload,
        headers={"Idempotency-Key": idempotency_key}
    )
    assert resp1.status_code == 201
    order1 = resp1.json()

    # Second request with same idempotency key
    resp2 = await async_client.post(
        "/orders/",
        json=payload,
        headers={"Idempotency-Key": idempotency_key}
    )
    assert resp2.status_code == 201
    order2 = resp2.json()

    # Must return identical order
    assert order1["id"] == order2["id"]
    assert order1["customer_id"] == order2["customer_id"]
import pytest
from uuid import uuid4

@pytest.mark.asyncio
async def test_create_order_success(async_client):
    customer_id = str(uuid4())
    product_id = str(uuid4())
    payload = {
        "customer_id": customer_id,
        "items": [
            {
                "product_id": product_id,
                "quantity": 2
            }
        ]
    }

    response = await async_client.post("/orders/", json=payload)
    
    assert response.status_code == 201
    data = response.json()
    assert data["customer_id"] == customer_id
    assert data["status"] == "CREATED"
    assert len(data["items"]) == 1
    # 2 quantity * 10.00 mock price = 20.00
    assert data["total_amount"] == "20.00"

@pytest.mark.asyncio
async def test_create_order_invalid_input(async_client):
    payload = {
        "customer_id": "not-a-uuid",
        "items": []
    }
    response = await async_client.post("/orders/", json=payload)
    assert response.status_code == 422

@pytest.mark.asyncio
async def test_get_order_not_found(async_client):
    random_id = str(uuid4())
    response = await async_client.get(f"/orders/{random_id}")
    assert response.status_code == 404

@pytest.mark.asyncio
async def test_create_and_get_order(async_client):
    payload = {
        "customer_id": str(uuid4()),
        "items": [
            {"product_id": str(uuid4()), "quantity": 1}
        ]
    }
    create_resp = await async_client.post("/orders/", json=payload)
    assert create_resp.status_code == 201
    order_id = create_resp.json()["id"]

    get_resp = await async_client.get(f"/orders/{order_id}")
    assert get_resp.status_code == 200
    assert get_resp.json()["id"] == order_id

@pytest.mark.asyncio
async def test_create_order_idempotency_key(async_client):
    idempotency_key = str(uuid4())
    payload = {
        "customer_id": str(uuid4()),
        "items": [
            {"product_id": str(uuid4()), "quantity": 3}
        ]
    }

    # First request
    resp1 = await async_client.post(
        "/orders/",
        json=payload,
        headers={"Idempotency-Key": idempotency_key}
    )
    assert resp1.status_code == 201
    order1 = resp1.json()

    # Second request with same idempotency key
    resp2 = await async_client.post(
        "/orders/",
        json=payload,
        headers={"Idempotency-Key": idempotency_key}
    )
    assert resp2.status_code == 201
    order2 = resp2.json()

    # Must return identical order
    assert order1["id"] == order2["id"]
    assert order1["customer_id"] == order2["customer_id"]

@pytest.mark.asyncio
async def test_create_order_idempotency_key(async_client):
    idempotency_key = str(uuid4())
    payload = {
        "customer_id": str(uuid4()),
        "items": [
            {"product_id": str(uuid4()), "quantity": 3}
        ]
    }

    # First request
    resp1 = await async_client.post(
        "/orders/",
        json=payload,
        headers={"Idempotency-Key": idempotency_key}
    )
    assert resp1.status_code == 201
    order1 = resp1.json()

    # Second request with same idempotency key
    resp2 = await async_client.post(
        "/orders/",
        json=payload,
        headers={"Idempotency-Key": idempotency_key}
    )
    assert resp2.status_code == 201
    order2 = resp2.json()

    # Must return identical order
    assert order1["id"] == order2["id"]
    assert order1["customer_id"] == order2["customer_id"]
import pytest
from uuid import uuid4

@pytest.mark.asyncio
async def test_create_order_success(async_client):
    customer_id = str(uuid4())
    product_id = str(uuid4())
    payload = {
        "customer_id": customer_id,
        "items": [
            {
                "product_id": product_id,
                "quantity": 2
            }
        ]
    }

    response = await async_client.post("/orders/", json=payload)
    
    assert response.status_code == 201
    data = response.json()
    assert data["customer_id"] == customer_id
    assert data["status"] == "CREATED"
    assert len(data["items"]) == 1
    # 2 quantity * 10.00 mock price = 20.00
    assert data["total_amount"] == "20.00"

@pytest.mark.asyncio
async def test_create_order_invalid_input(async_client):
    payload = {
        "customer_id": "not-a-uuid",
        "items": []
    }
    response = await async_client.post("/orders/", json=payload)
    assert response.status_code == 422

@pytest.mark.asyncio
async def test_get_order_not_found(async_client):
    random_id = str(uuid4())
    response = await async_client.get(f"/orders/{random_id}")
    assert response.status_code == 404

@pytest.mark.asyncio
async def test_create_and_get_order(async_client):
    payload = {
        "customer_id": str(uuid4()),
        "items": [
            {"product_id": str(uuid4()), "quantity": 1}
        ]
    }
    create_resp = await async_client.post("/orders/", json=payload)
    assert create_resp.status_code == 201
    order_id = create_resp.json()["id"]

    get_resp = await async_client.get(f"/orders/{order_id}")
    assert get_resp.status_code == 200
    assert get_resp.json()["id"] == order_id

