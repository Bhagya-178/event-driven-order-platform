import { Order, OrderTrace, InventoryItem, ServiceHealth, CreateOrderPayload } from '../types';

export const ORDER_API = 'http://localhost:8000';
export const PAYMENT_API = 'http://localhost:8001';
export const INVENTORY_API = 'http://localhost:8002';

// Standard mock products mapped to UUIDs
export const CATALOG_PRODUCTS = [
  {
    product_id: "11111111-1111-1111-1111-111111111111",
    name: "Enterprise Quantum Tensor Accelerator",
    description: "High-density neural compute node with dedicated cryogenic interlinks.",
    category: "Hardware",
    price: 499.00,
    badge: "Flash Sale",
    specs: "8,192 Cores • 128GB HBM3e • PCIe 5.0"
  },
  {
    product_id: "22222222-2222-2222-2222-222222222222",
    name: "Distributed NVMe Storage Appliance",
    description: "Sub-millisecond persistent event log cluster storage blade.",
    category: "Storage",
    price: 249.00,
    badge: "Low Stock",
    specs: "30TB Gen5 NVMe • 14GB/s Read • Dual 100GbE"
  },
  {
    product_id: "33333333-3333-3333-3333-333333333333",
    name: "Mesh Edge Telemetry Gateway",
    description: "Ruggedized IoT event broker proxy for low-latency edge ingestion.",
    category: "Networking",
    price: 129.00,
    badge: "Popular",
    specs: "Quad-Core ARM • Dual SFP+ • Zero-Trust Enclave"
  },
  {
    product_id: "44444444-4444-4444-4444-444444444444",
    name: "Redundant Hardware HSM Security Module",
    description: "FIPS 140-3 Level 4 certified cryptographic signing appliance.",
    category: "Security",
    price: 389.00,
    badge: "Enterprise",
    specs: "Tamper-Evident • Zero-Zeroization • ECC/RSA Key Vault"
  }
];

export async function createOrder(
  payload: CreateOrderPayload,
  idempotencyKey: string
): Promise<{ order: Order; latencyMs: number }> {
  const start = performance.now();
  const res = await fetch(`${ORDER_API}/orders/`, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      'Idempotency-Key': idempotencyKey,
    },
    body: JSON.stringify(payload),
  });
  const latencyMs = Math.round(performance.now() - start);

  if (!res.ok) {
    const errText = await res.text();
    throw new Error(`Failed to create order (${res.status}): ${errText}`);
  }

  const order = await res.json();
  return { order, latencyMs };
}

export async function getOrder(orderId: string): Promise<{ order: Order; latencyMs: number }> {
  const start = performance.now();
  const res = await fetch(`${ORDER_API}/orders/${orderId}`);
  const latencyMs = Math.round(performance.now() - start);

  if (!res.ok) {
    throw new Error(`Failed to fetch order: ${res.status}`);
  }
  const order = await res.json();
  return { order, latencyMs };
}

export async function getOrderTrace(orderId: string): Promise<OrderTrace> {
  const res = await fetch(`${ORDER_API}/orders/${orderId}/trace`);
  if (!res.ok) {
    throw new Error(`Failed to fetch order trace: ${res.status}`);
  }
  return await res.json();
}

export async function listOrders(limit = 20, offset = 0): Promise<Order[]> {
  try {
    const res = await fetch(`${ORDER_API}/orders/?limit=${limit}&offset=${offset}`);
    if (!res.ok) return [];
    return await res.json();
  } catch (err) {
    console.warn("Could not list orders from backend:", err);
    return [];
  }
}

export async function listInventory(): Promise<InventoryItem[]> {
  try {
    const res = await fetch(`${INVENTORY_API}/inventory/`);
    if (!res.ok) return [];
    return await res.json();
  } catch (err) {
    console.warn("Could not fetch inventory from backend:", err);
    return [];
  }
}

export async function restockInventory(productId: string, quantity: number): Promise<InventoryItem | null> {
  const res = await fetch(`${INVENTORY_API}/inventory/restock`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ product_id: productId, quantity }),
  });
  if (!res.ok) throw new Error(`Restock failed: ${res.status}`);
  return await res.json();
}

export async function checkServiceHealth(name: string, port: number): Promise<ServiceHealth> {
  const start = performance.now();
  try {
    const res = await fetch(`http://localhost:${port}/health/ready`, { method: 'GET' });
    const latency_ms = Math.round(performance.now() - start);
    return {
      service: name,
      port,
      status: res.ok ? 'UP' : 'DOWN',
      latency_ms,
    };
  } catch (err) {
    return {
      service: name,
      port,
      status: 'DOWN',
      latency_ms: 0,
    };
  }
}
