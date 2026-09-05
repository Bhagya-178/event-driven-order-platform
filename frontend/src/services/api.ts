import { Order, OrderTrace, InventoryItem, ServiceHealth, CreateOrderPayload } from '../types';

const getBaseHost = () => {
  if (typeof window !== 'undefined' && window.location && window.location.hostname) {
    return window.location.hostname;
  }
  return 'localhost';
};

const host = getBaseHost();
export const ORDER_API = (import.meta as any).env?.VITE_ORDER_API_URL || `http://${host}:8000`;
export const PAYMENT_API = (import.meta as any).env?.VITE_PAYMENT_API_URL || `http://${host}:8001`;
export const INVENTORY_API = (import.meta as any).env?.VITE_INVENTORY_API_URL || `http://${host}:8002`;

// Production Enterprise Hardware Catalog
export const CATALOG_PRODUCTS = [
  {
    product_id: "11111111-1111-1111-1111-111111111111",
    name: "NVIDIA H100 SXM5 80GB",
    description: "Fourth-generation Tensor Core GPU with dedicated Transformer Engine and 3.35 TB/s HBM3 memory bandwidth.",
    category: "Compute Accelerator",
    price: 32000.00,
    badge: "High Contention",
    specs: "80GB HBM3 • 3.35 TB/s • NVLink 4 • 700W SXM5",
    sku: "NV-H100-SXM5-80G"
  },
  {
    product_id: "22222222-2222-2222-2222-222222222222",
    name: "AMD EPYC™ 9654 Genoa Blade",
    description: "96 Cores / 192 Threads enterprise server processor with 384MB L3 Cache and 12-channel DDR5 memory.",
    category: "Compute Node",
    price: 11800.00,
    badge: "Enterprise Standard",
    specs: "96C / 192T • 3.70 GHz Max • 12x DDR5-4800 • SP5",
    sku: "AMD-EPYC-9654-SP5"
  },
  {
    product_id: "33333333-3333-3333-3333-333333333333",
    name: "Kioxia CD8-R 30.72TB NVMe SSD",
    description: "PCIe 4.0 NVMe read-intensive solid-state drive engineered for ultra-high throughput event stream logs.",
    category: "NVMe Storage",
    price: 3450.00,
    badge: "Sub-ms Latency",
    specs: "30.72TB • 6,500 MB/s Seq Read • 1.25M IOPS • U.3",
    sku: "KIO-CD8R-30TB-U3"
  },
  {
    product_id: "44444444-4444-4444-4444-444444444444",
    name: "YubiKey 5 FIPS HSM Cryptographic Key",
    description: "FIPS 140-2 Level 3 validated hardware security module for zero-trust microservice signing.",
    category: "Hardware Security",
    price: 85.00,
    badge: "FIPS 140-2 L3",
    specs: "USB-A / NFC • RSA 4096 / ECC • PIV Smart Card",
    sku: "YUBI-5-FIPS-SEC"
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
    let errMsg = `HTTP ${res.status}`;
    try {
      const errJson = await res.json();
      errMsg = errJson.message || errJson.detail || JSON.stringify(errJson);
    } catch {
      errMsg = await res.text();
    }
    throw new Error(errMsg);
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
    const res = await fetch(`http://${host}:${port}/health/ready`, { method: 'GET' });
    const latency_ms = Math.round(performance.now() - start);
    return {
      service: name,
      port,
      status: res.ok ? 'UP' : (res.status === 503 ? 'DEGRADED' : 'DOWN'),
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

