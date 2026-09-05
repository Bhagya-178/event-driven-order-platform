export type OrderStatus = 'CREATED' | 'PAYMENT_PENDING' | 'INVENTORY_PENDING' | 'CONFIRMED' | 'FAILED' | 'CANCELLED';
export type PaymentStatus = 'PENDING' | 'SUCCEEDED' | 'FAILED';
export type InventoryStatus = 'PENDING' | 'RESERVED' | 'FAILED';

export interface OrderItem {
  id: string;
  product_id: string;
  quantity: number;
  unit_price: string;
  total_price: string;
}

export interface Order {
  id: string;
  customer_id: string;
  status: OrderStatus;
  payment_status: PaymentStatus;
  inventory_status: InventoryStatus;
  idempotency_key?: string | null;
  currency: string;
  total_amount: string;
  created_at: string;
  updated_at: string;
  items: OrderItem[];
}

export interface OutboxEventTrace {
  id: string;
  event_type: string;
  status: string;
  retry_count: number;
  created_at: string;
  published_at?: string | null;
  correlation_id?: string | null;
  payload?: Record<string, any> | null;
}

export interface SagaMilestone {
  step: string;
  service: string;
  status: 'COMPLETED' | 'IN_PROGRESS' | 'FAILED' | 'PENDING';
  timestamp?: string | null;
  detail: string;
}

export interface OrderTrace {
  order: Order;
  saga_timeline: SagaMilestone[];
  outbox_events: OutboxEventTrace[];
}

export interface InventoryItem {
  product_id: string;
  name?: string;
  description?: string;
  price?: number;
  image_url?: string;
  available_quantity: number;
  reserved_quantity: number;
  version: number;
}

export interface ServiceHealth {
  service: string;
  port: number;
  status: 'UP' | 'DOWN' | 'CHECKING' | 'DEGRADED';
  latency_ms?: number;
  components?: Record<string, boolean>;
}


export interface CreateOrderPayload {
  customer_id: string;
  items: {
    product_id: string;
    quantity: number;
  }[];
}
