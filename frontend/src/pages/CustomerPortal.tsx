import React, { useState, useEffect } from 'react';
import { 
  CheckCircle2, ArrowRight, ShieldCheck, 
  Plus, Minus, Trash2, RefreshCw, Terminal, 
  AlertCircle, ShoppingCart
} from 'lucide-react';
import { CATALOG_PRODUCTS, createOrder, listOrders, listInventory, generateUUID } from '../services/api';
import { Order } from '../types';

interface CustomerPortalProps {
  onOpenTrace: (orderId: string) => void;
  cart: { productId: string; quantity: number }[];
  onAddToCart: (productId: string) => void;
  onUpdateCartQty: (productId: string, delta: number) => void;
  onRemoveFromCart: (productId: string) => void;
  onClearCart: () => void;
}

export const CustomerPortal: React.FC<CustomerPortalProps> = ({
  onOpenTrace,
  cart,
  onAddToCart,
  onUpdateCartQty,
  onRemoveFromCart,
  onClearCart
}) => {
  const [orders, setOrders] = useState<Order[]>([]);
  const [inventory, setInventory] = useState<Record<string, number>>({});
  const [selectedCustomerId, setSelectedCustomerId] = useState('c1010000-0000-0000-0000-000000000101');
  const [submitting, setSubmitting] = useState(false);
  const [activeOrder, setActiveOrder] = useState<Order | null>(null);
  const [lastLatencyMs, setLastLatencyMs] = useState<number | null>(null);
  const [orderError, setOrderError] = useState<string | null>(null);

  const refreshData = async () => {
    try {
      const [fetchedOrders, fetchedInv] = await Promise.all([
        listOrders(10),
        listInventory()
      ]);
      setOrders(fetchedOrders);
      
      const invMap: Record<string, number> = {};
      fetchedInv.forEach(item => {
        invMap[item.product_id] = item.available_quantity;
      });
      setInventory(invMap);
    } catch (err) {
      console.warn("Telemetry refresh warning:", err);
    }
  };

  useEffect(() => {
    refreshData();
    const interval = setInterval(refreshData, 3000);
    return () => clearInterval(interval);
  }, []);

  const totalCartAmount = cart.reduce((sum, item) => {
    const product = CATALOG_PRODUCTS.find(p => p.product_id === item.productId);
    return sum + (product ? product.price * item.quantity : 0);
  }, 0);

  const handleCheckout = async () => {
    if (cart.length === 0) return;
    setSubmitting(true);
    setOrderError(null);
    const idempotencyKey = generateUUID();

    try {
      const payload = {
        customer_id: selectedCustomerId,
        items: cart.map(item => ({
          product_id: item.productId,
          quantity: item.quantity
        }))
      };

      const { order, latencyMs } = await createOrder(payload, idempotencyKey);
      setActiveOrder(order);
      setLastLatencyMs(latencyMs);
      onClearCart();
      refreshData();
    } catch (err: any) {
      setOrderError(err.message || "Failed to commit order transaction.");
    } finally {
      setSubmitting(false);
    }
  };

  return (
    <div className="space-y-8">
      {/* Enterprise Header Ribbon */}
      <div className="flex flex-col md:flex-row md:items-center justify-between gap-4 pb-6 border-b border-zinc-800">
        <div>
          <div className="flex items-center space-x-2.5">
            <h1 className="text-xl font-semibold tracking-tight text-zinc-100">
              Enterprise Infrastructure Storefront
            </h1>
            <span className="px-2 py-0.5 rounded text-[10px] font-mono uppercase tracking-wider bg-emerald-500/10 text-emerald-400 border border-emerald-500/20">
              ACID • Kafka Saga
            </span>
          </div>
          <p className="text-xs text-zinc-400 mt-1">
            Production hardware procurement with optimistic locking, transactional outbox publishing, and distributed compensation.
          </p>
        </div>

        <div className="flex items-center space-x-4 font-mono text-xs text-zinc-400">
          <div className="flex items-center space-x-1.5">
            <span className="w-2 h-2 rounded-full bg-emerald-400"></span>
            <span>Zero-Oversell Engine</span>
          </div>
          <span className="text-zinc-700">|</span>
          <div>P99 SLA: &lt;50ms</div>
        </div>
      </div>

      {/* Order Commit Alert / Telemetry Launcher */}
      {activeOrder && (
        <div className="rounded-lg bg-zinc-900/90 border border-emerald-500/30 p-5 shadow-lg">
          <div className="flex flex-col md:flex-row md:items-center justify-between gap-4">
            <div className="flex items-start space-x-3.5">
              <div className="p-2 rounded-md bg-emerald-500/10 text-emerald-400 border border-emerald-500/20">
                <CheckCircle2 className="w-5 h-5" />
              </div>
              <div>
                <div className="flex items-center space-x-2">
                  <span className="font-semibold text-zinc-100 text-sm">Order Committed Successfully</span>
                  <span className="px-2 py-0.5 rounded text-[10px] font-mono font-semibold uppercase bg-emerald-500/10 text-emerald-400 border border-emerald-500/20">
                    {activeOrder.status}
                  </span>
                </div>
                <div className="flex flex-wrap items-center gap-3 text-xs text-zinc-400 font-mono mt-1">
                  <span>Order ID: <span className="text-zinc-200">{activeOrder.id}</span></span>
                  <span>&bull;</span>
                  <span>Amount: <span className="text-zinc-200">${parseFloat(activeOrder.total_amount).toLocaleString()} USD</span></span>
                  {lastLatencyMs !== null && (
                    <>
                      <span>&bull;</span>
                      <span className="text-emerald-400">Response Latency: {lastLatencyMs}ms</span>
                    </>
                  )}
                </div>
              </div>
            </div>

            <button
              onClick={() => onOpenTrace(activeOrder.id)}
              className="flex items-center space-x-2 px-4 py-2 rounded-md text-xs font-semibold bg-zinc-100 hover:bg-white text-zinc-950 transition-colors shadow-sm self-start md:self-center"
            >
              <Terminal className="w-3.5 h-3.5 text-zinc-800" />
              <span>Inspect Saga & Kafka Telemetry</span>
              <ArrowRight className="w-3.5 h-3.5" />
            </button>
          </div>
        </div>
      )}

      {/* Order Submission Error Banner */}
      {orderError && (
        <div className="rounded-lg bg-rose-950/30 border border-rose-500/40 p-4 text-xs flex items-start space-x-3">
          <AlertCircle className="w-5 h-5 text-rose-400 shrink-0 mt-0.5" />
          <div>
            <span className="font-semibold text-rose-200">Transaction Rejection:</span>
            <p className="text-rose-300 font-mono mt-0.5">{orderError}</p>
          </div>
        </div>
      )}

      {/* Product Catalog Grid */}
      <section>
        <div className="flex items-center justify-between mb-4">
          <div className="flex items-center space-x-2">
            <h2 className="text-sm font-semibold uppercase tracking-wider text-zinc-400">
              Hardware Fleet Catalog
            </h2>
            <span className="text-xs font-mono text-zinc-500">({CATALOG_PRODUCTS.length} SKUs Active)</span>
          </div>
          <span className="text-xs text-zinc-400 font-mono">Live Sync: PostgreSQL Inventory</span>
        </div>

        <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-4">
          {CATALOG_PRODUCTS.map((prod) => {
            const stock = inventory[prod.product_id] ?? 100;
            const inCart = cart.find(c => c.productId === prod.product_id)?.quantity || 0;

            return (
              <div
                key={prod.product_id}
                className="rounded-lg bg-[#11131b] border border-zinc-800/90 p-4 flex flex-col justify-between hover:border-zinc-700 transition-colors"
              >
                <div>
                  {/* Category & Stock Pill */}
                  <div className="flex items-center justify-between mb-3 text-[11px]">
                    <span className="font-mono text-zinc-400 text-[10px] uppercase tracking-wide">
                      {prod.category}
                    </span>
                    <span className={`font-mono text-[10px] px-2 py-0.5 rounded border ${
                      stock > 10 
                        ? 'bg-emerald-500/10 text-emerald-400 border-emerald-500/20'
                        : stock > 0
                        ? 'bg-amber-500/10 text-amber-400 border-amber-500/20'
                        : 'bg-rose-500/10 text-rose-400 border-rose-500/20'
                    }`}>
                      {stock > 0 ? `${stock} AVAILABLE` : 'DEPLETED'}
                    </span>
                  </div>

                  {/* Title & SKU */}
                  <h3 className="font-semibold text-zinc-100 text-sm leading-snug">
                    {prod.name}
                  </h3>
                  <p className="text-[11px] font-mono text-zinc-500 mt-1">
                    SKU: {prod.sku}
                  </p>

                  <p className="text-xs text-zinc-400 mt-2.5 leading-relaxed line-clamp-2">
                    {prod.description}
                  </p>

                  <div className="mt-3 py-2 px-2.5 rounded bg-zinc-900/80 border border-zinc-800/80 font-mono text-[10px] text-zinc-400">
                    {prod.specs}
                  </div>
                </div>

                {/* Pricing & Add Action */}
                <div className="pt-4 border-t border-zinc-800/80 mt-4 flex items-center justify-between">
                  <div>
                    <span className="text-[10px] uppercase font-mono text-zinc-500 block">Unit Price</span>
                    <span className="text-base font-bold text-zinc-100 font-mono">
                      ${prod.price.toLocaleString(undefined, { minimumFractionDigits: 2 })}
                    </span>
                  </div>

                  <button
                    onClick={() => onAddToCart(prod.product_id)}
                    disabled={stock <= 0}
                    className={`px-3 py-1.5 rounded-md text-xs font-semibold flex items-center space-x-1.5 transition-colors ${
                      stock > 0
                        ? 'bg-zinc-100 hover:bg-white text-zinc-900'
                        : 'bg-zinc-800 text-zinc-500 cursor-not-allowed'
                    }`}
                  >
                    <Plus className="w-3.5 h-3.5" />
                    <span>{inCart > 0 ? `In Cart (${inCart})` : 'Add'}</span>
                  </button>
                </div>
              </div>
            );
          })}
        </div>
      </section>

      {/* Shopping Cart & Idempotency Checkout Console */}
      {cart.length > 0 && (
        <section className="rounded-lg bg-[#11131b] border border-zinc-800 p-5 shadow-xl">
          <div className="flex items-center justify-between pb-3.5 border-b border-zinc-800">
            <div className="flex items-center space-x-2.5">
              <ShoppingCart className="w-4 h-4 text-emerald-400" />
              <h2 className="text-sm font-semibold text-zinc-100">
                Pending Procurement Order ({cart.reduce((s, i) => s + i.quantity, 0)} items)
              </h2>
            </div>
            <button
              onClick={onClearCart}
              className="text-xs text-zinc-500 hover:text-rose-400 font-mono transition-colors"
            >
              [Clear All]
            </button>
          </div>

          <div className="divide-y divide-zinc-800/60 my-3 font-mono text-xs">
            {cart.map(item => {
              const prod = CATALOG_PRODUCTS.find(p => p.product_id === item.productId);
              if (!prod) return null;

              return (
                <div key={item.productId} className="py-2.5 flex items-center justify-between">
                  <div className="flex items-center space-x-3">
                    <span className="text-zinc-500"># {prod.sku}</span>
                    <span className="text-zinc-200 font-sans font-medium">{prod.name}</span>
                  </div>

                  <div className="flex items-center space-x-5">
                    <span className="text-zinc-400">
                      ${prod.price.toLocaleString(undefined, { minimumFractionDigits: 2 })} &times; {item.quantity}
                    </span>

                    <div className="flex items-center space-x-1.5 bg-zinc-900 px-2 py-0.5 rounded border border-zinc-800">
                      <button
                        onClick={() => onUpdateCartQty(item.productId, -1)}
                        className="p-1 text-zinc-400 hover:text-white"
                      >
                        <Minus className="w-3 h-3" />
                      </button>
                      <span className="font-bold text-zinc-200 px-1">{item.quantity}</span>
                      <button
                        onClick={() => onUpdateCartQty(item.productId, 1)}
                        className="p-1 text-zinc-400 hover:text-white"
                      >
                        <Plus className="w-3 h-3" />
                      </button>
                    </div>

                    <span className="font-bold text-zinc-100 w-24 text-right">
                      ${(prod.price * item.quantity).toLocaleString(undefined, { minimumFractionDigits: 2 })}
                    </span>

                    <button
                      onClick={() => onRemoveFromCart(item.productId)}
                      className="text-zinc-500 hover:text-rose-400 p-1 transition-colors"
                    >
                      <Trash2 className="w-3.5 h-3.5" />
                    </button>
                  </div>
                </div>
              );
            })}
          </div>

          {/* Checkout Controls */}
          <div className="pt-4 border-t border-zinc-800 flex flex-col md:flex-row md:items-center justify-between gap-4">
            <div className="flex flex-col sm:flex-row sm:items-center gap-3 text-xs">
              <div className="flex items-center space-x-2">
                <span className="text-zinc-400">Customer Tenant:</span>
                <select
                  value={selectedCustomerId}
                  onChange={(e) => setSelectedCustomerId(e.target.value)}
                  className="bg-zinc-900 text-zinc-200 rounded px-2.5 py-1 border border-zinc-700 font-mono text-xs focus:outline-none focus:border-zinc-500"
                >
                  <option value="c1010000-0000-0000-0000-000000000101">Alpha Corp (0000-0101)</option>
                  <option value="c1010000-0000-0000-0000-000000000102">Beta Holdings (0000-0102)</option>
                  <option value="c1010000-0000-0000-0000-000000000103">Gamma Research (0000-0103)</option>
                </select>
              </div>

              <span className="text-zinc-600 hidden sm:inline">&bull;</span>
              <span className="font-mono text-[11px] text-zinc-500">
                Idempotency Header: Auto-generated UUID v4
              </span>
            </div>

            <div className="flex items-center space-x-6">
              <div className="text-right">
                <span className="text-[10px] font-mono uppercase text-zinc-500 block">Total Commitment</span>
                <span className="text-xl font-bold font-mono text-zinc-100">
                  ${totalCartAmount.toLocaleString(undefined, { minimumFractionDigits: 2 })} USD
                </span>
              </div>

              <button
                onClick={handleCheckout}
                disabled={submitting}
                className="px-5 py-2.5 rounded-md font-medium text-xs bg-emerald-500 hover:bg-emerald-400 text-zinc-950 flex items-center space-x-2 transition-colors disabled:opacity-50"
              >
                {submitting ? (
                  <>
                    <RefreshCw className="w-3.5 h-3.5 animate-spin" />
                    <span>Executing Saga Transaction...</span>
                  </>
                ) : (
                  <>
                    <ShieldCheck className="w-4 h-4" />
                    <span>Authorize & Commit Order</span>
                  </>
                )}
              </button>
            </div>
          </div>
        </section>
      )}

      {/* Enterprise Orders Audit Ledger */}
      <section className="rounded-lg bg-[#11131b] border border-zinc-800 p-5">
        <div className="flex items-center justify-between mb-4">
          <div>
            <h2 className="text-sm font-semibold uppercase tracking-wider text-zinc-400">
              Recent Transactions Ledger
            </h2>
            <p className="text-xs text-zinc-500">Direct PostgreSQL read with Redis dynamic cache invalidation</p>
          </div>
          <button
            onClick={refreshData}
            className="flex items-center space-x-1.5 text-xs text-zinc-400 hover:text-zinc-200 font-mono transition-colors"
          >
            <RefreshCw className="w-3.5 h-3.5" />
            <span>Sync</span>
          </button>
        </div>

        {orders.length === 0 ? (
          <div className="py-10 text-center text-zinc-500 font-mono text-xs">
            No transactions found. Authorize an order above to initiate the distributed Kafka saga.
          </div>
        ) : (
          <div className="overflow-x-auto">
            <table className="w-full text-left text-xs font-mono">
              <thead className="text-[10px] text-zinc-500 uppercase tracking-wider border-b border-zinc-800 pb-2">
                <tr>
                  <th className="pb-2.5">Order ID</th>
                  <th className="pb-2.5">Items</th>
                  <th className="pb-2.5">Total USD</th>
                  <th className="pb-2.5">Saga Status</th>
                  <th className="pb-2.5">Payment</th>
                  <th className="pb-2.5">Inventory</th>
                  <th className="pb-2.5 text-right">Distributed Trace</th>
                </tr>
              </thead>
              <tbody className="divide-y divide-zinc-800/50">
                {orders.map((ord) => (
                  <tr key={ord.id} className="hover:bg-zinc-900/40 transition-colors">
                    <td className="py-3 text-zinc-300 font-medium">{(ord.id || '').slice(0, 8)}...</td>
                    <td className="py-3 text-zinc-400 font-sans">{(ord.items || []).length} sku(s)</td>
                    <td className="py-3 text-zinc-100 font-bold">
                      ${parseFloat(ord.total_amount || '0').toLocaleString(undefined, { minimumFractionDigits: 2 })}
                    </td>
                    <td className="py-3">
                      <span className={`px-2 py-0.5 rounded text-[10px] font-semibold ${
                        ord.status === 'CONFIRMED'
                          ? 'bg-emerald-500/10 text-emerald-400 border border-emerald-500/20'
                          : ord.status === 'FAILED'
                          ? 'bg-rose-500/10 text-rose-400 border border-rose-500/20'
                          : 'bg-amber-500/10 text-amber-400 border border-amber-500/20'
                      }`}>
                        {ord.status}
                      </span>
                    </td>
                    <td className="py-3">
                      <span className={`text-[11px] ${
                        ord.payment_status === 'SUCCEEDED' ? 'text-emerald-400' : 'text-amber-400'
                      }`}>
                        {ord.payment_status}
                      </span>
                    </td>
                    <td className="py-3">
                      <span className={`text-[11px] ${
                        ord.inventory_status === 'RESERVED' ? 'text-emerald-400' : 'text-zinc-400'
                      }`}>
                        {ord.inventory_status}
                      </span>
                    </td>
                    <td className="py-3 text-right">
                      <button
                        onClick={() => onOpenTrace(ord.id)}
                        className="inline-flex items-center space-x-1.5 px-2.5 py-1 rounded bg-zinc-900 hover:bg-zinc-800 text-zinc-200 border border-zinc-700/70 text-[11px] transition-colors"
                      >
                        <Terminal className="w-3 h-3 text-emerald-400" />
                        <span>Inspect Trace</span>
                      </button>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}
      </section>
    </div>
  );
};

