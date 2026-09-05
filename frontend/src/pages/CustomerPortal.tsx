import React, { useState, useEffect } from 'react';
import { 
  ShoppingBag, CheckCircle2, ArrowRight, 
  Zap, Shield, Plus, Minus, Trash2, RefreshCw
} from 'lucide-react';
import { CATALOG_PRODUCTS, createOrder, listOrders, listInventory } from '../services/api';
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

  // Fetch initial inventory and recent orders
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
      console.warn("Failed to refresh customer data:", err);
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
    const idempotencyKey = crypto.randomUUID();

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
      alert(`Checkout failed: ${err.message}`);
    } finally {
      setSubmitting(false);
    }
  };

  return (
    <div className="space-y-12">
      {/* Hero Banner */}
      <section className="relative overflow-hidden rounded-3xl bg-gradient-to-br from-slate-900 via-indigo-950/40 to-slate-900 border border-slate-800 p-8 sm:p-12 shadow-2xl">
        <div className="relative z-10 max-w-2xl">
          <span className="px-3 py-1 text-xs font-semibold rounded-full bg-indigo-500/10 text-indigo-400 border border-indigo-500/30 uppercase tracking-wider">
            Next-Gen Infrastructure Hardware
          </span>
          <h1 className="text-3xl sm:text-4xl font-extrabold text-white tracking-tight mt-4">
            Enterprise Cloud Hardware Storefront
          </h1>
          <p className="text-slate-300 mt-3 text-sm sm:text-base leading-relaxed">
            Order enterprise nodes, quantum tensor accelerators, and persistent event clusters backed by ACID transactions and distributed Kafka sagas.
          </p>
        </div>

        {/* Decorative Background Grid */}
        <div className="absolute right-0 top-0 w-96 h-96 bg-indigo-600/10 rounded-full blur-3xl pointer-events-none" />
      </section>

      {/* Active Order Banner (If just placed) */}
      {activeOrder && (
        <section className="p-6 rounded-2xl bg-emerald-950/20 border border-emerald-500/30 shadow-xl transition-all">
          <div className="flex flex-col sm:flex-row sm:items-center justify-between gap-4">
            <div className="flex items-start space-x-3">
              <div className="p-2 rounded-xl bg-emerald-500/10 text-emerald-400 border border-emerald-500/20">
                <CheckCircle2 className="w-6 h-6" />
              </div>
              <div>
                <div className="flex items-center space-x-2">
                  <span className="font-bold text-white text-base">Order Placed Successfully!</span>
                  <span className="px-2 py-0.5 rounded text-[10px] font-bold uppercase bg-emerald-500/10 text-emerald-400 border border-emerald-500/30">
                    {activeOrder.status}
                  </span>
                </div>
                <p className="text-xs text-slate-400 font-mono mt-1">
                  Order ID: {activeOrder.id} &bull; Total: ${activeOrder.total_amount} {activeOrder.currency}
                </p>
                {lastLatencyMs !== null && (
                  <p className="text-[11px] text-emerald-400 mt-1">
                    Atomic database commit in {lastLatencyMs}ms (Redis cache & Kafka outbox staged)
                  </p>
                )}
              </div>
            </div>

            {/* The Magic Button requested by user */}
            <button
              onClick={() => onOpenTrace(activeOrder.id)}
              className="flex items-center space-x-2 px-4 py-2.5 rounded-xl text-xs font-bold bg-indigo-600 hover:bg-indigo-500 text-white shadow-lg shadow-indigo-600/30 transition-all self-start sm:self-center"
            >
              <Zap className="w-4 h-4 text-cyan-300" />
              <span>Inspect Distributed Saga & Kafka Trace</span>
              <ArrowRight className="w-4 h-4" />
            </button>
          </div>
        </section>
      )}

      {/* Product Catalog Grid */}
      <section>
        <div className="flex items-center justify-between mb-6">
          <div>
            <h2 className="text-xl font-bold text-white">Available Products</h2>
            <p className="text-xs text-slate-400">Live stock synchronized with distributed inventory database</p>
          </div>
          <span className="text-xs text-slate-400 font-mono">{CATALOG_PRODUCTS.length} Models Ready</span>
        </div>

        <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-6">
          {CATALOG_PRODUCTS.map((prod) => {
            const stock = inventory[prod.product_id] ?? 25;
            const inCart = cart.find(c => c.productId === prod.product_id)?.quantity || 0;

            return (
              <div
                key={prod.product_id}
                className="rounded-2xl bg-[#0e1424] border border-slate-800/80 p-5 flex flex-col justify-between hover:border-slate-700 transition-all hover:shadow-xl hover:shadow-indigo-500/5 group"
              >
                <div>
                  <div className="flex items-center justify-between mb-3">
                    <span className="px-2 py-0.5 text-[10px] font-semibold rounded bg-slate-800 text-indigo-400 border border-slate-700">
                      {prod.category}
                    </span>
                    <span className={`text-[11px] font-bold px-2 py-0.5 rounded-full ${
                      stock > 5 
                        ? 'bg-emerald-500/10 text-emerald-400 border border-emerald-500/20'
                        : stock > 0
                        ? 'bg-amber-500/10 text-amber-400 border border-amber-500/20'
                        : 'bg-rose-500/10 text-rose-400 border border-rose-500/20'
                    }`}>
                      {stock > 0 ? `${stock} in stock` : 'Out of Stock'}
                    </span>
                  </div>

                  <h3 className="font-bold text-white text-base group-hover:text-indigo-400 transition-colors">
                    {prod.name}
                  </h3>
                  <p className="text-xs text-slate-400 mt-2 line-clamp-2">
                    {prod.description}
                  </p>
                  <p className="text-[11px] text-slate-500 font-mono mt-2.5">
                    {prod.specs}
                  </p>
                </div>

                <div className="pt-5 border-t border-slate-800/60 mt-4 flex items-center justify-between">
                  <div>
                    <span className="text-xs text-slate-400">Price</span>
                    <p className="text-lg font-extrabold text-white">${prod.price.toFixed(2)}</p>
                  </div>

                  <button
                    onClick={() => onAddToCart(prod.product_id)}
                    disabled={stock <= 0}
                    className={`px-3.5 py-2 rounded-xl text-xs font-semibold flex items-center space-x-1.5 transition-all ${
                      stock > 0
                        ? 'bg-indigo-600 hover:bg-indigo-500 text-white shadow-md shadow-indigo-600/20'
                        : 'bg-slate-800 text-slate-500 cursor-not-allowed'
                    }`}
                  >
                    <Plus className="w-3.5 h-3.5" />
                    <span>{inCart > 0 ? `In Cart (${inCart})` : 'Add to Cart'}</span>
                  </button>
                </div>
              </div>
            );
          })}
        </div>
      </section>

      {/* Shopping Cart Drawer / Checkout Card */}
      {cart.length > 0 && (
        <section className="p-6 rounded-2xl bg-slate-900/80 border border-slate-800 shadow-2xl">
          <div className="flex items-center justify-between pb-4 border-b border-slate-800">
            <div className="flex items-center space-x-2">
              <ShoppingBag className="w-5 h-5 text-indigo-400" />
              <h2 className="text-lg font-bold text-white">Your Shopping Cart ({cart.length} items)</h2>
            </div>
            <button
              onClick={onClearCart}
              className="text-xs text-slate-400 hover:text-rose-400 transition-colors"
            >
              Clear Cart
            </button>
          </div>

          <div className="divide-y divide-slate-800/60 my-4">
            {cart.map(item => {
              const prod = CATALOG_PRODUCTS.find(p => p.product_id === item.productId);
              if (!prod) return null;

              return (
                <div key={item.productId} className="py-3 flex items-center justify-between">
                  <div>
                    <h4 className="text-sm font-semibold text-white">{prod.name}</h4>
                    <p className="text-xs text-slate-400">${prod.price.toFixed(2)} each</p>
                  </div>

                  <div className="flex items-center space-x-4">
                    <div className="flex items-center space-x-2 bg-[#090d16] px-2 py-1 rounded-lg border border-slate-800">
                      <button
                        onClick={() => onUpdateCartQty(item.productId, -1)}
                        className="p-1 text-slate-400 hover:text-white"
                      >
                        <Minus className="w-3 h-3" />
                      </button>
                      <span className="text-xs font-mono font-bold text-slate-200 px-1">{item.quantity}</span>
                      <button
                        onClick={() => onUpdateCartQty(item.productId, 1)}
                        className="p-1 text-slate-400 hover:text-white"
                      >
                        <Plus className="w-3 h-3" />
                      </button>
                    </div>

                    <span className="text-sm font-bold text-white w-16 text-right">
                      ${(prod.price * item.quantity).toFixed(2)}
                    </span>

                    <button
                      onClick={() => onRemoveFromCart(item.productId)}
                      className="text-slate-500 hover:text-rose-400 p-1"
                    >
                      <Trash2 className="w-4 h-4" />
                    </button>
                  </div>
                </div>
              );
            })}
          </div>

          {/* Checkout Footer */}
          <div className="pt-4 border-t border-slate-800 flex flex-col sm:flex-row sm:items-center justify-between gap-4">
            <div className="flex items-center space-x-2 text-xs text-slate-400">
              <span>Customer UUID:</span>
              <select
                value={selectedCustomerId}
                onChange={(e) => setSelectedCustomerId(e.target.value)}
                className="bg-[#090d16] text-slate-300 rounded-lg px-2.5 py-1.5 border border-slate-700 font-mono text-xs focus:outline-none focus:border-indigo-500"
              >
                <option value="c1010000-0000-0000-0000-000000000101">Customer Alpha (Default)</option>
                <option value="c1010000-0000-0000-0000-000000000102">Customer Beta</option>
                <option value="c1010000-0000-0000-0000-000000000103">Customer Gamma</option>
              </select>
            </div>

            <div className="flex items-center space-x-6">
              <div className="text-right">
                <span className="text-xs text-slate-400 block">Total Due</span>
                <span className="text-2xl font-black text-white">${totalCartAmount.toFixed(2)}</span>
              </div>

              <button
                onClick={handleCheckout}
                disabled={submitting}
                className="px-6 py-3 rounded-xl font-bold text-sm bg-gradient-to-r from-indigo-600 to-indigo-500 hover:from-indigo-500 hover:to-indigo-400 text-white shadow-xl shadow-indigo-600/30 flex items-center space-x-2 disabled:opacity-50"
              >
                {submitting ? (
                  <>
                    <RefreshCw className="w-4 h-4 animate-spin" />
                    <span>Processing Saga...</span>
                  </>
                ) : (
                  <>
                    <Shield className="w-4 h-4" />
                    <span>Place Order (Idempotent)</span>
                  </>
                )}
              </button>
            </div>
          </div>
        </section>
      )}

      {/* Customer Orders History */}
      <section className="p-6 rounded-2xl bg-[#0e1424] border border-slate-800">
        <div className="flex items-center justify-between mb-4">
          <div>
            <h2 className="text-lg font-bold text-white">Recent Orders</h2>
            <p className="text-xs text-slate-400">Served with sub-millisecond Redis cache invalidation</p>
          </div>
          <button
            onClick={refreshData}
            className="flex items-center space-x-1 text-xs text-indigo-400 hover:text-indigo-300"
          >
            <RefreshCw className="w-3.5 h-3.5" />
            <span>Refresh</span>
          </button>
        </div>

        {orders.length === 0 ? (
          <div className="py-8 text-center text-slate-500 text-xs">
            No orders placed yet. Add a product above to trigger your first Kafka event!
          </div>
        ) : (
          <div className="overflow-x-auto">
            <table className="w-full text-left text-xs">
              <thead className="text-[11px] text-slate-400 uppercase tracking-wider border-b border-slate-800/80">
                <tr>
                  <th className="pb-3">Order ID</th>
                  <th className="pb-3">Items</th>
                  <th className="pb-3">Total</th>
                  <th className="pb-3">Status</th>
                  <th className="pb-3">Payment</th>
                  <th className="pb-3">Inventory</th>
                  <th className="pb-3 text-right">Technical Trace</th>
                </tr>
              </thead>
              <tbody className="divide-y divide-slate-800/50">
                {orders.map((ord) => (
                  <tr key={ord.id} className="hover:bg-slate-800/30 transition-colors">
                    <td className="py-3.5 font-mono text-slate-300">{ord.id.slice(0, 8)}...</td>
                    <td className="py-3.5 text-slate-300">{ord.items.length} item(s)</td>
                    <td className="py-3.5 font-bold text-white">${ord.total_amount}</td>
                    <td className="py-3.5">
                      <span className={`px-2 py-0.5 rounded text-[10px] font-bold ${
                        ord.status === 'CONFIRMED'
                          ? 'bg-emerald-500/10 text-emerald-400 border border-emerald-500/30'
                          : ord.status === 'FAILED'
                          ? 'bg-rose-500/10 text-rose-400 border border-rose-500/30'
                          : 'bg-indigo-500/10 text-indigo-400 border border-indigo-500/30'
                      }`}>
                        {ord.status}
                      </span>
                    </td>
                    <td className="py-3.5">
                      <span className={`text-[11px] font-medium ${
                        ord.payment_status === 'SUCCEEDED' ? 'text-emerald-400' : 'text-amber-400'
                      }`}>
                        {ord.payment_status}
                      </span>
                    </td>
                    <td className="py-3.5">
                      <span className={`text-[11px] font-medium ${
                        ord.inventory_status === 'RESERVED' ? 'text-emerald-400' : 'text-cyan-400'
                      }`}>
                        {ord.inventory_status}
                      </span>
                    </td>
                    <td className="py-3.5 text-right">
                      <button
                        onClick={() => onOpenTrace(ord.id)}
                        className="inline-flex items-center space-x-1 px-2.5 py-1 rounded-lg text-xs font-semibold bg-slate-800 hover:bg-slate-700 text-indigo-300 border border-slate-700 transition-colors"
                      >
                        <Zap className="w-3 h-3 text-cyan-400" />
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
