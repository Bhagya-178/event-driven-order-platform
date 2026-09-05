import React, { useState, useEffect } from 'react';
import { 
  X, Database, Server, RefreshCw, CheckCircle2, 
  AlertCircle, Copy, Check, Clock, Layers,
  Terminal, ShieldCheck, ChevronRight
} from 'lucide-react';
import { OrderTrace } from '../types';
import { getOrderTrace, createOrder } from '../services/api';

interface SagaTraceDrawerProps {
  orderId: string | null;
  onClose: () => void;
}

export const SagaTraceDrawer: React.FC<SagaTraceDrawerProps> = ({ orderId, onClose }) => {
  const [trace, setTrace] = useState<OrderTrace | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [copiedKey, setCopiedKey] = useState(false);
  const [retryingIdempotency, setRetryingIdempotency] = useState(false);
  const [idempotencyResult, setIdempotencyResult] = useState<string | null>(null);
  const [inspectingPayloadId, setInspectingPayloadId] = useState<string | null>(null);

  useEffect(() => {
    if (!orderId) return;

    let isMounted = true;
    const fetchTrace = async () => {
      setLoading(true);
      setError(null);
      try {
        const data = await getOrderTrace(orderId);
        if (isMounted) setTrace(data);
      } catch (err: any) {
        if (isMounted) setError(err.message || 'Failed to load trace');
      } finally {
        if (isMounted) setLoading(false);
      }
    };

    fetchTrace();
    const timer = setInterval(fetchTrace, 2000);

    return () => {
      isMounted = false;
      clearInterval(timer);
    };
  }, [orderId]);

  if (!orderId) return null;

  const handleCopy = (text: string) => {
    navigator.clipboard.writeText(text);
    setCopiedKey(true);
    setTimeout(() => setCopiedKey(false), 2000);
  };

  const handleSimulateDuplicate = async () => {
    if (!trace?.order) return;
    setRetryingIdempotency(true);
    setIdempotencyResult(null);

    try {
      const payload = {
        customer_id: trace.order.customer_id,
        items: trace.order.items.map(item => ({
          product_id: item.product_id,
          quantity: item.quantity
        }))
      };

      const { order, latencyMs } = await createOrder(
        payload,
        trace.order.idempotency_key || 'test-key'
      );

      if (order.id === trace.order.id) {
        setIdempotencyResult(`Idempotency Verified (${latencyMs}ms): Cache/DB returned existing Order aggregate with zero side effects.`);
      } else {
        setIdempotencyResult(`Warning: A different order ID was returned: ${order.id}`);
      }
    } catch (err: any) {
      setIdempotencyResult(`Idempotency Replay: ${err.message}`);
    } finally {
      setRetryingIdempotency(false);
    }
  };

  return (
    <div className="fixed inset-0 z-50 flex justify-end bg-black/70 backdrop-blur-sm transition-opacity">
      <div className="w-full max-w-2xl bg-[#0e1017] border-l border-zinc-800 h-full flex flex-col shadow-2xl overflow-hidden font-sans">
        {/* Telemetry Header */}
        <div className="px-6 py-4 border-b border-zinc-800 flex items-center justify-between bg-zinc-900/90">
          <div className="flex items-center space-x-3">
            <div className="w-8 h-8 rounded bg-zinc-800 border border-zinc-700 flex items-center justify-center">
              <Terminal className="w-4 h-4 text-emerald-400" />
            </div>
            <div>
              <div className="flex items-center space-x-2">
                <h2 className="text-sm font-semibold text-zinc-100 font-mono">
                  SAGA DISTRIBUTED TRACE
                </h2>
                <span className="px-1.5 py-0.2 rounded text-[10px] font-mono uppercase bg-zinc-800 text-zinc-400 border border-zinc-700">
                  APM • OpenTelemetry
                </span>
              </div>
              <p className="text-[11px] text-zinc-400 font-mono mt-0.5">
                Aggregate UUID: <span className="text-zinc-200">{orderId}</span>
              </p>
            </div>
          </div>
          <button
            onClick={onClose}
            className="p-1.5 rounded text-zinc-400 hover:text-zinc-100 hover:bg-zinc-800 transition-colors"
          >
            <X className="w-4 h-4" />
          </button>
        </div>

        {/* Telemetry Body */}
        <div className="flex-1 overflow-y-auto p-6 space-y-6 text-xs">
          {loading && !trace && (
            <div className="flex items-center justify-center py-16 text-zinc-400 space-x-2 font-mono">
              <RefreshCw className="w-4 h-4 animate-spin text-emerald-400" />
              <span>Querying distributed trace from order-service:8000...</span>
            </div>
          )}

          {error && (
            <div className="p-4 rounded bg-rose-950/30 border border-rose-500/40 text-rose-300 font-mono flex items-start space-x-2.5">
              <AlertCircle className="w-4 h-4 text-rose-400 shrink-0 mt-0.5" />
              <div>
                <p className="font-semibold">Failed to load aggregate telemetry</p>
                <p className="mt-1 text-[11px] opacity-90">{error}</p>
              </div>
            </div>
          )}

          {trace && (
            <>
              {/* Choreographed Microservices Topology */}
              <div className="p-4 rounded-lg bg-zinc-900/60 border border-zinc-800/90">
                <div className="flex items-center justify-between mb-3">
                  <h3 className="text-xs font-semibold text-zinc-300 uppercase tracking-wider flex items-center space-x-2">
                    <Layers className="w-3.5 h-3.5 text-emerald-400" />
                    <span>Microservice Cluster Topology</span>
                  </h3>
                  <span className="text-[10px] font-mono text-zinc-500">Event-Choreographed</span>
                </div>

                <div className="grid grid-cols-3 gap-2.5 text-center font-mono">
                  {/* Order Service */}
                  <div className="p-3 rounded bg-zinc-950/80 border border-zinc-800">
                    <span className="text-[10px] text-zinc-500 block">PORT 8000</span>
                    <span className="text-xs font-semibold text-zinc-200 mt-1 block">Order Service</span>
                    <span className="mt-2 inline-flex items-center space-x-1 px-1.5 py-0.5 rounded text-[10px] font-semibold bg-emerald-500/10 text-emerald-400 border border-emerald-500/20">
                      <CheckCircle2 className="w-2.5 h-2.5" />
                      <span>{trace.order.status}</span>
                    </span>
                  </div>

                  {/* Payment Service */}
                  <div className="p-3 rounded bg-zinc-950/80 border border-zinc-800">
                    <span className="text-[10px] text-zinc-500 block">PORT 8001</span>
                    <span className="text-xs font-semibold text-zinc-200 mt-1 block">Payment Service</span>
                    <span className={`mt-2 inline-flex items-center space-x-1 px-1.5 py-0.5 rounded text-[10px] font-semibold border ${
                      trace.order.payment_status === 'SUCCEEDED'
                        ? 'bg-emerald-500/10 text-emerald-400 border-emerald-500/20'
                        : 'bg-amber-500/10 text-amber-400 border-amber-500/20'
                    }`}>
                      <CheckCircle2 className="w-2.5 h-2.5" />
                      <span>{trace.order.payment_status}</span>
                    </span>
                  </div>

                  {/* Inventory Service */}
                  <div className="p-3 rounded bg-zinc-950/80 border border-zinc-800">
                    <span className="text-[10px] text-zinc-500 block">PORT 8002</span>
                    <span className="text-xs font-semibold text-zinc-200 mt-1 block">Inventory Service</span>
                    <span className={`mt-2 inline-flex items-center space-x-1 px-1.5 py-0.5 rounded text-[10px] font-semibold border ${
                      trace.order.inventory_status === 'RESERVED'
                        ? 'bg-emerald-500/10 text-emerald-400 border-emerald-500/20'
                        : 'bg-zinc-800 text-zinc-400 border-zinc-700'
                    }`}>
                      <CheckCircle2 className="w-2.5 h-2.5" />
                      <span>{trace.order.inventory_status}</span>
                    </span>
                  </div>
                </div>
              </div>

              {/* Distributed Execution Waterfall */}
              <div className="p-4 rounded-lg bg-zinc-900/60 border border-zinc-800/90">
                <div className="flex items-center justify-between mb-3">
                  <h3 className="text-xs font-semibold text-zinc-300 uppercase tracking-wider flex items-center space-x-2">
                    <Clock className="w-3.5 h-3.5 text-zinc-400" />
                    <span>Distributed Saga Waterfall Spans</span>
                  </h3>
                  <span className="text-[10px] font-mono text-zinc-500">{trace.saga_timeline.length} Spans</span>
                </div>

                <div className="space-y-2.5">
                  {trace.saga_timeline.map((m, idx) => (
                    <div key={idx} className="p-2.5 rounded bg-zinc-950/60 border border-zinc-800/80 flex items-start space-x-3">
                      <div className={`mt-0.5 w-4 h-4 rounded-full flex items-center justify-center shrink-0 ${
                        m.status === 'COMPLETED'
                          ? 'bg-emerald-500/20 text-emerald-400 border border-emerald-500/40'
                          : m.status === 'FAILED'
                          ? 'bg-rose-500/20 text-rose-400 border border-rose-500/40'
                          : 'bg-amber-500/20 text-amber-400 border border-amber-500/40'
                      }`}>
                        {m.status === 'COMPLETED' ? <Check className="w-2.5 h-2.5" /> : <Clock className="w-2.5 h-2.5" />}
                      </div>

                      <div className="flex-1">
                        <div className="flex items-center justify-between font-mono">
                          <span className="font-semibold text-zinc-200">{m.step}</span>
                          <span className="text-[10px] text-zinc-500">{m.service}</span>
                        </div>
                        <p className="text-zinc-400 text-[11px] mt-0.5 font-sans">{m.detail}</p>
                      </div>
                    </div>
                  ))}
                </div>
              </div>

              {/* Transactional Outbox Records */}
              <div className="p-4 rounded-lg bg-zinc-900/60 border border-zinc-800/90">
                <div className="flex items-center justify-between mb-3">
                  <div>
                    <h3 className="text-xs font-semibold text-zinc-300 uppercase tracking-wider flex items-center space-x-2">
                      <Database className="w-3.5 h-3.5 text-emerald-400" />
                      <span>Transactional Outbox Events ({trace.outbox_events.length})</span>
                    </h3>
                    <span className="text-[10px] font-mono text-zinc-500">Atomic PostgreSQL Commit &bull; Relayed to Kafka</span>
                  </div>
                </div>

                {trace.outbox_events.length === 0 ? (
                  <p className="text-zinc-500 font-mono text-xs italic">No outbox events recorded for this aggregate.</p>
                ) : (
                  <div className="space-y-2">
                    {trace.outbox_events.map((evt) => (
                      <div key={evt.id} className="p-2.5 rounded bg-zinc-950/80 border border-zinc-800 font-mono text-[11px]">
                        <div className="flex items-center justify-between mb-1">
                          <span className="text-emerald-400 font-semibold">{evt.event_type}</span>
                          <span className="px-1.5 py-0.2 rounded text-[9px] uppercase bg-emerald-500/10 text-emerald-400 border border-emerald-500/30">
                            {evt.status}
                          </span>
                        </div>
                        <div className="text-[10px] text-zinc-500 space-y-0.5">
                          <p>Outbox UUID: {evt.id}</p>
                          <p>Correlation ID: {evt.correlation_id || 'N/A'}</p>
                        </div>
                      </div>
                    ))}
                  </div>
                )}
              </div>

              {/* Idempotency Protection & Duplicate Replay Test */}
              <div className="p-4 rounded-lg bg-zinc-900/60 border border-zinc-800/90">
                <div className="flex items-center justify-between mb-2">
                  <h3 className="text-xs font-semibold text-zinc-300 uppercase tracking-wider flex items-center space-x-2">
                    <ShieldCheck className="w-3.5 h-3.5 text-emerald-400" />
                    <span>Client Idempotency Proof</span>
                  </h3>
                  <button
                    onClick={() => handleCopy(trace.order.idempotency_key || '')}
                    className="flex items-center space-x-1 text-[11px] text-zinc-400 hover:text-zinc-200 font-mono"
                  >
                    {copiedKey ? <Check className="w-3 h-3 text-emerald-400" /> : <Copy className="w-3 h-3" />}
                    <span>{copiedKey ? 'Copied' : 'Copy'}</span>
                  </button>
                </div>

                <div className="p-2 rounded bg-zinc-950 border border-zinc-800 font-mono text-[11px] text-zinc-400 break-all mb-3 select-all">
                  {trace.order.idempotency_key || 'No idempotency key registered'}
                </div>

                <button
                  onClick={handleSimulateDuplicate}
                  disabled={retryingIdempotency}
                  className="w-full py-2 px-3 rounded text-xs font-medium bg-zinc-800 hover:bg-zinc-700 text-zinc-200 border border-zinc-700/80 transition-colors flex items-center justify-center space-x-2 font-mono"
                >
                  <RefreshCw className={`w-3 h-3 ${retryingIdempotency ? 'animate-spin' : ''}`} />
                  <span>Simulate Duplicate Request (Re-play Same Key)</span>
                </button>

                {idempotencyResult && (
                  <div className="mt-3 p-2.5 rounded bg-emerald-950/30 border border-emerald-500/30 text-emerald-300 font-mono text-[11px]">
                    {idempotencyResult}
                  </div>
                )}
              </div>
            </>
          )}
        </div>
      </div>
    </div>
  );
};

