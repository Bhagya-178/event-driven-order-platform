import React, { useState, useEffect } from 'react';
import { 
  X, Cpu, Database, Server, RefreshCw, CheckCircle2, 
  AlertCircle, Zap, Copy, Check, Clock, Layers
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
    // Poll every 1.5s if order is in progress
    const timer = setInterval(fetchTrace, 1500);

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
        setIdempotencyResult(`Idempotency Verified! Backend detected identical key and returned existing Order (${order.id}) in ${latencyMs}ms with zero duplicate charges.`);
      } else {
        setIdempotencyResult(`Warning: A different order ID was returned: ${order.id}`);
      }
    } catch (err: any) {
      setIdempotencyResult(`Error: ${err.message}`);
    } finally {
      setRetryingIdempotency(false);
    }
  };

  return (
    <div className="fixed inset-0 z-50 flex justify-end bg-black/60 backdrop-blur-sm transition-opacity">
      <div className="w-full max-w-2xl bg-[#0d1322] border-l border-slate-800 h-full flex flex-col shadow-2xl overflow-hidden">
        {/* Header */}
        <div className="p-5 border-b border-slate-800 flex items-center justify-between bg-slate-900/80">
          <div className="flex items-center space-x-3">
            <div className="p-2 rounded-xl bg-indigo-500/10 text-indigo-400 border border-indigo-500/20">
              <Zap className="w-5 h-5" />
            </div>
            <div>
              <h2 className="text-base font-bold text-white flex items-center space-x-2">
                <span>Distributed Saga & Kafka Trace</span>
                <span className="px-2 py-0.5 text-[10px] rounded font-mono uppercase bg-slate-800 text-indigo-300 border border-slate-700">
                  Internal Telemetry
                </span>
              </h2>
              <p className="text-xs text-slate-400 font-mono mt-0.5">Order ID: {orderId}</p>
            </div>
          </div>
          <button
            onClick={onClose}
            className="p-1.5 rounded-lg text-slate-400 hover:text-white hover:bg-slate-800 transition-colors"
          >
            <X className="w-5 h-5" />
          </button>
        </div>

        {/* Scrollable Content */}
        <div className="flex-1 overflow-y-auto p-6 space-y-6">
          {loading && !trace && (
            <div className="flex items-center justify-center py-12 text-slate-400 space-x-2">
              <RefreshCw className="w-5 h-5 animate-spin text-indigo-400" />
              <span className="text-sm">Retrieving distributed trace from Order Service...</span>
            </div>
          )}

          {error && (
            <div className="p-4 rounded-xl bg-rose-500/10 border border-rose-500/30 text-rose-300 text-xs flex items-start space-x-3">
              <AlertCircle className="w-5 h-5 flex-shrink-0 mt-0.5" />
              <div>
                <p className="font-semibold">Failed to fetch technical trace</p>
                <p className="mt-1 opacity-80">{error}</p>
              </div>
            </div>
          )}

          {trace && (
            <>
              {/* Architecture Node Diagram */}
              <div className="p-4 rounded-xl bg-slate-900/60 border border-slate-800">
                <h3 className="text-xs font-bold text-slate-300 uppercase tracking-wider mb-3 flex items-center space-x-2">
                  <Layers className="w-4 h-4 text-indigo-400" />
                  <span>Choreographed Microservices Flow</span>
                </h3>

                <div className="grid grid-cols-3 gap-3 text-center">
                  {/* Step 1: Order */}
                  <div className="p-3 rounded-lg bg-[#111827] border border-slate-800">
                    <div className="w-8 h-8 rounded-full mx-auto mb-2 flex items-center justify-center bg-indigo-500/10 text-indigo-400 border border-indigo-500/30">
                      <Server className="w-4 h-4" />
                    </div>
                    <span className="text-[11px] font-bold text-slate-200 block">Order Service</span>
                    <span className="text-[10px] text-emerald-400 flex items-center justify-center space-x-1 mt-1">
                      <CheckCircle2 className="w-3 h-3" />
                      <span>{trace.order.status}</span>
                    </span>
                  </div>

                  {/* Step 2: Payment */}
                  <div className="p-3 rounded-lg bg-[#111827] border border-slate-800">
                    <div className="w-8 h-8 rounded-full mx-auto mb-2 flex items-center justify-center bg-amber-500/10 text-amber-400 border border-amber-500/30">
                      <Cpu className="w-4 h-4" />
                    </div>
                    <span className="text-[11px] font-bold text-slate-200 block">Payment Service</span>
                    <span className={`text-[10px] flex items-center justify-center space-x-1 mt-1 ${
                      trace.order.payment_status === 'SUCCEEDED' ? 'text-emerald-400' : 'text-amber-400'
                    }`}>
                      <CheckCircle2 className="w-3 h-3" />
                      <span>{trace.order.payment_status}</span>
                    </span>
                  </div>

                  {/* Step 3: Inventory */}
                  <div className="p-3 rounded-lg bg-[#111827] border border-slate-800">
                    <div className="w-8 h-8 rounded-full mx-auto mb-2 flex items-center justify-center bg-cyan-500/10 text-cyan-400 border border-cyan-500/30">
                      <Database className="w-4 h-4" />
                    </div>
                    <span className="text-[11px] font-bold text-slate-200 block">Inventory Service</span>
                    <span className={`text-[10px] flex items-center justify-center space-x-1 mt-1 ${
                      trace.order.inventory_status === 'RESERVED' ? 'text-emerald-400' : 'text-cyan-400'
                    }`}>
                      <CheckCircle2 className="w-3 h-3" />
                      <span>{trace.order.inventory_status}</span>
                    </span>
                  </div>
                </div>
              </div>

              {/* Saga Milestones Timeline */}
              <div className="p-4 rounded-xl bg-slate-900/60 border border-slate-800">
                <h3 className="text-xs font-bold text-slate-300 uppercase tracking-wider mb-3 flex items-center space-x-2">
                  <Clock className="w-4 h-4 text-indigo-400" />
                  <span>Distributed Execution Timeline</span>
                </h3>

                <div className="space-y-3">
                  {trace.saga_timeline.map((m, idx) => (
                    <div key={idx} className="flex items-start space-x-3 text-xs">
                      <div className={`mt-0.5 w-5 h-5 rounded-full flex items-center justify-center flex-shrink-0 ${
                        m.status === 'COMPLETED'
                          ? 'bg-emerald-500/20 text-emerald-400 border border-emerald-500/40'
                          : m.status === 'FAILED'
                          ? 'bg-rose-500/20 text-rose-400 border border-rose-500/40'
                          : 'bg-indigo-500/20 text-indigo-400 border border-indigo-500/40 animate-pulse'
                      }`}>
                        {m.status === 'COMPLETED' ? <CheckCircle2 className="w-3 h-3" /> : <Clock className="w-3 h-3" />}
                      </div>
                      <div className="flex-1">
                        <div className="flex items-center space-x-2">
                          <span className="font-semibold text-slate-200">{m.step}</span>
                          <span className="px-1.5 py-0.2 text-[9px] rounded font-mono bg-slate-800 text-slate-400">
                            {m.service}
                          </span>
                        </div>
                        <p className="text-slate-400 mt-0.5">{m.detail}</p>
                      </div>
                    </div>
                  ))}
                </div>
              </div>

              {/* Transactional Outbox Events Emitted */}
              <div className="p-4 rounded-xl bg-slate-900/60 border border-slate-800">
                <div className="flex items-center justify-between mb-3">
                  <h3 className="text-xs font-bold text-slate-300 uppercase tracking-wider flex items-center space-x-2">
                    <Zap className="w-4 h-4 text-amber-400" />
                    <span>Transactional Outbox Records ({trace.outbox_events.length})</span>
                  </h3>
                  <span className="text-[10px] text-slate-400 font-mono">PostgreSQL SKIP LOCKED</span>
                </div>

                {trace.outbox_events.length === 0 ? (
                  <p className="text-xs text-slate-500 italic">No outbox events recorded for this aggregate.</p>
                ) : (
                  <div className="space-y-2">
                    {trace.outbox_events.map((evt) => (
                      <div key={evt.id} className="p-3 rounded-lg bg-[#0a0f1d] border border-slate-800 font-mono text-[11px]">
                        <div className="flex items-center justify-between text-slate-300 mb-1">
                          <span className="text-indigo-400 font-bold">{evt.event_type}</span>
                          <span className="px-1.5 py-0.5 text-[9px] rounded bg-emerald-500/10 text-emerald-400 border border-emerald-500/30">
                            {evt.status}
                          </span>
                        </div>
                        <p className="text-slate-500 text-[10px] truncate">Outbox ID: {evt.id}</p>
                        <p className="text-slate-500 text-[10px] truncate">Correlation: {evt.correlation_id || 'N/A'}</p>
                      </div>
                    ))}
                  </div>
                )}
              </div>

              {/* Idempotency Key Live Proof */}
              <div className="p-4 rounded-xl bg-slate-900/60 border border-slate-800">
                <div className="flex items-center justify-between mb-2">
                  <h3 className="text-xs font-bold text-slate-300 uppercase tracking-wider">
                    Idempotency Guarantee
                  </h3>
                  <button
                    onClick={() => handleCopy(trace.order.idempotency_key || '')}
                    className="flex items-center space-x-1 text-[11px] text-indigo-400 hover:text-indigo-300"
                  >
                    {copiedKey ? <Check className="w-3.5 h-3.5" /> : <Copy className="w-3.5 h-3.5" />}
                    <span>{copiedKey ? 'Copied' : 'Copy Key'}</span>
                  </button>
                </div>
                <div className="p-2.5 rounded-lg bg-[#0a0f1d] border border-slate-800 text-[11px] font-mono text-slate-400 break-all mb-3">
                  {trace.order.idempotency_key || 'None assigned'}
                </div>

                <button
                  onClick={handleSimulateDuplicate}
                  disabled={retryingIdempotency}
                  className="w-full py-2 px-3 rounded-lg text-xs font-semibold bg-slate-800 hover:bg-slate-700 text-slate-200 border border-slate-700 transition-colors flex items-center justify-center space-x-2"
                >
                  <RefreshCw className={`w-3.5 h-3.5 ${retryingIdempotency ? 'animate-spin' : ''}`} />
                  <span>Simulate Duplicate Network Request (Same Key)</span>
                </button>

                {idempotencyResult && (
                  <div className="mt-3 p-3 rounded-lg bg-emerald-500/10 border border-emerald-500/30 text-emerald-300 text-xs">
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
