import React from 'react';
import { ShoppingCart, Activity, Layers, Server, ShieldCheck } from 'lucide-react';

interface NavbarProps {
  currentView: 'customer' | 'business';
  onSwitchView: (view: 'customer' | 'business') => void;
  cartCount: number;
  onOpenCart: () => void;
  backendOnline: boolean;
}

export const Navbar: React.FC<NavbarProps> = ({
  currentView,
  onSwitchView,
  cartCount,
  onOpenCart,
  backendOnline,
}) => {
  return (
    <header className="sticky top-0 z-40 border-b border-zinc-800/80 bg-[#0b0c10]/95 backdrop-blur-xl">
      <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8">
        <div className="flex items-center justify-between h-16">
          {/* Brand & System Architecture Badge */}
          <div className="flex items-center space-x-3.5">
            <div className="w-8 h-8 rounded-lg bg-zinc-900 border border-zinc-700/80 flex items-center justify-center shadow-inner">
              <Layers className="w-4 h-4 text-emerald-400" />
            </div>
            <div>
              <div className="flex items-center space-x-2">
                <span className="font-semibold tracking-tight text-zinc-100 text-sm">
                  NEXUS <span className="text-zinc-500 font-normal">EVENT PLATFORM</span>
                </span>
                <span className="px-1.5 py-0.5 text-[10px] font-mono font-medium rounded bg-zinc-800/80 text-zinc-400 border border-zinc-700/60">
                  v1.4.0
                </span>
              </div>
              <div className="flex items-center space-x-2 text-[11px] text-zinc-400 font-mono">
                <span>PostgreSQL 15</span>
                <span className="text-zinc-600">&bull;</span>
                <span>Kafka 7.5</span>
                <span className="text-zinc-600">&bull;</span>
                <span>Redis 7</span>
              </div>
            </div>
          </div>

          {/* Central Portal Switcher (Clean Segmented Control) */}
          <div className="flex items-center p-0.5 bg-zinc-900/90 rounded-lg border border-zinc-800 text-xs">
            <button
              onClick={() => onSwitchView('customer')}
              className={`flex items-center space-x-2 px-3 py-1.5 rounded-md font-medium transition-all ${
                currentView === 'customer'
                  ? 'bg-zinc-800 text-zinc-100 shadow-sm border border-zinc-700/60'
                  : 'text-zinc-400 hover:text-zinc-200'
              }`}
            >
              <ShoppingCart className="w-3.5 h-3.5" />
              <span>Customer Storefront</span>
            </button>

            <button
              onClick={() => onSwitchView('business')}
              className={`flex items-center space-x-2 px-3 py-1.5 rounded-md font-medium transition-all ${
                currentView === 'business'
                  ? 'bg-zinc-800 text-zinc-100 shadow-sm border border-zinc-700/60'
                  : 'text-zinc-400 hover:text-zinc-200'
              }`}
            >
              <Activity className="w-3.5 h-3.5" />
              <span>SRE & Saga Operations</span>
            </button>
          </div>

          {/* Right Action: Cluster Status & Cart */}
          <div className="flex items-center space-x-3">
            {/* Real-time Health Badge */}
            <div className="hidden sm:flex items-center space-x-2 px-2.5 py-1 rounded-md bg-zinc-900 border border-zinc-800 text-xs font-mono">
              <span className={`w-1.5 h-1.5 rounded-full ${backendOnline ? 'bg-emerald-400 shadow-[0_0_8px_rgba(52,211,153,0.6)]' : 'bg-rose-500'}`} />
              <span className="text-zinc-300 text-[11px]">
                {backendOnline ? 'CLUSTER OPERATIONAL' : 'SYSTEM DEGRADED'}
              </span>
            </div>

            {/* Cart Trigger (Customer Portal) */}
            {currentView === 'customer' && (
              <button
                onClick={onOpenCart}
                className="relative flex items-center space-x-2 px-3 py-1.5 rounded-md bg-zinc-900 border border-zinc-800 hover:border-zinc-700 hover:bg-zinc-800/60 text-zinc-200 text-xs font-medium transition-all"
              >
                <ShoppingCart className="w-3.5 h-3.5 text-zinc-300" />
                <span className="hidden sm:inline">Cart</span>
                <span className={`px-1.5 py-0.2 rounded text-[10px] font-mono font-bold ${cartCount > 0 ? 'bg-emerald-500/20 text-emerald-300 border border-emerald-500/30' : 'bg-zinc-800 text-zinc-400'}`}>
                  {cartCount}
                </span>
              </button>
            )}
          </div>
        </div>
      </div>
    </header>
  );
};

