import React from 'react';
import { ShoppingBag, LayoutDashboard, Cpu } from 'lucide-react';

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
    <header className="sticky top-0 z-40 border-b border-slate-800/80 bg-[#090d16]/90 backdrop-blur-md">
      <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8">
        <div className="flex items-center justify-between h-16">
          {/* Brand Logo & Tag */}
          <div className="flex items-center space-x-3">
            <div className="w-10 h-10 rounded-xl bg-gradient-to-tr from-indigo-600 via-indigo-500 to-cyan-400 flex items-center justify-center shadow-lg shadow-indigo-500/20">
              <Cpu className="w-5 h-5 text-white" />
            </div>
            <div>
              <div className="flex items-center space-x-2">
                <span className="font-extrabold tracking-tight text-white text-lg">NovaCloud</span>
                <span className="px-2 py-0.5 text-[10px] font-bold rounded-full uppercase tracking-wider bg-indigo-500/10 text-indigo-400 border border-indigo-500/30">
                  Event-Driven
                </span>
              </div>
              <p className="text-xs text-slate-400 hidden sm:block">Kafka Saga Architecture</p>
            </div>
          </div>

          {/* Central Portal Switcher */}
          <div className="flex items-center p-1 bg-slate-900/90 rounded-xl border border-slate-800">
            <button
              onClick={() => onSwitchView('customer')}
              className={`flex items-center space-x-2 px-3.5 py-1.5 rounded-lg text-xs font-semibold transition-all ${
                currentView === 'customer'
                  ? 'bg-indigo-600 text-white shadow-md shadow-indigo-600/30'
                  : 'text-slate-400 hover:text-slate-200'
              }`}
            >
              <ShoppingBag className="w-3.5 h-3.5" />
              <span>Customer Storefront</span>
            </button>

            <button
              onClick={() => onSwitchView('business')}
              className={`flex items-center space-x-2 px-3.5 py-1.5 rounded-lg text-xs font-semibold transition-all ${
                currentView === 'business'
                  ? 'bg-indigo-600 text-white shadow-md shadow-indigo-600/30'
                  : 'text-slate-400 hover:text-slate-200'
              }`}
            >
              <LayoutDashboard className="w-3.5 h-3.5" />
              <span>Business Console</span>
            </button>
          </div>

          {/* Right Action: Cluster Status & Cart */}
          <div className="flex items-center space-x-3">
            {/* Cluster Health Pill */}
            <div className="hidden md:flex items-center space-x-2 px-2.5 py-1 rounded-full bg-slate-900 border border-slate-800 text-xs">
              <span className={`w-2 h-2 rounded-full ${backendOnline ? 'bg-emerald-400 animate-pulse' : 'bg-rose-500'}`} />
              <span className="text-slate-300 text-[11px] font-medium">
                {backendOnline ? 'Cluster Active' : 'Offline / Standalone'}
              </span>
            </div>

            {/* Cart Button (Visible on Customer view) */}
            {currentView === 'customer' && (
              <button
                onClick={onOpenCart}
                className="relative p-2 rounded-xl bg-slate-900 border border-slate-800 hover:border-slate-700 text-slate-200 transition-all"
                title="View Cart"
              >
                <ShoppingBag className="w-5 h-5 text-indigo-400" />
                {cartCount > 0 && (
                  <span className="absolute -top-1.5 -right-1.5 w-5 h-5 rounded-full bg-indigo-600 text-white text-[10px] font-bold flex items-center justify-center border-2 border-[#090d16]">
                    {cartCount}
                  </span>
                )}
              </button>
            )}
          </div>
        </div>
      </div>
    </header>
  );
};
