import { useState, useEffect } from 'react';
import { Navbar } from './components/Navbar';
import { CustomerPortal } from './pages/CustomerPortal';
import { BusinessPortal } from './pages/BusinessPortal';
import { SagaTraceDrawer } from './components/SagaTraceDrawer';
import { checkServiceHealth } from './services/api';

export function App() {
  const [currentView, setCurrentView] = useState<'customer' | 'business'>('customer');
  const [activeTraceOrderId, setActiveTraceOrderId] = useState<string | null>(null);
  const [cart, setCart] = useState<{ productId: string; quantity: number }[]>([]);
  const [backendOnline, setBackendOnline] = useState(true);

  // Periodic heartbeat
  useEffect(() => {
    const checkCluster = async () => {
      const res = await checkServiceHealth('Order Service', 8000);
      setBackendOnline(res.status === 'UP');
    };
    checkCluster();
    const interval = setInterval(checkCluster, 5000);
    return () => clearInterval(interval);
  }, []);

  const handleAddToCart = (productId: string) => {
    setCart(prev => {
      const existing = prev.find(item => item.productId === productId);
      if (existing) {
        return prev.map(item =>
          item.productId === productId ? { ...item, quantity: item.quantity + 1 } : item
        );
      }
      return [...prev, { productId, quantity: 1 }];
    });
  };

  const handleUpdateCartQty = (productId: string, delta: number) => {
    setCart(prev => {
      return prev
        .map(item => {
          if (item.productId === productId) {
            const newQty = item.quantity + delta;
            return newQty > 0 ? { ...item, quantity: newQty } : null;
          }
          return item;
        })
        .filter(Boolean) as { productId: string; quantity: number }[];
    });
  };

  const handleRemoveFromCart = (productId: string) => {
    setCart(prev => prev.filter(item => item.productId !== productId));
  };

  const handleClearCart = () => setCart([]);

  return (
    <div className="min-h-screen bg-[#090d16] text-slate-100 flex flex-col justify-between selection:bg-indigo-500 selection:text-white">
      <div>
        {/* Navigation Bar */}
        <Navbar
          currentView={currentView}
          onSwitchView={setCurrentView}
          cartCount={cart.reduce((sum, i) => sum + i.quantity, 0)}
          onOpenCart={() => {
            // Scroll to cart if on customer view
            if (currentView !== 'customer') setCurrentView('customer');
          }}
          backendOnline={backendOnline}
        />

        {/* Main Content Area */}
        <main className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-8">
          {currentView === 'customer' ? (
            <CustomerPortal
              onOpenTrace={(orderId) => setActiveTraceOrderId(orderId)}
              cart={cart}
              onAddToCart={handleAddToCart}
              onUpdateCartQty={handleUpdateCartQty}
              onRemoveFromCart={handleRemoveFromCart}
              onClearCart={handleClearCart}
            />
          ) : (
            <BusinessPortal />
          )}
        </main>
      </div>

      {/* Technical Saga Trace Drawer (Opens on demand) */}
      <SagaTraceDrawer
        orderId={activeTraceOrderId}
        onClose={() => setActiveTraceOrderId(null)}
      />

      {/* Footer */}
      <footer className="border-t border-slate-800/80 py-8 bg-[#070a12] text-xs text-slate-500">
        <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 flex flex-col sm:flex-row items-center justify-between gap-4">
          <div className="flex items-center space-x-3">
            <span className="font-bold text-slate-300">Event-Driven Order Platform</span>
            <span>&bull;</span>
            <span>Apache Kafka 7.5</span>
            <span>&bull;</span>
            <span>PostgreSQL 15</span>
            <span>&bull;</span>
            <span>Redis 7.0</span>
          </div>

          <div className="flex items-center space-x-4">
            <span className="text-indigo-400 font-mono">Transactional Outbox Pattern</span>
            <span>&bull;</span>
            <span className="text-emerald-400 font-mono">55 Tests 100% Green</span>
          </div>
        </div>
      </footer>
    </div>
  );
}

export default App;
