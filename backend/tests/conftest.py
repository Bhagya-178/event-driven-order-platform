import sys
from pathlib import Path

# Add services and shared directories to sys.path
backend_dir = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(backend_dir / "services" / "order-service"))
sys.path.insert(0, str(backend_dir / "services" / "payment-service"))
sys.path.insert(0, str(backend_dir / "services" / "inventory-service"))
sys.path.insert(0, str(backend_dir / "shared"))
