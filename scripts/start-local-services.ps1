# start-local-services.ps1
# Helper script to launch all 3 microservices locally against the running Docker infrastructure (PostgreSQL:5433, Redis:6379, Kafka:9092)

$root = "$PSScriptRoot\.."
$python = "$root\backend\services\inventory-service\.venv\Scripts\python.exe"

Write-Host "==========================================================" -ForegroundColor Cyan
Write-Host "  Event-Driven Order Platform - Microservices Launcher    " -ForegroundColor Cyan
Write-Host "==========================================================" -ForegroundColor Cyan

# 1. Order Service (:8000)
Write-Host "[1/3] Starting Order Service on port 8000..." -ForegroundColor Green
Start-Process -FilePath "powershell.exe" -ArgumentList "-NoExit", "-Command", "`$env:ORDER_DB_URL='postgresql+asyncpg://order_user:order_pass@localhost:5433/orders_db'; `$env:REDIS_URL='redis://localhost:6379/0'; `$env:KAFKA_BOOTSTRAP_SERVERS='localhost:9092'; `$env:PYTHONPATH='$root\backend\shared;$root\backend\services\order-service'; cd '$root\backend\services\order-service'; & '$python' -m uvicorn app.main:app --port 8000 --host 0.0.0.0 --reload"

# 2. Payment Service (:8001)
Write-Host "[2/3] Starting Payment Service on port 8001..." -ForegroundColor Green
Start-Process -FilePath "powershell.exe" -ArgumentList "-NoExit", "-Command", "`$env:PAYMENT_DB_URL='postgresql+asyncpg://payment_user:payment_pass@localhost:5433/payments_db'; `$env:REDIS_URL='redis://localhost:6379/0'; `$env:KAFKA_BOOTSTRAP_SERVERS='localhost:9092'; `$env:PYTHONPATH='$root\backend\shared;$root\backend\services\payment-service'; cd '$root\backend\services\payment-service'; & '$python' -m uvicorn app.main:app --port 8001 --host 0.0.0.0 --reload"

# 3. Inventory Service (:8002)
Write-Host "[3/3] Starting Inventory Service on port 8002..." -ForegroundColor Green
Start-Process -FilePath "powershell.exe" -ArgumentList "-NoExit", "-Command", "`$env:INVENTORY_DB_URL='postgresql+asyncpg://inventory_user:inventory_pass@localhost:5433/inventory_db'; `$env:REDIS_URL='redis://localhost:6379/0'; `$env:KAFKA_BOOTSTRAP_SERVERS='localhost:9092'; `$env:PYTHONPATH='$root\backend\shared;$root\backend\services\inventory-service'; cd '$root\backend\services\inventory-service'; & '$python' -m uvicorn app.main:app --port 8002 --host 0.0.0.0 --reload"

Write-Host "All 3 services launched in separate windows!" -ForegroundColor Yellow
Write-Host "Frontend Probes: http://localhost:8000/health/ready, :8001/health/ready, :8002/health/ready" -ForegroundColor Cyan
