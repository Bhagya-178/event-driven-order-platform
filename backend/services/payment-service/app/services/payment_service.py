import asyncio
from uuid import UUID
from sqlalchemy.ext.asyncio import AsyncSession
from app.repositories.payment_repository import PaymentRepository
from app.models.payment import Payment
from app.schemas.payment import PaymentCreate
from app.services.fake_payment_provider import FakePaymentProvider

class PaymentService:
    _sqlite_lock = asyncio.Lock()

    def __init__(self, session: AsyncSession):
        self.session = session
        self.repo = PaymentRepository(session)
        self.provider = FakePaymentProvider()

    async def get_payment(self, payment_id: UUID) -> Payment:
        payment = await self.repo.get_by_id(payment_id)
        if not payment:
            raise ValueError(f"Payment {payment_id} not found")
        return payment

    async def process_payment(self, data: PaymentCreate, idempotency_key: str) -> Payment:
        is_postgres = self.session.bind and self.session.bind.dialect.name == "postgresql"
        if not is_postgres:
            await self._sqlite_lock.acquire()
        try:
            # Check idempotency first (must be transaction safe, using DB unique constraint covers race conditions)
            existing_payment = await self.repo.get_by_idempotency_key(idempotency_key)
            if existing_payment:
                return existing_payment
                
            payment = Payment(
                order_id=data.order_id,
                idempotency_key=idempotency_key,
                amount=data.amount,
                currency=data.currency,
                status="PENDING"
            )
            self.repo.add(payment)
            
            # Flush to get the payment ID and ensure unique constraint holds
            try:
                await self.session.flush()
            except Exception as e:
                await self.session.rollback()
                for _ in range(10):
                    await asyncio.sleep(0.05)
                    existing = await self.repo.get_by_idempotency_key(idempotency_key)
                    if existing:
                        return existing
                raise ValueError("Idempotency key conflict") from e

            # Mock Provider processing
            try:
                result = await self.provider.process_payment(
                    amount=payment.amount,
                    currency=payment.currency,
                    order_id=payment.order_id,
                    idempotency_key=idempotency_key
                )
                
                if result["status"] == "SUCCEEDED":
                    payment.status = "SUCCEEDED"
                    payment.provider_payment_id = result.get("provider_id")
                elif result["status"] == "FAILED":
                    payment.status = "FAILED"
                else:
                    payment.status = "FAILED"
                    
            except Exception as e:
                # e.g. Timeout
                payment.status = "FAILED"
                
            await self.session.commit()
            return payment
        finally:
            if not is_postgres:
                self._sqlite_lock.release()
