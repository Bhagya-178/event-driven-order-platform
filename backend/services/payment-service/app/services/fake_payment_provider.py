import uuid

class FakePaymentProvider:
    # Deterministic behaviors
    # "SUCCESS_*" -> returns SUCCEEDED
    # "DECLINE_*" -> returns DECLINED
    # "TIMEOUT_*" -> raises Timeout
    # "ERROR_*" -> returns PROVIDER_ERROR

    async def process_payment(self, amount, currency, order_id, idempotency_key) -> dict:
        prefix = idempotency_key.split('_')[0].upper()
        
        provider_id = f"pi_{uuid.uuid4().hex}"
        
        if prefix == "DECLINE":
            return {"status": "FAILED"}
        if prefix == "TIMEOUT":
            raise Exception("Provider timeout")
        if prefix == "ERROR":
            return {"status": "PROVIDER_ERROR"}
            
        return {"status": "SUCCEEDED", "provider_id": provider_id}
