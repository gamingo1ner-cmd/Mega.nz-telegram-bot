from services.payment_service import verify_crypto_payment

def test_payment():
    assert verify_crypto_payment("tx123")
