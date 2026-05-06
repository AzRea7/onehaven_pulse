from fastapi.testclient import TestClient

from app.main import app

client = TestClient(app)

ACCEPTED_CYCLE_PHASES = {
    "Expansion",
    "Peak",
    "Correction",
    "Recovery",
    "Insufficient Data",
}

ACCEPTED_INVESTOR_SIGNALS = {
    "Strong Buy",
    "Buy",
    "Hold",
    "Watch",
    "Avoid",
    "Insufficient Data",
}


def test_market_context_classification_values_are_contract_safe():
    response = client.get("/markets/us/context")

    assert response.status_code in {200, 404}

    if response.status_code == 404:
        return

    payload = response.json()

    assert payload["cycle_phase"] in ACCEPTED_CYCLE_PHASES
    assert payload["investor_signal"] in ACCEPTED_INVESTOR_SIGNALS
