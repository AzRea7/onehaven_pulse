from datetime import date
from decimal import Decimal

from pipelines.extractors.fema_nri.config import FEMA_NRI_COUNTY_RISK
from pipelines.loaders.fema_nri_loader import load_fema_nri_county_risk, _record_to_params


def test_record_to_params_fema_nri_county():
    record = {
        "COUNTYFIPS": "01001",
        "COUNTY": "Autauga",
        "STATE": "Alabama",
        "STATEABBRV": "AL",
        "RISK_SCORE": "50.1",
        "RISK_RATNG": "Relatively Moderate",
        "EAL_VALT": "123456.78",
        "EAL_SCORE": "44.2",
        "SOVI_SCORE": "60.3",
        "RESL_SCORE": "70.4",
    }

    params = _record_to_params(
        record=record,
        dataset=FEMA_NRI_COUNTY_RISK,
        source_file_id="source_file_1",
        load_date=date(2026, 5, 1),
    )

    assert params is not None
    assert params["county_fips"] == "01001"
    assert params["county_name"] == "Autauga"
    assert params["state_code"] == "AL"
    assert params["risk_score"] == Decimal("50.1")
    assert params["expected_annual_loss"] == Decimal("123456.78")
    assert params["social_vulnerability_score"] == Decimal("60.3")
    assert params["community_resilience_score"] == Decimal("70.4")


def test_load_fema_nri_county_risk_empty_payload_returns_zero():
    assert (
        load_fema_nri_county_risk(
            payload={"records": []},
            dataset=FEMA_NRI_COUNTY_RISK,
            source_file_id=None,
            load_date=date(2026, 5, 1),
        )
        == 0
    )
