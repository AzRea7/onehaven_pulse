from datetime import date

from pipelines.extractors.bls_laus.config import BLS_LAUS_DATASET
from pipelines.loaders.bls_laus_loader import _period_to_month, load_bls_laus_observations


def test_period_to_month():
    assert _period_to_month(2026, "M01") == date(2026, 1, 1)
    assert _period_to_month(2026, "M12") == date(2026, 12, 1)
    assert _period_to_month(2026, "M13") is None
    assert _period_to_month(2026, "Q01") is None


def test_load_bls_laus_observations_empty_payload_returns_zero():
    assert (
        load_bls_laus_observations(
            payload={"response": {"Results": {"series": []}}},
            dataset=BLS_LAUS_DATASET,
            source_file_id=None,
            load_date=date(2026, 5, 1),
        )
        == 0
    )
