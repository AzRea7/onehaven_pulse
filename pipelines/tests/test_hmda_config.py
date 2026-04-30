from pipelines.extractors.hmda.config import HMDA_MODIFIED_LAR


def test_hmda_modified_lar_configured():
    assert HMDA_MODIFIED_LAR.dataset == "modified_lar"
    assert HMDA_MODIFIED_LAR.expected_frequency == "annual"
    assert HMDA_MODIFIED_LAR.filename.endswith(".csv")
    assert 2018 <= HMDA_MODIFIED_LAR.year <= 2023
    assert HMDA_MODIFIED_LAR.geography_type in {"states", "msamds", "counties", "leis"}
