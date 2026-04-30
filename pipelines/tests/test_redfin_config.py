from pipelines.extractors.redfin.config import REDFIN_MARKET_TRACKER


def test_redfin_market_tracker_configured():
    assert REDFIN_MARKET_TRACKER.dataset == "market_tracker"
    assert REDFIN_MARKET_TRACKER.metric_name == "market_tracker"
    assert REDFIN_MARKET_TRACKER.filename == "market_tracker.csv"
    assert REDFIN_MARKET_TRACKER.expected_frequency == "monthly"
    assert "third full week" in REDFIN_MARKET_TRACKER.release_cadence_note
