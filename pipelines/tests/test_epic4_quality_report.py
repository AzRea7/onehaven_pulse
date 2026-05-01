from pipelines.quality.epic4_metric_catalog import EPIC4_METRIC_CATALOG
from pipelines.quality.run_epic4_quality_report import required_metric_status


def test_epic4_metric_catalog_not_empty():
    assert EPIC4_METRIC_CATALOG


def test_epic4_metric_catalog_has_required_metrics():
    required_names = {metric.metric_name for metric in EPIC4_METRIC_CATALOG if metric.required}

    assert "mortgage_rate_30y" in required_names
    assert "zhvi" in required_names
    assert "home_price_index" in required_names
    assert "population" in required_names
    assert "hmda_applications" in required_names
    assert "estimated_monthly_payment" in required_names


def test_epic4_metric_catalog_has_optional_overture():
    overture_metrics = [
        metric
        for metric in EPIC4_METRIC_CATALOG
        if metric.source == "overture_maps_api"
    ]

    assert overture_metrics
    assert all(not metric.required for metric in overture_metrics)


def test_required_metric_status_function_imports():
    assert callable(required_metric_status)
