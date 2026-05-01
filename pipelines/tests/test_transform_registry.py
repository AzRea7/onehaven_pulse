from pipelines.transforms.registry.transform_registry import (
    get_transform_definition,
    list_transform_names,
    resolve_transform_names,
)


def test_transform_registry_lists_smoke_transform():
    assert "smoke_market_metric" in list_transform_names()


def test_get_transform_definition():
    definition = get_transform_definition("smoke_market_metric")

    assert definition.name == "smoke_market_metric"
    assert definition.target_table == "analytics.market_monthly_metrics"


def test_resolve_transform_names_all():
    assert resolve_transform_names(["all"]) == list_transform_names()


def test_resolve_transform_names_rejects_unknown():
    try:
        resolve_transform_names(["bad_transform"])
    except ValueError as exc:
        assert "Unknown transform" in str(exc)
    else:
        raise AssertionError("Expected ValueError")
