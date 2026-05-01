from pipelines.orchestration.raw_pipeline_registry import (
    RAW_PIPELINES,
    get_pipeline_definition,
    list_pipeline_names,
    resolve_pipeline_names,
)


def test_raw_pipeline_names_are_unique():
    names = [pipeline.name for pipeline in RAW_PIPELINES]

    assert len(names) == len(set(names))


def test_list_pipeline_names_contains_expected_sources():
    names = list_pipeline_names()

    assert "fred" in names
    assert "fhfa" in names
    assert "census_acs" in names
    assert "fema_nri" in names
    assert "overture_maps" in names


def test_get_pipeline_definition():
    definition = get_pipeline_definition("fred")

    assert definition.name == "fred"
    assert definition.module == "pipelines.extractors.fred.extract"


def test_get_pipeline_definition_rejects_unknown():
    try:
        get_pipeline_definition("bad_pipeline")
    except ValueError as exc:
        assert "Unknown raw pipeline" in str(exc)
    else:
        raise AssertionError("Expected ValueError for unknown pipeline")


def test_resolve_pipeline_names_all():
    names = resolve_pipeline_names(["all"])

    assert names == list_pipeline_names()


def test_resolve_pipeline_names_subset():
    names = resolve_pipeline_names(["fred", "fema_nri"])

    assert names == ["fred", "fema_nri"]
