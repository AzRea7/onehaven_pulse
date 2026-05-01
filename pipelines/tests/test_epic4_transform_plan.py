from pipelines.orchestration.epic4_transform_plan import EPIC4_TRANSFORM_PLAN
from pipelines.transforms.registry.transform_registry import TRANSFORMS


def test_epic4_transform_plan_not_empty():
    assert EPIC4_TRANSFORM_PLAN


def test_epic4_transform_plan_steps_are_registered():
    missing = [step.name for step in EPIC4_TRANSFORM_PLAN if step.name not in TRANSFORMS]

    assert missing == []


def test_epic4_transform_plan_runs_derived_last():
    assert EPIC4_TRANSFORM_PLAN[-1].name == "derived_market_ratios"


def test_epic4_transform_plan_contains_hmda_and_overture():
    names = {step.name for step in EPIC4_TRANSFORM_PLAN}

    assert "hmda_mortgage_credit" in names
    assert "overture_places_amenities" in names
