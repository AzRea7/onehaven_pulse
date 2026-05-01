from pipelines.orchestration.epic4_transform_plan import EPIC4_TRANSFORM_PLAN
from pipelines.orchestration.run_epic4_transforms import _selected_plan
from pipelines.transforms.registry.transform_registry import TRANSFORMS


def test_epic4_plan_registered():
    missing = [step.name for step in EPIC4_TRANSFORM_PLAN if step.name not in TRANSFORMS]

    assert missing == []


def test_epic4_plan_has_derived_last():
    assert EPIC4_TRANSFORM_PLAN[-1].name == "derived_market_ratios"


def test_selected_plan_only():
    selected = _selected_plan("hmda_mortgage_credit", skip_optional=False)

    assert len(selected) == 1
    assert selected[0].name == "hmda_mortgage_credit"


def test_selected_plan_skip_optional():
    selected = _selected_plan(None, skip_optional=True)

    assert all(step.required for step in selected)
    assert "overture_places_amenities" not in {step.name for step in selected}
