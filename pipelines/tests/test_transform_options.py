from datetime import date

import pytest

from pipelines.common.periods import recent_month_cutoff
from pipelines.common.transform_options import (
    TransformRunMode,
    build_transform_options,
)


def test_build_full_options():
    options = build_transform_options(mode="full")

    assert options.mode == TransformRunMode.FULL
    assert options.start_date is None
    assert options.recent_months is None
    assert options.is_incremental is False


def test_build_since_options():
    options = build_transform_options(mode="since", start_date="2025-01-01")

    assert options.mode == TransformRunMode.SINCE
    assert options.start_date == date(2025, 1, 1)
    assert options.is_incremental is True


def test_since_requires_start_date():
    with pytest.raises(ValueError):
        build_transform_options(mode="since")


def test_build_recent_options():
    options = build_transform_options(mode="recent", recent_months=6)

    assert options.mode == TransformRunMode.RECENT
    assert options.recent_months == 6
    assert options.is_incremental is True


def test_recent_requires_recent_months():
    with pytest.raises(ValueError):
        build_transform_options(mode="recent")


def test_recent_month_cutoff():
    assert recent_month_cutoff(date(2026, 3, 1), 3) == date(2026, 1, 1)
