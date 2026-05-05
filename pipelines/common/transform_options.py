from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from enum import StrEnum
from typing import Iterable


class TransformRunMode(StrEnum):
    FULL = "full"
    RECENT = "recent"
    SINCE = "since"


@dataclass(frozen=True)
class TransformOptions:
    mode: TransformRunMode = TransformRunMode.FULL
    start_date: date | None = None
    recent_months: int | None = None

    @property
    def is_incremental(self) -> bool:
        return self.mode in {TransformRunMode.RECENT, TransformRunMode.SINCE}


def parse_date(value: str | None) -> date | None:
    if not value:
        return None

    return date.fromisoformat(value)


def validate_transform_options(options: TransformOptions) -> TransformOptions:
    if options.mode == TransformRunMode.SINCE and options.start_date is None:
        raise ValueError("--start-date is required when --mode since")

    if options.mode == TransformRunMode.RECENT:
        if options.recent_months is None:
            raise ValueError("--recent-months is required when --mode recent")

        if options.recent_months <= 0:
            raise ValueError("--recent-months must be greater than zero")

    if options.mode == TransformRunMode.FULL:
        if options.start_date is not None:
            raise ValueError("--start-date is only valid with --mode since")

        if options.recent_months is not None:
            raise ValueError("--recent-months is only valid with --mode recent")

    return options


def build_transform_options(
    *,
    mode: str = "full",
    start_date: str | None = None,
    recent_months: int | None = None,
) -> TransformOptions:
    options = TransformOptions(
        mode=TransformRunMode(mode),
        start_date=parse_date(start_date),
        recent_months=recent_months,
    )

    return validate_transform_options(options)


def filter_records_by_start_date(records: Iterable, start_date: date | None):
    if start_date is None:
        return list(records)

    return [
        record
        for record in records
        if getattr(record, "period_month") >= start_date
    ]
