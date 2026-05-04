from datetime import date

from app.core.errors import ApiError


DEFAULT_TIMESERIES_MONTHS = 60
MAX_TIMESERIES_MONTHS = 180


def month_start(value: date) -> date:
    return date(value.year, value.month, 1)


def add_months(value: date, months: int) -> date:
    month_index = value.year * 12 + value.month - 1 + months
    year = month_index // 12
    month = month_index % 12 + 1
    return date(year, month, 1)


def months_between(start_date: date, end_date: date) -> int:
    return (end_date.year - start_date.year) * 12 + (end_date.month - start_date.month)


def resolve_date_window(
    *,
    start_date: date | None,
    end_date: date | None,
    latest_period: date | None,
    default_months: int = DEFAULT_TIMESERIES_MONTHS,
    max_months: int = MAX_TIMESERIES_MONTHS,
) -> tuple[date | None, date | None, str]:
    if latest_period is None:
        return start_date, end_date, "explicit_or_empty"

    resolved_end = month_start(end_date or latest_period)

    if start_date is None:
        resolved_start = add_months(resolved_end, -(default_months - 1))
        source = "default_last_60_months"
    else:
        resolved_start = month_start(start_date)
        source = "explicit"

    if resolved_start > resolved_end:
        raise ApiError(
            status_code=422,
            code="invalid_date_range",
            message="start_date must be before or equal to end_date.",
            details={
                "start_date": resolved_start.isoformat(),
                "end_date": resolved_end.isoformat(),
            },
        )

    requested_months = months_between(resolved_start, resolved_end) + 1

    if requested_months > max_months:
        raise ApiError(
            status_code=422,
            code="date_range_too_large",
            message="Requested date range is too large.",
            details={
                "requested_months": requested_months,
                "maximum_months": max_months,
                "start_date": resolved_start.isoformat(),
                "end_date": resolved_end.isoformat(),
            },
        )

    return resolved_start, resolved_end, source
