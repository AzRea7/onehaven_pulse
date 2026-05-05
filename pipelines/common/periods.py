from __future__ import annotations

from datetime import date

from dateutil.relativedelta import relativedelta


def month_start(value: date) -> date:
    return date(value.year, value.month, 1)


def recent_month_cutoff(latest_period: date, recent_months: int) -> date:
    if recent_months <= 0:
        raise ValueError("recent_months must be greater than zero")

    return month_start(latest_period) - relativedelta(months=recent_months - 1)
