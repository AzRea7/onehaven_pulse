from datetime import UTC, date, datetime


def utc_now() -> datetime:
    return datetime.now(UTC)


def utc_timestamp_slug() -> str:
    return utc_now().strftime("%Y%m%dT%H%M%SZ")


def today_iso() -> str:
    return date.today().isoformat()
