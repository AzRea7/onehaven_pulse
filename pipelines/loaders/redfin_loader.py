import csv
from datetime import date
from decimal import Decimal, InvalidOperation
from io import StringIO
from typing import Any

from sqlalchemy import text

from pipelines.common.db import engine


UPSERT_REDFIN_MARKET_TRACKER_SQL = text(
    """
    INSERT INTO raw.redfin_market_tracker (
        source_region_id,
        region_name,
        region_type,
        state_code,
        property_type,
        period_month,
        median_sale_price,
        homes_sold,
        pending_sales,
        new_listings,
        active_listings,
        months_supply,
        median_days_on_market,
        sale_to_list_ratio,
        price_drops_pct,
        source_file_id,
        load_date
    )
    VALUES (
        :source_region_id,
        :region_name,
        :region_type,
        :state_code,
        :property_type,
        :period_month,
        :median_sale_price,
        :homes_sold,
        :pending_sales,
        :new_listings,
        :active_listings,
        :months_supply,
        :median_days_on_market,
        :sale_to_list_ratio,
        :price_drops_pct,
        :source_file_id,
        :load_date
    )
    ON CONFLICT (
        region_name,
        region_type,
        property_type,
        period_month,
        load_date
    )
    DO UPDATE SET
        source_region_id = EXCLUDED.source_region_id,
        state_code = EXCLUDED.state_code,
        median_sale_price = EXCLUDED.median_sale_price,
        homes_sold = EXCLUDED.homes_sold,
        pending_sales = EXCLUDED.pending_sales,
        new_listings = EXCLUDED.new_listings,
        active_listings = EXCLUDED.active_listings,
        months_supply = EXCLUDED.months_supply,
        median_days_on_market = EXCLUDED.median_days_on_market,
        sale_to_list_ratio = EXCLUDED.sale_to_list_ratio,
        price_drops_pct = EXCLUDED.price_drops_pct,
        source_file_id = EXCLUDED.source_file_id
    """
)


MONTH_NAMES = {
    "january": 1,
    "february": 2,
    "march": 3,
    "april": 4,
    "may": 5,
    "june": 6,
    "july": 7,
    "august": 8,
    "september": 9,
    "october": 10,
    "november": 11,
    "december": 12,
}


def _decode_csv(content: bytes) -> str:
    for encoding in ("utf-8-sig", "utf-16", "utf-16-le", "utf-16-be", "latin-1"):
        try:
            return content.decode(encoding)
        except UnicodeDecodeError:
            continue

    return content.decode("latin-1", errors="replace")


def _detect_delimiter(text: str) -> str:
    first_line = text.splitlines()[0] if text.splitlines() else ""

    if "\t" in first_line:
        return "\t"

    return ","


def _clean_key(value: str) -> str:
    return (
        value.strip()
        .lower()
        .replace("\ufeff", "")
        .replace(" ", "_")
        .replace("-", "_")
        .replace("/", "_")
        .replace("%", "pct")
    )


def _normalize_row(row: dict[str, Any]) -> dict[str, Any]:
    return {_clean_key(str(key)): value for key, value in row.items()}


def _first_value(row: dict[str, Any], keys: tuple[str, ...]) -> Any:
    for key in keys:
        value = row.get(key)

        if value not in (None, ""):
            return value

    return None


def _parse_period_month(value: Any) -> date | None:
    if value is None:
        return None

    raw = str(value).strip()

    if not raw:
        return None

    try:
        parsed = date.fromisoformat(raw[:10])
        return date(parsed.year, parsed.month, 1)
    except ValueError:
        pass

    parts = raw.split("/")

    if len(parts) == 3:
        try:
            month = int(parts[0])
            year = int(parts[2])
            return date(year, month, 1)
        except ValueError:
            return None

    month_year = raw.lower().split()

    if len(month_year) == 2 and month_year[0] in MONTH_NAMES:
        try:
            return date(int(month_year[1]), MONTH_NAMES[month_year[0]], 1)
        except ValueError:
            return None

    return None


def _parse_decimal(value: Any) -> Decimal | None:
    if value is None:
        return None

    raw = str(value).strip()

    if not raw or raw == "." or raw.upper() in {"N/A", "NA", "NULL"}:
        return None

    multiplier = Decimal("1")

    raw = raw.replace(",", "").replace("%", "").replace("$", "")

    if raw.endswith(("K", "k")):
        multiplier = Decimal("1000")
        raw = raw[:-1]

    if raw.endswith(("M", "m")):
        multiplier = Decimal("1000000")
        raw = raw[:-1]

    if raw.endswith(("B", "b")):
        multiplier = Decimal("1000000000")
        raw = raw[:-1]

    try:
        return Decimal(raw) * multiplier
    except InvalidOperation:
        return None


def _normalize_ratio(value: Decimal | None) -> Decimal | None:
    if value is None:
        return None

    # "95.7%" should become 0.957.
    if value > Decimal("10"):
        return value / Decimal("100")

    return value


def _normalize_pct(value: Decimal | None) -> Decimal | None:
    if value is None:
        return None

    # If source is 0.12, store 12.0 percent.
    if Decimal("-1") <= value <= Decimal("1"):
        return value * Decimal("100")

    return value


def _infer_region_type(region_name: str) -> str | None:
    normalized = region_name.strip().lower()

    if normalized in {"united states", "us", "usa"}:
        return "country"

    if " metro area" in normalized or normalized.endswith(" metro"):
        return "metro"

    if len(normalized) == 2:
        return "state"

    return None


def _clean_region_name(region_name: str) -> str:
    return (
        region_name.strip()
        .replace(" metro area", "")
        .replace(" Metro Area", "")
        .replace(" metro", "")
        .replace(" Metro", "")
    )


def _state_code_from_region(region_name: str) -> str | None:
    # Examples: "Boston, MA metro area" -> MA
    if "," not in region_name:
        return None

    tail = region_name.split(",")[-1].strip()
    state = tail.split()[0].strip()

    if len(state) == 2:
        return state.upper()

    return None


def _record_to_params(
    row: dict[str, Any],
    *,
    source_file_id: str | None,
    load_date: date,
) -> dict | None:
    normalized = _normalize_row(row)

    region_name_raw = _first_value(
        normalized,
        (
            "region",
            "region_name",
            "parent_metro_region",
            "market_name",
            "city",
            "county",
            "state",
        ),
    )

    period_month = _parse_period_month(
        _first_value(
            normalized,
            (
                "month_of_period_end",
                "period_begin",
                "period_end",
                "month",
                "period",
                "date",
                "last_updated",
            ),
        )
    )

    if period_month is None or not region_name_raw:
        return None

    region_name_raw = str(region_name_raw).strip()
    region_type = str(_first_value(normalized, ("region_type",)) or "").strip() or None
    region_type = region_type or _infer_region_type(region_name_raw)

    state_code = (
        str(_first_value(normalized, ("state_code", "state", "state_or_province")) or "").strip()
        or _state_code_from_region(region_name_raw)
    )

    median_sale_price = _parse_decimal(
        _first_value(
            normalized,
            (
                "median_sale_price",
                "median_sale_price_usd",
                "median_price",
            ),
        )
    )
    homes_sold = _parse_decimal(
        _first_value(normalized, ("homes_sold", "sales", "closed_sales"))
    )
    pending_sales = _parse_decimal(
        _first_value(normalized, ("pending_sales", "pending_sales_count"))
    )
    new_listings = _parse_decimal(
        _first_value(normalized, ("new_listings", "new_listing_count"))
    )
    active_listings = _parse_decimal(
        _first_value(
            normalized,
            (
                "active_listings",
                "inventory",
                "active_listing_count",
                "homes_for_sale",
            ),
        )
    )
    months_supply = _parse_decimal(
        _first_value(
            normalized,
            (
                "months_supply",
                "months_of_supply",
                "months_of_inventory",
            ),
        )
    )
    median_days_on_market = _parse_decimal(
        _first_value(
            normalized,
            (
                "days_on_market",
                "median_days_on_market",
                "median_dom",
                "median_days_to_sale",
            ),
        )
    )
    sale_to_list_ratio = _normalize_ratio(
        _parse_decimal(
            _first_value(
                normalized,
                (
                    "average_sale_to_list",
                    "sale_to_list_ratio",
                    "avg_sale_to_list",
                    "average_sale_to_list_ratio",
                ),
            )
        )
    )
    price_drops_pct = _normalize_pct(
        _parse_decimal(
            _first_value(
                normalized,
                (
                    "price_drops_pct",
                    "price_drops",
                    "price_drop_pct",
                    "price_drops_percent",
                ),
            )
        )
    )

    return {
        "source_region_id": str(
            _first_value(
                normalized,
                (
                    "table_id",
                    "region_id",
                    "region_type_id",
                    "parent_metro_region_metro_code",
                ),
            )
            or ""
        )
        or None,
        "region_name": _clean_region_name(region_name_raw),
        "region_type": region_type,
        "state_code": state_code,
        "property_type": str(
            _first_value(normalized, ("property_type", "property_type_name")) or "All Residential"
        ).strip(),
        "period_month": period_month,
        "median_sale_price": median_sale_price,
        "homes_sold": homes_sold,
        "pending_sales": pending_sales,
        "new_listings": new_listings,
        "active_listings": active_listings,
        "months_supply": months_supply,
        "median_days_on_market": median_days_on_market,
        "sale_to_list_ratio": sale_to_list_ratio,
        "price_drops_pct": price_drops_pct,
        "source_file_id": source_file_id,
        "load_date": load_date,
    }


def parse_redfin_market_tracker_csv(
    *,
    content: bytes,
    source_file_id: str | None,
    load_date: date,
) -> list[dict]:
    text = _decode_csv(content)
    delimiter = _detect_delimiter(text)
    reader = csv.DictReader(StringIO(text), delimiter=delimiter)

    return [
        parsed
        for row in reader
        if (
            parsed := _record_to_params(
                row,
                source_file_id=source_file_id,
                load_date=load_date,
            )
        )
        is not None
    ]


def load_redfin_market_tracker(
    *,
    content: bytes,
    source_file_id: str | None,
    load_date: date,
) -> int:
    params = parse_redfin_market_tracker_csv(
        content=content,
        source_file_id=source_file_id,
        load_date=load_date,
    )

    if not params:
        return 0

    with engine.begin() as connection:
        connection.execute(UPSERT_REDFIN_MARKET_TRACKER_SQL, params)

    return len(params)
