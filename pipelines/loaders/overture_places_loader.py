import json
from datetime import date
from decimal import Decimal
from typing import Any

from sqlalchemy import text

from pipelines.common.db import engine


INSERT_SQL = text(
    """
    INSERT INTO raw.overture_places (
        area_slug,
        area_name,
        place_id,
        name,
        category,
        primary_category,
        latitude,
        longitude,
        source_payload,
        source_file_id,
        load_date
    )
    VALUES (
        :area_slug,
        :area_name,
        :place_id,
        :name,
        :category,
        :primary_category,
        :latitude,
        :longitude,
        CAST(:source_payload AS JSONB),
        :source_file_id,
        :load_date
    )
    """
)


def _first(record: dict[str, Any], keys: tuple[str, ...]) -> Any:
    for key in keys:
        value = record.get(key)

        if value not in (None, ""):
            return value

    return None


def _decimal(value: Any) -> Decimal | None:
    try:
        if value in (None, ""):
            return None
        return Decimal(str(value))
    except Exception:
        return None


def _properties(record: dict[str, Any]) -> dict[str, Any]:
    properties = record.get("properties")

    if isinstance(properties, dict):
        return properties

    return {}


def _category(record: dict[str, Any]) -> str | None:
    properties = _properties(record)

    categories = properties.get("categories") or record.get("categories")

    if isinstance(categories, dict):
        primary = categories.get("primary")

        if primary:
            return str(primary)

        alternate = categories.get("alternate")

        if isinstance(alternate, list) and alternate:
            return str(alternate[0])

    if isinstance(categories, list) and categories:
        return str(categories[0])

    if isinstance(categories, str):
        return categories

    return (
        _first(properties, ("category", "primary_category", "type"))
        or _first(record, ("category", "primary_category", "type"))
    )


def load_overture_places(
    *,
    payload: dict,
    dataset: Any,
    source_file_id: str | None,
    load_date: date,
) -> int:
    records = (
        payload.get("records")
        or payload.get("places")
        or payload.get("features")
        or payload.get("data")
        or []
    )

    if isinstance(records, dict):
        records = records.get("records") or records.get("places") or records.get("features") or []

    params = []

    for record in records:
        if not isinstance(record, dict):
            continue

        geometry = record.get("geometry") if isinstance(record.get("geometry"), dict) else {}
        coordinates = geometry.get("coordinates") if isinstance(geometry.get("coordinates"), list) else []

        lng = _decimal(coordinates[0]) if len(coordinates) >= 1 else _decimal(_first(record, ("lng", "lon", "longitude")))
        lat = _decimal(coordinates[1]) if len(coordinates) >= 2 else _decimal(_first(record, ("lat", "latitude")))

        category = _category(record)

        params.append(
            {
                "area_slug": dataset.area_slug,
                "area_name": dataset.area_name,
                "place_id": str(_first(record, ("id", "place_id")) or ""),
                "name": (
                    _first(_properties(record).get("names", {}) or {}, ("primary", "common"))
                    or _first(record, ("name", "display_name"))
                ),
                "category": category,
                "primary_category": category,
                "latitude": lat,
                "longitude": lng,
                "source_payload": json.dumps(record, default=str),
                "source_file_id": source_file_id,
                "load_date": load_date,
            }
        )

    if not params:
        return 0

    with engine.begin() as connection:
        connection.execute(
            text("DELETE FROM raw.overture_places WHERE area_slug = :area_slug"),
            {"area_slug": dataset.area_slug},
        )

        connection.execute(INSERT_SQL, params)

    return len(params)
