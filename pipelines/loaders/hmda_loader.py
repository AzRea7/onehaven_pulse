import csv
import json
from datetime import date
from decimal import Decimal, InvalidOperation
from io import StringIO
from typing import Any

from sqlalchemy import text

from pipelines.common.db import engine


INSERT_SQL = text(
    """
    INSERT INTO raw.hmda_modified_lar (
        activity_year,
        state_code,
        county_code,
        census_tract,
        lei,
        action_taken,
        loan_purpose,
        loan_type,
        lien_status,
        loan_amount,
        income,
        source_payload,
        source_file_id,
        load_date
    )
    VALUES (
        :activity_year,
        :state_code,
        :county_code,
        :census_tract,
        :lei,
        :action_taken,
        :loan_purpose,
        :loan_type,
        :lien_status,
        :loan_amount,
        :income,
        CAST(:source_payload AS JSONB),
        :source_file_id,
        :load_date
    )
    """
)


def _decode(content: bytes) -> str:
    try:
        return content.decode("utf-8-sig")
    except UnicodeDecodeError:
        return content.decode("latin-1")


def _first(row: dict[str, Any], keys: tuple[str, ...]) -> Any:
    lower = {str(k).lower(): v for k, v in row.items()}

    for key in keys:
        value = row.get(key)

        if value not in (None, ""):
            return value

        value = lower.get(key.lower())

        if value not in (None, ""):
            return value

    return None


def _decimal(value: Any) -> Decimal | None:
    if value is None:
        return None

    raw = str(value).strip().replace(",", "")

    if not raw or raw.upper() in {"NA", "EXEMPT", "N/A"}:
        return None

    try:
        return Decimal(raw)
    except InvalidOperation:
        return None


def _int(value: Any) -> int | None:
    parsed = _decimal(value)
    return int(parsed) if parsed is not None else None


def parse_hmda_modified_lar_csv(
    *,
    content: bytes,
    source_file_id: str | None,
    load_date: date,
) -> list[dict]:
    text_value = _decode(content)
    reader = csv.DictReader(StringIO(text_value))

    params: list[dict] = []

    for row in reader:
        year = _int(_first(row, ("activity_year", "year")))

        if year is None:
            continue

        params.append(
            {
                "activity_year": year,
                "state_code": _first(row, ("state_code", "state")),
                "county_code": _first(row, ("county_code", "county")),
                "census_tract": _first(row, ("census_tract", "tract")),
                "lei": _first(row, ("lei",)),
                "action_taken": str(_first(row, ("action_taken",)) or "").strip() or None,
                "loan_purpose": str(_first(row, ("loan_purpose",)) or "").strip() or None,
                "loan_type": str(_first(row, ("loan_type",)) or "").strip() or None,
                "lien_status": str(_first(row, ("lien_status",)) or "").strip() or None,
                "loan_amount": _decimal(_first(row, ("loan_amount",))),
                "income": _decimal(_first(row, ("income",))),
                "source_payload": json.dumps(row, default=str),
                "source_file_id": source_file_id,
                "load_date": load_date,
            }
        )

    return params


def load_hmda_modified_lar(
    *,
    content: bytes,
    source_file_id: str | None,
    load_date: date,
) -> int:
    params = parse_hmda_modified_lar_csv(
        content=content,
        source_file_id=source_file_id,
        load_date=load_date,
    )

    if not params:
        return 0

    with engine.begin() as connection:
        if source_file_id:
            connection.execute(
                text("DELETE FROM raw.hmda_modified_lar WHERE source_file_id = :source_file_id"),
                {"source_file_id": source_file_id},
            )

        connection.execute(INSERT_SQL, params)

    return len(params)
