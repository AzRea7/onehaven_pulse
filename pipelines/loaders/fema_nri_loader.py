import json
from datetime import date
from decimal import Decimal, InvalidOperation
from typing import Any

from sqlalchemy import text

from pipelines.common.db import engine
from pipelines.extractors.fema_nri.config import FemaNriDataset


UPSERT_FEMA_NRI_COUNTY_RISK_SQL = text(
    """
    INSERT INTO raw.fema_nri_county_risk (
        county_fips,
        county_name,
        state_name,
        state_code,
        source_year,
        release_label,
        risk_score,
        risk_rating,
        expected_annual_loss,
        expected_annual_loss_score,
        expected_annual_loss_rating,
        social_vulnerability_score,
        social_vulnerability_rating,
        community_resilience_score,
        community_resilience_rating,
        source_payload,
        source_file_id,
        load_date
    )
    VALUES (
        :county_fips,
        :county_name,
        :state_name,
        :state_code,
        :source_year,
        :release_label,
        :risk_score,
        :risk_rating,
        :expected_annual_loss,
        :expected_annual_loss_score,
        :expected_annual_loss_rating,
        :social_vulnerability_score,
        :social_vulnerability_rating,
        :community_resilience_score,
        :community_resilience_rating,
        CAST(:source_payload AS JSONB),
        :source_file_id,
        :load_date
    )
    ON CONFLICT (
        county_fips,
        release_label,
        load_date
    )
    DO UPDATE SET
        county_name = EXCLUDED.county_name,
        state_name = EXCLUDED.state_name,
        state_code = EXCLUDED.state_code,
        source_year = EXCLUDED.source_year,
        risk_score = EXCLUDED.risk_score,
        risk_rating = EXCLUDED.risk_rating,
        expected_annual_loss = EXCLUDED.expected_annual_loss,
        expected_annual_loss_score = EXCLUDED.expected_annual_loss_score,
        expected_annual_loss_rating = EXCLUDED.expected_annual_loss_rating,
        social_vulnerability_score = EXCLUDED.social_vulnerability_score,
        social_vulnerability_rating = EXCLUDED.social_vulnerability_rating,
        community_resilience_score = EXCLUDED.community_resilience_score,
        community_resilience_rating = EXCLUDED.community_resilience_rating,
        source_payload = EXCLUDED.source_payload,
        source_file_id = EXCLUDED.source_file_id
    """
)


def _first_value(record: dict[str, Any], keys: tuple[str, ...]) -> Any:
    for key in keys:
        if key in record and record[key] not in (None, ""):
            return record[key]

    lower_record = {str(k).lower(): v for k, v in record.items()}

    for key in keys:
        value = lower_record.get(key.lower())

        if value not in (None, ""):
            return value

    return None


def _parse_decimal(value: Any) -> Decimal | None:
    if value is None:
        return None

    raw = str(value).strip().replace(",", "").replace("$", "")

    if not raw or raw.lower() in {"nan", "none", "null", "n/a"} or raw in {"-", "."}:
        return None

    try:
        return Decimal(raw)
    except InvalidOperation:
        return None


def _parse_int(value: Any) -> int | None:
    parsed = _parse_decimal(value)

    if parsed is None:
        return None

    return int(parsed)


def _state_code_from_county_fips(county_fips: str) -> str | None:
    if len(county_fips) >= 2 and county_fips[:2].isdigit():
        return county_fips[:2]

    return None


def _record_to_params(
    *,
    record: dict[str, Any],
    dataset: FemaNriDataset,
    source_file_id: str | None,
    load_date: date,
) -> dict | None:
    county_fips = _first_value(
        record,
        (
            "COUNTYFIPS",
            "STCOFIPS",
            "GEOID",
            "GEOID10",
            "FIPS",
            "county_fips",
        ),
    )

    if county_fips is None:
        return None

    county_fips = str(county_fips).strip().zfill(5)

    risk_score = _parse_decimal(
        _first_value(record, ("RISK_SCORE", "RISK_SCORE_ALL", "risk_score"))
    )

    expected_annual_loss = _parse_decimal(
        _first_value(
            record,
            (
                "EAL_VALT",
                "EAL_VAL",
                "EAL_VALUE",
                "EXPECTED_ANNUAL_LOSS",
                "expected_annual_loss",
            ),
        )
    )

    expected_annual_loss_score = _parse_decimal(
        _first_value(record, ("EAL_SCORE", "EAL_SCORE_ALL", "expected_annual_loss_score"))
    )

    social_vulnerability_score = _parse_decimal(
        _first_value(record, ("SOVI_SCORE", "SOVI_SCORE_ALL", "social_vulnerability_score"))
    )

    community_resilience_score = _parse_decimal(
        _first_value(record, ("RESL_SCORE", "RESL_SCORE_ALL", "community_resilience_score"))
    )

    return {
        "county_fips": county_fips,
        "county_name": _first_value(record, ("COUNTY", "COUNTYNAME", "county_name")),
        "state_name": _first_value(record, ("STATE", "STATE_NAME", "state_name")),
        "state_code": _first_value(record, ("STATEABBRV", "STATE_ABBR", "state_code"))
        or _state_code_from_county_fips(county_fips),
        "source_year": _parse_int(_first_value(record, ("YEAR", "NRI_VER", "source_year"))),
        "release_label": dataset.release_label,
        "risk_score": risk_score,
        "risk_rating": _first_value(record, ("RISK_RATNG", "RISK_RATING", "risk_rating")),
        "expected_annual_loss": expected_annual_loss,
        "expected_annual_loss_score": expected_annual_loss_score,
        "expected_annual_loss_rating": _first_value(
            record,
            ("EAL_RATNG", "EAL_RATING", "expected_annual_loss_rating"),
        ),
        "social_vulnerability_score": social_vulnerability_score,
        "social_vulnerability_rating": _first_value(
            record,
            ("SOVI_RATNG", "SOVI_RATING", "social_vulnerability_rating"),
        ),
        "community_resilience_score": community_resilience_score,
        "community_resilience_rating": _first_value(
            record,
            ("RESL_RATNG", "RESL_RATING", "community_resilience_rating"),
        ),
        "source_payload": json.dumps(record, default=str),
        "source_file_id": source_file_id,
        "load_date": load_date,
    }


def load_fema_nri_county_risk(
    *,
    payload: dict,
    dataset: FemaNriDataset,
    source_file_id: str | None,
    load_date: date,
) -> int:
    records = payload.get("records", [])

    params = [
        parsed
        for record in records
        if isinstance(record, dict)
        if (
            parsed := _record_to_params(
                record=record,
                dataset=dataset,
                source_file_id=source_file_id,
                load_date=load_date,
            )
        )
        is not None
    ]

    if not params:
        return 0

    with engine.begin() as connection:
        connection.execute(UPSERT_FEMA_NRI_COUNTY_RISK_SQL, params)

    return len(params)
