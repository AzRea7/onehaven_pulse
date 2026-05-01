from datetime import date

from sqlalchemy import text

from pipelines.common.db import engine


UPSERT_FRED_OBSERVATION_SQL = text(
    """
    INSERT INTO raw.fred_observations (
        series_id,
        observation_date,
        value,
        realtime_start,
        realtime_end,
        load_date,
        source_file_id
    )
    VALUES (
        :series_id,
        :observation_date,
        :value,
        :realtime_start,
        :realtime_end,
        :load_date,
        :source_file_id
    )
    ON CONFLICT (
        series_id,
        observation_date,
        load_date
    )
    DO UPDATE SET
        value = EXCLUDED.value,
        realtime_start = EXCLUDED.realtime_start,
        realtime_end = EXCLUDED.realtime_end,
        source_file_id = EXCLUDED.source_file_id
    """
)


def _parse_date(value: str | None) -> date | None:
    if not value:
        return None

    try:
        return date.fromisoformat(value)
    except ValueError:
        return None


def _observation_to_params(
    *,
    series_id: str,
    observation: dict,
    load_date: date,
    source_file_id: str | None,
) -> dict | None:
    observation_date = _parse_date(observation.get("date"))

    if observation_date is None:
        return None

    return {
        "series_id": series_id,
        "observation_date": observation_date,
        "value": observation.get("value"),
        "realtime_start": _parse_date(observation.get("realtime_start")),
        "realtime_end": _parse_date(observation.get("realtime_end")),
        "load_date": load_date,
        "source_file_id": source_file_id,
    }


def load_fred_observations(
    *,
    series_id: str,
    observations: list[dict],
    load_date: date,
    source_file_id: str | None,
) -> int:
    params = [
        parsed
        for observation in observations
        if (
            parsed := _observation_to_params(
                series_id=series_id,
                observation=observation,
                load_date=load_date,
                source_file_id=source_file_id,
            )
        )
        is not None
    ]

    if not params:
        return 0

    with engine.begin() as connection:
        connection.execute(UPSERT_FRED_OBSERVATION_SQL, params)

    return len(params)
