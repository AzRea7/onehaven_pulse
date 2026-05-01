import json
from datetime import date
from decimal import Decimal, InvalidOperation
from io import BytesIO
from typing import Any

import pandas as pd
from sqlalchemy import text

from pipelines.common.db import engine
from pipelines.extractors.census_building_permits.config import CensusBpsDataset


UPSERT_CENSUS_BPS_SQL = text(
    """
    INSERT INTO raw.census_building_permits (
        geography_level,
        period_type,
        source_period_label,
        source_geo_id,
        source_name,
        state_fips,
        county_fips,
        cbsa_code,
        period_month,
        building_permits,
        single_family_permits,
        multi_family_permits,
        permit_units,
        source_payload,
        source_file_id,
        load_date
    )
    VALUES (
        :geography_level,
        :period_type,
        :source_period_label,
        :source_geo_id,
        :source_name,
        :state_fips,
        :county_fips,
        :cbsa_code,
        :period_month,
        :building_permits,
        :single_family_permits,
        :multi_family_permits,
        :permit_units,
        CAST(:source_payload AS JSONB),
        :source_file_id,
        :load_date
    )
    ON CONFLICT (
        geography_level,
        period_type,
        source_geo_id,
        period_month,
        load_date
    )
    DO UPDATE SET
        source_period_label = EXCLUDED.source_period_label,
        source_name = EXCLUDED.source_name,
        state_fips = EXCLUDED.state_fips,
        county_fips = EXCLUDED.county_fips,
        cbsa_code = EXCLUDED.cbsa_code,
        building_permits = EXCLUDED.building_permits,
        single_family_permits = EXCLUDED.single_family_permits,
        multi_family_permits = EXCLUDED.multi_family_permits,
        permit_units = EXCLUDED.permit_units,
        source_payload = EXCLUDED.source_payload,
        source_file_id = EXCLUDED.source_file_id
    """
)


STATE_FIPS_BY_NAME = {
    "alabama": "01",
    "alaska": "02",
    "arizona": "04",
    "arkansas": "05",
    "california": "06",
    "colorado": "08",
    "connecticut": "09",
    "delaware": "10",
    "district of columbia": "11",
    "florida": "12",
    "georgia": "13",
    "hawaii": "15",
    "idaho": "16",
    "illinois": "17",
    "indiana": "18",
    "iowa": "19",
    "kansas": "20",
    "kentucky": "21",
    "louisiana": "22",
    "maine": "23",
    "maryland": "24",
    "massachusetts": "25",
    "michigan": "26",
    "minnesota": "27",
    "mississippi": "28",
    "missouri": "29",
    "montana": "30",
    "nebraska": "31",
    "nevada": "32",
    "new hampshire": "33",
    "new jersey": "34",
    "new mexico": "35",
    "new york": "36",
    "north carolina": "37",
    "north dakota": "38",
    "ohio": "39",
    "oklahoma": "40",
    "oregon": "41",
    "pennsylvania": "42",
    "rhode island": "44",
    "south carolina": "45",
    "south dakota": "46",
    "tennessee": "47",
    "texas": "48",
    "utah": "49",
    "vermont": "50",
    "virginia": "51",
    "washington": "53",
    "west virginia": "54",
    "wisconsin": "55",
    "wyoming": "56",
}


REGION_OR_DIVISION_NAMES = {
    "northeast region",
    "midwest region",
    "south region",
    "west region",
    "new england division",
    "middle atlantic division",
    "east north central division",
    "west north central division",
    "south atlantic division",
    "east south central division",
    "west south central division",
    "mountain division",
    "pacific division",
}


def _parse_decimal(value: Any) -> Decimal | None:
    if value is None:
        return None

    raw = str(value).strip().replace(",", "")

    if not raw or raw.lower() in {"nan", "none", "null"} or raw in {"-", "."}:
        return None

    try:
        return Decimal(raw)
    except InvalidOperation:
        return None


def _period_month(dataset: CensusBpsDataset) -> date:
    label = dataset.source_period_label.strip()

    if dataset.period_type == "annual":
        return date(int(label), 12, 1)

    if "-" in label:
        year_raw, month_raw = label.split("-", maxsplit=1)
        return date(int(year_raw), int(month_raw), 1)

    raise ValueError(f"Unsupported Census BPS source_period_label: {dataset.source_period_label}")


def _read_excel_raw(content: bytes) -> pd.DataFrame:
    dataframe = pd.read_excel(BytesIO(content), header=None, dtype=str)
    return dataframe.dropna(how="all")


def _clean_name(value: Any) -> str | None:
    if value is None:
        return None

    name = " ".join(str(value).strip().split())

    if not name or name.lower() == "nan":
        return None

    return name


def _row_payload(row: pd.Series) -> str:
    return json.dumps(
        {str(index): None if pd.isna(value) else str(value) for index, value in row.items()},
        default=str,
    )


def _permit_values_from_columns(
    row: pd.Series,
    *,
    total_col: int,
    one_unit_col: int,
    two_unit_col: int,
    three_four_col: int,
    five_plus_col: int,
) -> tuple[Decimal | None, Decimal | None, Decimal | None, Decimal | None]:
    total = _parse_decimal(row.get(total_col))
    one_unit = _parse_decimal(row.get(one_unit_col))
    two_unit = _parse_decimal(row.get(two_unit_col)) or Decimal("0")
    three_four = _parse_decimal(row.get(three_four_col)) or Decimal("0")
    five_plus = _parse_decimal(row.get(five_plus_col)) or Decimal("0")

    multi_family = two_unit + three_four + five_plus
    permit_units = total

    return total, one_unit, multi_family, permit_units


def _parse_state_rows(
    *,
    dataframe: pd.DataFrame,
    dataset: CensusBpsDataset,
    source_file_id: str | None,
    load_date: date,
) -> list[dict]:
    rows: list[dict] = []
    period_month = _period_month(dataset)

    # State annual:
    #   name=0, total=1, 1-unit=2, 2-unit=3, 3-4=4, 5+=5
    # State monthly:
    #   name=0, current month total=1, 1-unit=2, 2-unit=3, 3-4=4, 5+=5
    total_col = 1
    one_unit_col = 2
    two_unit_col = 3
    three_four_col = 4
    five_plus_col = 5

    for _, row in dataframe.iterrows():
        source_name = _clean_name(row.get(0))

        if not source_name:
            continue

        lower_name = source_name.lower()

        if lower_name in REGION_OR_DIVISION_NAMES:
            continue

        if lower_name == "united states":
            source_geo_id = "us"
            geography_level = "national"
            state_fips = None
        else:
            state_fips = STATE_FIPS_BY_NAME.get(lower_name)

            if not state_fips:
                continue

            geography_level = "state"
            source_geo_id = f"state:{state_fips}"

        building_permits, single_family, multi_family, permit_units = _permit_values_from_columns(
            row,
            total_col=total_col,
            one_unit_col=one_unit_col,
            two_unit_col=two_unit_col,
            three_four_col=three_four_col,
            five_plus_col=five_plus_col,
        )

        if building_permits is None and permit_units is None:
            continue

        rows.append(
            {
                "geography_level": geography_level,
                "period_type": dataset.period_type,
                "source_period_label": dataset.source_period_label,
                "source_geo_id": source_geo_id,
                "source_name": source_name,
                "state_fips": state_fips,
                "county_fips": None,
                "cbsa_code": None,
                "period_month": period_month,
                "building_permits": building_permits,
                "single_family_permits": single_family,
                "multi_family_permits": multi_family,
                "permit_units": permit_units,
                "source_payload": _row_payload(row),
                "source_file_id": source_file_id,
                "load_date": load_date,
            }
        )

    return rows


def _parse_cbsa_rows(
    *,
    dataframe: pd.DataFrame,
    dataset: CensusBpsDataset,
    source_file_id: str | None,
    load_date: date,
) -> list[dict]:
    rows: list[dict] = []
    period_month = _period_month(dataset)

    # CBSA annual:
    #   csa=0, cbsa=1, name=2, metro/micro=3, total=4, 1-unit=5, 2-unit=6, 3-4=7, 5+=8
    # CBSA monthly:
    #   current month uses the same first block:
    #   csa=0, cbsa=1, name=2, metro/micro=3, total=4, 1-unit=5, 2-unit=6, 3-4=7, 5+=8
    total_col = 4
    one_unit_col = 5
    two_unit_col = 6
    three_four_col = 7
    five_plus_col = 8

    for _, row in dataframe.iterrows():
        cbsa_code = _clean_name(row.get(1))
        source_name = _clean_name(row.get(2))

        if not cbsa_code or not source_name:
            continue

        if not cbsa_code.isdigit():
            continue

        building_permits, single_family, multi_family, permit_units = _permit_values_from_columns(
            row,
            total_col=total_col,
            one_unit_col=one_unit_col,
            two_unit_col=two_unit_col,
            three_four_col=three_four_col,
            five_plus_col=five_plus_col,
        )

        if building_permits is None and permit_units is None:
            continue

        rows.append(
            {
                "geography_level": "cbsa",
                "period_type": dataset.period_type,
                "source_period_label": dataset.source_period_label,
                "source_geo_id": f"cbsa:{cbsa_code}",
                "source_name": source_name,
                "state_fips": None,
                "county_fips": None,
                "cbsa_code": cbsa_code,
                "period_month": period_month,
                "building_permits": building_permits,
                "single_family_permits": single_family,
                "multi_family_permits": multi_family,
                "permit_units": permit_units,
                "source_payload": _row_payload(row),
                "source_file_id": source_file_id,
                "load_date": load_date,
            }
        )

    return rows


def parse_census_bps_content(
    *,
    content: bytes,
    dataset: CensusBpsDataset,
    source_file_id: str | None,
    load_date: date,
) -> list[dict]:
    dataframe = _read_excel_raw(content)

    if dataset.geography_level == "state":
        return _parse_state_rows(
            dataframe=dataframe,
            dataset=dataset,
            source_file_id=source_file_id,
            load_date=load_date,
        )

    if dataset.geography_level in {"cbsa", "metro"}:
        return _parse_cbsa_rows(
            dataframe=dataframe,
            dataset=dataset,
            source_file_id=source_file_id,
            load_date=load_date,
        )

    raise ValueError(f"Unsupported Census BPS geography_level: {dataset.geography_level}")


def load_census_bps_permits(
    *,
    content: bytes,
    dataset: CensusBpsDataset,
    source_file_id: str | None,
    load_date: date,
) -> int:
    params = parse_census_bps_content(
        content=content,
        dataset=dataset,
        source_file_id=source_file_id,
        load_date=load_date,
    )

    if not params:
        return 0

    with engine.begin() as connection:
        connection.execute(UPSERT_CENSUS_BPS_SQL, params)

    return len(params)
