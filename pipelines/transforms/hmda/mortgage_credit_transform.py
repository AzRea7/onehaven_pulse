from dataclasses import dataclass
from datetime import date
from decimal import Decimal, ROUND_HALF_UP

from sqlalchemy import text

from pipelines.common.db import engine
from pipelines.transforms.common.market_metric_loader import upsert_market_metrics
from pipelines.transforms.common.market_metric_record import MarketMetricRecord
from pipelines.transforms.common.transform_audit import finish_transform_run, start_transform_run


SOURCE = "hmda"
DATASET = "modified_lar"
TRANSFORM_NAME = "hmda_mortgage_credit_transform"
TARGET_TABLE = "analytics.market_monthly_metrics"


STATE_FIPS_BY_ABBR = {
    "AL": "01",
    "AK": "02",
    "AZ": "04",
    "AR": "05",
    "CA": "06",
    "CO": "08",
    "CT": "09",
    "DE": "10",
    "DC": "11",
    "FL": "12",
    "GA": "13",
    "HI": "15",
    "ID": "16",
    "IL": "17",
    "IN": "18",
    "IA": "19",
    "KS": "20",
    "KY": "21",
    "LA": "22",
    "ME": "23",
    "MD": "24",
    "MA": "25",
    "MI": "26",
    "MN": "27",
    "MS": "28",
    "MO": "29",
    "MT": "30",
    "NE": "31",
    "NV": "32",
    "NH": "33",
    "NJ": "34",
    "NM": "35",
    "NY": "36",
    "NC": "37",
    "ND": "38",
    "OH": "39",
    "OK": "40",
    "OR": "41",
    "PA": "42",
    "RI": "44",
    "SC": "45",
    "SD": "46",
    "TN": "47",
    "TX": "48",
    "UT": "49",
    "VT": "50",
    "VA": "51",
    "WA": "53",
    "WV": "54",
    "WI": "55",
    "WY": "56",
}


def _state_geo_id(state_code: str | None) -> str:
    if not state_code:
        return "us"

    value = state_code.strip().upper()

    if value.isdigit():
        return f"state:{value.zfill(2)}"

    fips = STATE_FIPS_BY_ABBR.get(value)

    if fips:
        return f"state:{fips}"

    return f"state:{value}"



@dataclass(frozen=True)
class HmdaAggregate:
    geo_id: str
    period_month: date
    applications: Decimal
    originations: Decimal
    denials: Decimal
    denial_rate: Decimal | None
    median_loan_amount: Decimal | None


def fetch_aggregates() -> list[HmdaAggregate]:
    sql = text(
        """
        WITH base AS (
            SELECT
                activity_year,
                CASE
                    WHEN county_code IS NOT NULL AND length(county_code) = 5
                        THEN 'county:' || county_code
                    WHEN state_code IS NOT NULL
                        THEN state_code
                    ELSE 'us'
                END AS geo_id,
                action_taken,
                loan_amount
            FROM raw.hmda_modified_lar
        ),
        agg AS (
            SELECT
                geo_id,
                activity_year,
                COUNT(*) AS applications,
                COUNT(*) FILTER (WHERE action_taken = '1') AS originations,
                COUNT(*) FILTER (WHERE action_taken = '3') AS denials,
                PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY loan_amount)
                    FILTER (WHERE loan_amount IS NOT NULL) AS median_loan_amount
            FROM base
            GROUP BY geo_id, activity_year
        )
        SELECT
            geo_id,
            activity_year,
            applications,
            originations,
            denials,
            median_loan_amount
        FROM agg
        ORDER BY geo_id, activity_year
        """
    )

    out: list[HmdaAggregate] = []

    with engine.begin() as connection:
        result = connection.execute(sql)

        for row in result.mappings():
            applications = Decimal(row["applications"])
            denials = Decimal(row["denials"])
            denial_rate = None

            if applications > 0:
                denial_rate = (denials / applications * Decimal("100")).quantize(
                    Decimal("0.000001"),
                    rounding=ROUND_HALF_UP,
                )

            out.append(
                HmdaAggregate(
                    geo_id=_state_geo_id(row["geo_id"])
                    if row["geo_id"] != "us" and not str(row["geo_id"]).startswith("county:")
                    else row["geo_id"],
                    period_month=date(int(row["activity_year"]), 12, 1),
                    applications=applications,
                    originations=Decimal(row["originations"]),
                    denials=denials,
                    denial_rate=denial_rate,
                    median_loan_amount=Decimal(str(row["median_loan_amount"]))
                    if row["median_loan_amount"] is not None
                    else None,
                )
            )

    return out



def ensure_hmda_geographies(aggregates: list[HmdaAggregate]) -> None:
    geo_ids = sorted(
        {
            aggregate.geo_id
            for aggregate in aggregates
            if aggregate.geo_id.startswith("county:") or aggregate.geo_id.startswith("state:")
        }
    )

    if not geo_ids:
        return

    sql = text(
        """
        INSERT INTO geo.dim_geo (
            geo_id,
            geo_type,
            name,
            display_name,
            country_code,
            is_active,
            created_at,
            updated_at
        )
        VALUES (
            :geo_id,
            :geo_type,
            :name,
            :display_name,
            'US',
            true,
            now(),
            now()
        )
        ON CONFLICT (geo_id)
        DO UPDATE SET
            name = EXCLUDED.name,
            display_name = EXCLUDED.display_name,
            updated_at = now()
        """
    )

    with engine.begin() as connection:
        for geo_id in geo_ids:
            geo_type, code = geo_id.split(":", maxsplit=1)

            if geo_type == "state":
                name = f"State {code}"
            else:
                name = f"County {code}"

            connection.execute(
                sql,
                {
                    "geo_id": geo_id,
                    "geo_type": geo_type,
                    "name": name,
                    "display_name": name,
                },
            )

def build_records(aggregates: list[HmdaAggregate], run_id: str) -> list[MarketMetricRecord]:
    records: list[MarketMetricRecord] = []

    for row in aggregates:
        values = {
            "hmda_applications": (row.applications, "count"),
            "hmda_originations": (row.originations, "count"),
            "hmda_denials": (row.denials, "count"),
            "hmda_denial_rate": (row.denial_rate, "percent"),
            "hmda_median_loan_amount": (row.median_loan_amount, "usd"),
        }

        for metric_name, (value, unit) in values.items():
            if value is None:
                continue

            records.append(
                MarketMetricRecord(
                    geo_id=row.geo_id,
                    period_month=row.period_month,
                    metric_name=metric_name,
                    metric_value=value,
                    metric_unit=unit,
                    source=SOURCE,
                    dataset=DATASET,
                    source_file_id=None,
                    pipeline_run_id=run_id,
                    source_value=value,
                    source_period=row.period_month,
                    period_grain="annual",
                    transformation_notes=f"Aggregated HMDA {metric_name}.",
                    source_flags={"derived_from": "raw.hmda_modified_lar"},
                    quality_flags={"hmda_modified_public_data": True},
                )
            )

    return records


def main() -> None:
    run_id = start_transform_run(
        transform_name=TRANSFORM_NAME,
        source=SOURCE,
        dataset=DATASET,
        target_table=TARGET_TABLE,
        metadata={"target_metrics": [
            "hmda_applications",
            "hmda_originations",
            "hmda_denials",
            "hmda_denial_rate",
            "hmda_median_loan_amount",
        ]},
    )

    try:
        aggregates = fetch_aggregates()
        ensure_hmda_geographies(aggregates)
        records = build_records(aggregates, run_id)
        loaded = upsert_market_metrics(records)

        finish_transform_run(
            run_id=run_id,
            status="success",
            records_extracted=len(aggregates),
            records_loaded=loaded,
            records_failed=0,
        )

        print(
            f"HMDA mortgage credit transform complete. "
            f"Aggregates: {len(aggregates)}. Loaded metrics: {loaded}. Run ID: {run_id}"
        )
    except Exception as exc:
        finish_transform_run(run_id=run_id, status="failed", error_message=str(exc))
        raise


if __name__ == "__main__":
    main()
