from __future__ import annotations

import json
import os
from datetime import date, datetime, timezone
from pathlib import Path

from sqlalchemy import create_engine, text


DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql+psycopg2://onehaven:onehaven_dev_password@localhost:5432/onehaven_market",
)

OUT_DIR = Path("data/diagnostics/data_quality")
OUT_DIR.mkdir(parents=True, exist_ok=True)


SQL = """
WITH latest AS (
    SELECT
        geo_id,
        MAX(period_month) AS latest_period
    FROM analytics.market_monthly_metrics
    GROUP BY geo_id
),
latest_rows AS (
    SELECT m.*
    FROM analytics.market_monthly_metrics m
    JOIN latest l
      ON l.geo_id = m.geo_id
     AND l.latest_period = m.period_month
),
quality AS (
    SELECT
        g.geo_id,
        g.geo_type,
        COALESCE(g.display_name, g.name) AS market_name,
        g.state_code,

        m.period_month AS latest_period,

        (m.zhvi_yoy IS NOT NULL OR m.home_price_index_yoy IS NOT NULL OR m.median_sale_price_yoy IS NOT NULL) AS has_price_growth,
        (m.zori_yoy IS NOT NULL OR m.median_rent_yoy IS NOT NULL) AS has_rent_growth,
        (m.active_listings IS NOT NULL OR m.months_supply IS NOT NULL OR m.median_days_on_market IS NOT NULL) AS has_inventory,
        (m.payment_to_income_ratio IS NOT NULL OR m.price_to_income_ratio IS NOT NULL OR m.estimated_monthly_payment IS NOT NULL) AS has_affordability,
        (m.unemployment_rate IS NOT NULL) AS has_labor,
        (m.building_permits IS NOT NULL OR m.permits_per_1000_people IS NOT NULL) AS has_permits,

        CASE
            WHEN m.payment_to_income_ratio IS NOT NULL
             AND (m.payment_to_income_ratio < 0 OR m.payment_to_income_ratio > 1.5)
            THEN true ELSE false
        END AS bad_payment_to_income,

        CASE
            WHEN m.unemployment_rate IS NOT NULL
             AND (m.unemployment_rate < 0 OR m.unemployment_rate > 40)
            THEN true ELSE false
        END AS bad_unemployment_rate,

        CASE
            WHEN m.zhvi IS NOT NULL AND m.zhvi <= 0 THEN true ELSE false
        END AS bad_zhvi,

        CASE
            WHEN m.zori IS NOT NULL AND m.zori <= 0 THEN true ELSE false
        END AS bad_zori,

        CASE
            WHEN m.median_days_on_market IS NOT NULL
             AND (m.median_days_on_market < 0 OR m.median_days_on_market > 730)
            THEN true ELSE false
        END AS bad_dom

    FROM geo.dim_geo g
    LEFT JOIN latest_rows m
      ON m.geo_id = g.geo_id
    WHERE g.is_active = true
),
scored AS (
    SELECT
        *,
        (
            CASE WHEN has_price_growth THEN 1 ELSE 0 END +
            CASE WHEN has_rent_growth THEN 1 ELSE 0 END +
            CASE WHEN has_inventory THEN 1 ELSE 0 END +
            CASE WHEN has_affordability THEN 1 ELSE 0 END +
            CASE WHEN has_labor THEN 1 ELSE 0 END +
            CASE WHEN has_permits THEN 1 ELSE 0 END
        ) AS available_core_categories,

        ROUND((
            (
                CASE WHEN has_price_growth THEN 1 ELSE 0 END +
                CASE WHEN has_rent_growth THEN 1 ELSE 0 END +
                CASE WHEN has_inventory THEN 1 ELSE 0 END +
                CASE WHEN has_affordability THEN 1 ELSE 0 END +
                CASE WHEN has_labor THEN 1 ELSE 0 END +
                CASE WHEN has_permits THEN 1 ELSE 0 END
            )::numeric / 6.0
        ), 4) AS raw_coverage_score,

        (
            bad_payment_to_income
            OR bad_unemployment_rate
            OR bad_zhvi
            OR bad_zori
            OR bad_dom
        ) AS has_bad_values
    FROM quality
)
SELECT
    *,
    ARRAY_REMOVE(ARRAY[
        CASE WHEN NOT has_price_growth THEN 'price_growth' END,
        CASE WHEN NOT has_rent_growth THEN 'rent_growth' END,
        CASE WHEN NOT has_inventory THEN 'inventory' END,
        CASE WHEN NOT has_affordability THEN 'affordability' END,
        CASE WHEN NOT has_labor THEN 'labor' END,
        CASE WHEN NOT has_permits THEN 'permits' END,
        CASE WHEN bad_payment_to_income THEN 'bad_payment_to_income' END,
        CASE WHEN bad_unemployment_rate THEN 'bad_unemployment_rate' END,
        CASE WHEN bad_zhvi THEN 'bad_zhvi' END,
        CASE WHEN bad_zori THEN 'bad_zori' END,
        CASE WHEN bad_dom THEN 'bad_dom' END
    ], NULL) AS data_quality_issues
FROM scored
ORDER BY raw_coverage_score ASC, has_bad_values DESC, geo_type, market_name;
"""


def row_to_dict(row):
    output = dict(row)
    for key, value in list(output.items()):
        if isinstance(value, (date, datetime)):
            output[key] = value.isoformat()
        elif hasattr(value, "__float__"):
            try:
                output[key] = float(value)
            except Exception:
                pass
    return output


def main() -> int:
    engine = create_engine(DATABASE_URL)

    with engine.connect() as connection:
        rows = [row_to_dict(row) for row in connection.execute(text(SQL)).mappings().all()]

    total = len(rows)
    perfect = [row for row in rows if row["raw_coverage_score"] == 1.0 and not row["has_bad_values"]]
    bad_values = [row for row in rows if row["has_bad_values"]]
    incomplete = [row for row in rows if row["raw_coverage_score"] < 1.0]

    summary = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "total_markets": total,
        "perfect_quality_markets": len(perfect),
        "incomplete_markets": len(incomplete),
        "bad_value_markets": len(bad_values),
        "perfect_quality_pct": round(len(perfect) / total, 4) if total else 0,
        "top_issues": {},
    }

    issue_counts: dict[str, int] = {}
    for row in rows:
        for issue in row["data_quality_issues"] or []:
            issue_counts[issue] = issue_counts.get(issue, 0) + 1

    summary["top_issues"] = dict(sorted(issue_counts.items(), key=lambda item: item[1], reverse=True))

    report = {
        "summary": summary,
        "rows": rows,
    }

    out_path = OUT_DIR / "market_data_quality_latest.json"
    out_path.write_text(json.dumps(report, indent=2), encoding="utf-8")

    csv_path = OUT_DIR / "market_data_quality_latest.csv"
    if rows:
        import csv

        with csv_path.open("w", encoding="utf-8", newline="") as file:
            writer = csv.DictWriter(file, fieldnames=list(rows[0].keys()))
            writer.writeheader()
            writer.writerows(rows)

    print(json.dumps(summary, indent=2))
    print(f"Report written to: {out_path}")
    print(f"CSV written to: {csv_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
