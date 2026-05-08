from __future__ import annotations

import csv
import json
import os
from datetime import date, datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any

from sqlalchemy import create_engine, text


DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql+psycopg2://onehaven:onehaven_dev_password@127.0.0.1:5432/onehaven_market",
)

OUT_DIR = Path("data/diagnostics/michigan")
OUT_DIR.mkdir(parents=True, exist_ok=True)


SQL = """
WITH mi_geos AS (
    SELECT
        g.geo_id,
        g.geo_type,
        COALESCE(g.display_name, g.name) AS market_name,
        g.state_code,
        g.state_name
    FROM geo.dim_geo g
    WHERE g.is_active = true
      AND (
            upper(COALESCE(g.state_code, '')) = 'MI'
            OR upper(COALESCE(g.name, '')) ~ '(^|[^A-Z])MI($|[^A-Z])'
            OR upper(COALESCE(g.display_name, '')) ~ '(^|[^A-Z])MI($|[^A-Z])'
      )
),
latest AS (
    SELECT
        m.geo_id,
        MAX(m.period_month) AS latest_period
    FROM analytics.market_monthly_metrics m
    JOIN mi_geos g
      ON g.geo_id = m.geo_id
    GROUP BY m.geo_id
),
latest_rows AS (
    SELECT m.*
    FROM analytics.market_monthly_metrics m
    JOIN latest l
      ON l.geo_id = m.geo_id
     AND l.latest_period = m.period_month
),
metric_family_latest AS (
    SELECT
        g.geo_id,

        MAX(m.period_month) FILTER (
            WHERE m.zhvi_yoy IS NOT NULL
               OR m.home_price_index_yoy IS NOT NULL
               OR m.median_sale_price_yoy IS NOT NULL
        ) AS price_latest_period,

        MAX(m.period_month) FILTER (
            WHERE m.zori_yoy IS NOT NULL
               OR m.median_rent_yoy IS NOT NULL
        ) AS rent_latest_period,

        MAX(m.period_month) FILTER (
            WHERE m.active_listings_yoy IS NOT NULL
               OR m.months_supply IS NOT NULL
               OR m.median_days_on_market IS NOT NULL
        ) AS inventory_latest_period,

        MAX(m.period_month) FILTER (
            WHERE m.payment_to_income_ratio IS NOT NULL
               OR m.price_to_income_ratio IS NOT NULL
               OR m.estimated_monthly_payment IS NOT NULL
        ) AS affordability_latest_period,

        MAX(m.period_month) FILTER (
            WHERE m.unemployment_rate IS NOT NULL
        ) AS labor_latest_period,

        MAX(m.period_month) FILTER (
            WHERE m.building_permits IS NOT NULL
               OR m.permits_per_1000_people IS NOT NULL
        ) AS permits_latest_period

    FROM mi_geos g
    LEFT JOIN analytics.market_monthly_metrics m
      ON m.geo_id = g.geo_id
    GROUP BY g.geo_id
),
surface_checks AS (
    SELECT
        g.geo_id,
        EXISTS (
            SELECT 1
            FROM geo.geo_geometry gg
            WHERE gg.geo_id = g.geo_id
              AND COALESCE(gg.simplified_geometry, gg.geometry) IS NOT NULL
        ) AS has_geometry
    FROM mi_geos g
),
base AS (
    SELECT
        g.geo_id,
        g.geo_type,
        g.market_name,
        g.state_code,
        g.state_name,

        lr.period_month AS latest_period,

        fl.price_latest_period,
        fl.rent_latest_period,
        fl.inventory_latest_period,
        fl.affordability_latest_period,
        fl.labor_latest_period,
        fl.permits_latest_period,

        (fl.price_latest_period IS NOT NULL) AS has_price,
        (fl.rent_latest_period IS NOT NULL) AS has_rent,
        (fl.inventory_latest_period IS NOT NULL) AS has_inventory,
        (fl.affordability_latest_period IS NOT NULL) AS has_affordability,
        (fl.labor_latest_period IS NOT NULL) AS has_labor,
        (fl.permits_latest_period IS NOT NULL) AS has_permits,

        CASE
            WHEN fl.price_latest_period IS NULL THEN false
            WHEN CURRENT_DATE - fl.price_latest_period <= 90 THEN true
            ELSE false
        END AS price_fresh,

        CASE
            WHEN fl.rent_latest_period IS NULL THEN false
            WHEN CURRENT_DATE - fl.rent_latest_period <= 90 THEN true
            ELSE false
        END AS rent_fresh,

        CASE
            WHEN fl.inventory_latest_period IS NULL THEN false
            WHEN CURRENT_DATE - fl.inventory_latest_period <= 90 THEN true
            ELSE false
        END AS inventory_fresh,

        CASE
            WHEN fl.affordability_latest_period IS NULL THEN false
            WHEN CURRENT_DATE - fl.affordability_latest_period <= 90 THEN true
            ELSE false
        END AS affordability_fresh,

        CASE
            WHEN fl.labor_latest_period IS NULL THEN false
            WHEN CURRENT_DATE - fl.labor_latest_period <= 90 THEN true
            ELSE false
        END AS labor_fresh,

        CASE
            WHEN fl.permits_latest_period IS NULL THEN false
            WHEN CURRENT_DATE - fl.permits_latest_period <= 120 THEN true
            ELSE false
        END AS permits_fresh,

        CASE
            WHEN lr.payment_to_income_ratio IS NOT NULL
             AND (lr.payment_to_income_ratio < 0 OR lr.payment_to_income_ratio > 1.5)
            THEN true ELSE false
        END AS bad_payment_to_income,

        CASE
            WHEN lr.unemployment_rate IS NOT NULL
             AND (lr.unemployment_rate < 0 OR lr.unemployment_rate > 40)
            THEN true ELSE false
        END AS bad_unemployment_rate,

        CASE WHEN lr.zhvi IS NOT NULL AND lr.zhvi <= 0 THEN true ELSE false END AS bad_zhvi,
        CASE WHEN lr.zori IS NOT NULL AND lr.zori <= 0 THEN true ELSE false END AS bad_zori,

        CASE
            WHEN lr.median_days_on_market IS NOT NULL
             AND (lr.median_days_on_market < 0 OR lr.median_days_on_market > 730)
            THEN true ELSE false
        END AS bad_dom,

        sc.has_geometry

    FROM mi_geos g
    LEFT JOIN latest_rows lr
      ON lr.geo_id = g.geo_id
    LEFT JOIN metric_family_latest fl
      ON fl.geo_id = g.geo_id
    LEFT JOIN surface_checks sc
      ON sc.geo_id = g.geo_id
),
scored AS (
    SELECT
        *,

        (
            CASE WHEN has_price THEN 1 ELSE 0 END +
            CASE WHEN has_rent THEN 1 ELSE 0 END +
            CASE WHEN has_inventory THEN 1 ELSE 0 END +
            CASE WHEN has_affordability THEN 1 ELSE 0 END +
            CASE WHEN has_labor THEN 1 ELSE 0 END +
            CASE WHEN has_permits THEN 1 ELSE 0 END
        )::numeric / 6.0 AS coverage_score,

        (
            CASE WHEN price_fresh THEN 1 ELSE 0 END +
            CASE WHEN rent_fresh THEN 1 ELSE 0 END +
            CASE WHEN inventory_fresh THEN 1 ELSE 0 END +
            CASE WHEN affordability_fresh THEN 1 ELSE 0 END +
            CASE WHEN labor_fresh THEN 1 ELSE 0 END +
            CASE WHEN permits_fresh THEN 1 ELSE 0 END
        )::numeric / 6.0 AS freshness_score,

        CASE
            WHEN bad_payment_to_income
              OR bad_unemployment_rate
              OR bad_zhvi
              OR bad_zori
              OR bad_dom
            THEN 0.0::numeric
            ELSE 1.0::numeric
        END AS validity_score,

        CASE WHEN has_geometry THEN 1.0::numeric ELSE 0.0::numeric END AS geometry_score,

        CASE
            WHEN geo_type = 'metro' AND has_geometry THEN 1.0::numeric
            WHEN geo_type <> 'metro' THEN 0.75::numeric
            ELSE 0.0::numeric
        END AS surface_score,

        ARRAY_REMOVE(ARRAY[
            CASE WHEN NOT has_price THEN 'missing_price' END,
            CASE WHEN NOT has_rent THEN 'missing_rent' END,
            CASE WHEN NOT has_inventory THEN 'missing_inventory' END,
            CASE WHEN NOT has_affordability THEN 'missing_affordability' END,
            CASE WHEN NOT has_labor THEN 'missing_labor' END,
            CASE WHEN NOT has_permits THEN 'missing_permits' END,

            CASE WHEN has_price AND NOT price_fresh THEN 'stale_price' END,
            CASE WHEN has_rent AND NOT rent_fresh THEN 'stale_rent' END,
            CASE WHEN has_inventory AND NOT inventory_fresh THEN 'stale_inventory' END,
            CASE WHEN has_affordability AND NOT affordability_fresh THEN 'stale_affordability' END,
            CASE WHEN has_labor AND NOT labor_fresh THEN 'stale_labor' END,
            CASE WHEN has_permits AND NOT permits_fresh THEN 'stale_permits' END,

            CASE WHEN bad_payment_to_income THEN 'bad_payment_to_income' END,
            CASE WHEN bad_unemployment_rate THEN 'bad_unemployment_rate' END,
            CASE WHEN bad_zhvi THEN 'bad_zhvi' END,
            CASE WHEN bad_zori THEN 'bad_zori' END,
            CASE WHEN bad_dom THEN 'bad_days_on_market' END,

            CASE WHEN NOT has_geometry THEN 'missing_map_geometry' END
        ], NULL) AS reasons
    FROM base
),
final AS (
    SELECT
        *,
        ROUND(
            (
                coverage_score * 0.45 +
                freshness_score * 0.25 +
                validity_score * 0.15 +
                geometry_score * 0.10 +
                surface_score * 0.05
            ),
            4
        ) AS overall_status_score
    FROM scored
)
SELECT
    geo_id,
    geo_type,
    market_name,
    state_code,
    latest_period,

    ROUND(coverage_score, 4) AS coverage_score,
    ROUND(freshness_score, 4) AS freshness_score,
    ROUND(validity_score, 4) AS validity_score,
    ROUND(geometry_score, 4) AS geometry_score,
    ROUND(surface_score, 4) AS surface_score,
    overall_status_score,

    CASE
        WHEN overall_status_score >= 0.95 THEN 'ready'
        WHEN overall_status_score >= 0.80 THEN 'usable_with_caveats'
        WHEN overall_status_score >= 0.60 THEN 'limited'
        ELSE 'not_reliable'
    END AS status_label,

    has_price,
    has_rent,
    has_inventory,
    has_affordability,
    has_labor,
    has_permits,

    price_latest_period,
    rent_latest_period,
    inventory_latest_period,
    affordability_latest_period,
    labor_latest_period,
    permits_latest_period,

    price_fresh,
    rent_fresh,
    inventory_fresh,
    affordability_fresh,
    labor_fresh,
    permits_fresh,

    has_geometry,
    reasons

FROM final
ORDER BY
    overall_status_score ASC,
    geo_type,
    market_name;
"""


def convert(value: Any) -> Any:
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, list):
        return [convert(item) for item in value]
    return value


def row_to_dict(row: Any) -> dict[str, Any]:
    return {key: convert(value) for key, value in dict(row).items()}


def write_markdown(rows: list[dict[str, Any]], summary: dict[str, Any], path: Path) -> None:
    lines: list[str] = []
    lines.append("# Michigan Jurisdiction Data Status")
    lines.append("")
    lines.append(f"Generated at: `{summary['generated_at']}`")
    lines.append("")
    lines.append("## Summary")
    lines.append("")
    lines.append(f"- Total Michigan jurisdictions: **{summary['total']}**")
    lines.append(f"- Ready: **{summary['ready']}**")
    lines.append(f"- Usable with caveats: **{summary['usable_with_caveats']}**")
    lines.append(f"- Limited: **{summary['limited']}**")
    lines.append(f"- Not reliable: **{summary['not_reliable']}**")
    lines.append(f"- Average score: **{summary['average_score']}**")
    lines.append("")

    lines.append("## Worst jurisdictions first")
    lines.append("")
    lines.append("| Score | Status | Geo ID | Type | Market | Reasons |")
    lines.append("|---:|---|---|---|---|---|")

    for row in rows[:100]:
        reasons = ", ".join(row.get("reasons") or [])
        lines.append(
            f"| {row['overall_status_score']:.4f} "
            f"| {row['status_label']} "
            f"| `{row['geo_id']}` "
            f"| {row['geo_type']} "
            f"| {row['market_name']} "
            f"| {reasons or 'none'} |"
        )

    lines.append("")
    lines.append("## Score logic")
    lines.append("")
    lines.append("- Coverage: 45%")
    lines.append("- Freshness: 25%")
    lines.append("- Validity: 15%")
    lines.append("- Geometry: 10%")
    lines.append("- Surface/app usability: 5%")
    lines.append("")
    lines.append("A score is a data readiness score, not an investment attractiveness score.")

    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> int:
    engine = create_engine(DATABASE_URL)

    with engine.connect() as connection:
        rows = [row_to_dict(row) for row in connection.execute(text(SQL)).mappings().all()]

    total = len(rows)
    summary = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "total": total,
        "ready": sum(1 for row in rows if row["status_label"] == "ready"),
        "usable_with_caveats": sum(1 for row in rows if row["status_label"] == "usable_with_caveats"),
        "limited": sum(1 for row in rows if row["status_label"] == "limited"),
        "not_reliable": sum(1 for row in rows if row["status_label"] == "not_reliable"),
        "average_score": round(
            sum(float(row["overall_status_score"]) for row in rows) / total,
            4,
        ) if total else 0,
    }

    report = {
        "summary": summary,
        "rows": rows,
    }

    json_path = OUT_DIR / "michigan_jurisdiction_status_latest.json"
    csv_path = OUT_DIR / "michigan_jurisdiction_status_latest.csv"
    md_path = OUT_DIR / "michigan_jurisdiction_status_latest.md"

    json_path.write_text(json.dumps(report, indent=2), encoding="utf-8")

    if rows:
        with csv_path.open("w", encoding="utf-8", newline="") as file:
            writer = csv.DictWriter(file, fieldnames=list(rows[0].keys()))
            writer.writeheader()
            writer.writerows(rows)

    write_markdown(rows, summary, md_path)

    print("== Michigan jurisdiction status summary ==")
    print(json.dumps(summary, indent=2))
    print()
    print("Worst 25 jurisdictions:")
    for row in rows[:25]:
        reasons = ", ".join(row.get("reasons") or [])
        print(
            f"{row['overall_status_score']:.4f} | "
            f"{row['status_label']:<20} | "
            f"{row['geo_id']:<16} | "
            f"{row['geo_type']:<8} | "
            f"{row['market_name']} | "
            f"{reasons or 'none'}"
        )

    print()
    print(f"JSON: {json_path}")
    print(f"CSV:  {csv_path}")
    print(f"MD:   {md_path}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
