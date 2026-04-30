from datetime import date, datetime
from decimal import Decimal
from typing import Any

from sqlalchemy import Date, DateTime, ForeignKey, Numeric, String
from sqlalchemy.dialects.postgresql import JSON
from sqlalchemy.orm import Mapped, mapped_column
from sqlalchemy.sql import func

from app.models.geography import Base


class MarketMonthlyMetric(Base):
    __tablename__ = "market_monthly_metrics"
    __table_args__ = {"schema": "analytics"}

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    geo_id: Mapped[str] = mapped_column(
        String(64),
        ForeignKey("geo.dim_geo.geo_id", ondelete="CASCADE"),
        nullable=False,
    )
    period_month: Mapped[date] = mapped_column(Date, nullable=False)

    home_price_index: Mapped[Decimal | None] = mapped_column(Numeric(18, 6), nullable=True)
    home_price_index_yoy: Mapped[Decimal | None] = mapped_column(Numeric(12, 6), nullable=True)
    home_price_index_mom: Mapped[Decimal | None] = mapped_column(Numeric(12, 6), nullable=True)
    real_home_price_index: Mapped[Decimal | None] = mapped_column(Numeric(18, 6), nullable=True)

    zhvi: Mapped[Decimal | None] = mapped_column(Numeric(18, 2), nullable=True)
    zhvi_yoy: Mapped[Decimal | None] = mapped_column(Numeric(12, 6), nullable=True)
    zhvi_mom: Mapped[Decimal | None] = mapped_column(Numeric(12, 6), nullable=True)

    median_sale_price: Mapped[Decimal | None] = mapped_column(Numeric(18, 2), nullable=True)
    median_sale_price_yoy: Mapped[Decimal | None] = mapped_column(Numeric(12, 6), nullable=True)
    median_sale_price_mom: Mapped[Decimal | None] = mapped_column(Numeric(12, 6), nullable=True)

    zori: Mapped[Decimal | None] = mapped_column(Numeric(18, 2), nullable=True)
    zori_yoy: Mapped[Decimal | None] = mapped_column(Numeric(12, 6), nullable=True)
    zori_mom: Mapped[Decimal | None] = mapped_column(Numeric(12, 6), nullable=True)

    median_rent: Mapped[Decimal | None] = mapped_column(Numeric(18, 2), nullable=True)
    median_rent_yoy: Mapped[Decimal | None] = mapped_column(Numeric(12, 6), nullable=True)
    rent_to_price_ratio: Mapped[Decimal | None] = mapped_column(Numeric(12, 6), nullable=True)

    active_listings: Mapped[Decimal | None] = mapped_column(Numeric(18, 2), nullable=True)
    active_listings_yoy: Mapped[Decimal | None] = mapped_column(Numeric(12, 6), nullable=True)
    new_listings: Mapped[Decimal | None] = mapped_column(Numeric(18, 2), nullable=True)
    new_listings_yoy: Mapped[Decimal | None] = mapped_column(Numeric(12, 6), nullable=True)
    homes_sold: Mapped[Decimal | None] = mapped_column(Numeric(18, 2), nullable=True)
    homes_sold_yoy: Mapped[Decimal | None] = mapped_column(Numeric(12, 6), nullable=True)
    months_supply: Mapped[Decimal | None] = mapped_column(Numeric(12, 4), nullable=True)
    median_days_on_market: Mapped[Decimal | None] = mapped_column(Numeric(12, 4), nullable=True)
    sale_to_list_ratio: Mapped[Decimal | None] = mapped_column(Numeric(12, 6), nullable=True)
    price_drops_pct: Mapped[Decimal | None] = mapped_column(Numeric(12, 6), nullable=True)

    mortgage_rate_30y: Mapped[Decimal | None] = mapped_column(Numeric(8, 4), nullable=True)
    fed_funds_rate: Mapped[Decimal | None] = mapped_column(Numeric(8, 4), nullable=True)
    cpi: Mapped[Decimal | None] = mapped_column(Numeric(18, 6), nullable=True)
    unemployment_rate: Mapped[Decimal | None] = mapped_column(Numeric(8, 4), nullable=True)

    estimated_monthly_payment: Mapped[Decimal | None] = mapped_column(Numeric(18, 2), nullable=True)
    payment_to_income_ratio: Mapped[Decimal | None] = mapped_column(Numeric(12, 6), nullable=True)
    price_to_income_ratio: Mapped[Decimal | None] = mapped_column(Numeric(12, 6), nullable=True)

    building_permits: Mapped[Decimal | None] = mapped_column(Numeric(18, 2), nullable=True)
    permits_per_1000_people: Mapped[Decimal | None] = mapped_column(Numeric(12, 6), nullable=True)
    population: Mapped[Decimal | None] = mapped_column(Numeric(18, 2), nullable=True)
    population_yoy: Mapped[Decimal | None] = mapped_column(Numeric(12, 6), nullable=True)
    median_household_income: Mapped[Decimal | None] = mapped_column(Numeric(18, 2), nullable=True)
    households: Mapped[Decimal | None] = mapped_column(Numeric(18, 2), nullable=True)

    source_flags: Mapped[dict[str, Any] | None] = mapped_column(JSON, nullable=True)
    quality_flags: Mapped[dict[str, Any] | None] = mapped_column(JSON, nullable=True)

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
    )


class MarketMetricSource(Base):
    __tablename__ = "market_metric_sources"
    __table_args__ = {"schema": "analytics"}

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    geo_id: Mapped[str] = mapped_column(
        String(64),
        ForeignKey("geo.dim_geo.geo_id", ondelete="CASCADE"),
        nullable=False,
    )
    period_month: Mapped[date] = mapped_column(Date, nullable=False)
    metric_name: Mapped[str] = mapped_column(String(150), nullable=False)
    source: Mapped[str] = mapped_column(String(100), nullable=False)
    dataset: Mapped[str] = mapped_column(String(150), nullable=False)
    source_file_id: Mapped[str | None] = mapped_column(
        String(64),
        ForeignKey("audit.source_files.id", ondelete="SET NULL"),
        nullable=True,
    )
    pipeline_run_id: Mapped[str | None] = mapped_column(
        String(64),
        ForeignKey("audit.pipeline_runs.id", ondelete="SET NULL"),
        nullable=True,
    )
    source_value: Mapped[Decimal | None] = mapped_column(Numeric(24, 8), nullable=True)
    normalized_value: Mapped[Decimal | None] = mapped_column(Numeric(24, 8), nullable=True)
    source_period: Mapped[date | None] = mapped_column(Date, nullable=True)
    transformation_notes: Mapped[str | None] = mapped_column(String, nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
    )
