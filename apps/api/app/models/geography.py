from datetime import datetime
from decimal import Decimal

from sqlalchemy import Boolean, DateTime, ForeignKey, Numeric, String, Text
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column
from sqlalchemy.sql import func


class Base(DeclarativeBase):
    pass


class DimGeo(Base):
    __tablename__ = "dim_geo"
    __table_args__ = {"schema": "geo"}

    geo_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    geo_type: Mapped[str] = mapped_column(String(50), nullable=False)
    name: Mapped[str] = mapped_column(String(255), nullable=False)
    display_name: Mapped[str | None] = mapped_column(String(255), nullable=True)
    state_code: Mapped[str | None] = mapped_column(String(2), nullable=True)
    state_name: Mapped[str | None] = mapped_column(String(100), nullable=True)
    county_fips: Mapped[str | None] = mapped_column(String(5), nullable=True)
    cbsa_code: Mapped[str | None] = mapped_column(String(5), nullable=True)
    zcta: Mapped[str | None] = mapped_column(String(5), nullable=True)
    country_code: Mapped[str] = mapped_column(String(2), nullable=False, default="US")
    latitude: Mapped[Decimal | None] = mapped_column(Numeric(10, 6), nullable=True)
    longitude: Mapped[Decimal | None] = mapped_column(Numeric(10, 6), nullable=True)
    is_active: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
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


class GeoCrosswalk(Base):
    __tablename__ = "geo_crosswalk"
    __table_args__ = {"schema": "geo"}

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    source: Mapped[str] = mapped_column(String(100), nullable=False)
    source_geo_id: Mapped[str | None] = mapped_column(String(255), nullable=True)
    source_geo_name: Mapped[str] = mapped_column(String(255), nullable=False)
    source_geo_type: Mapped[str] = mapped_column(String(100), nullable=False)
    canonical_geo_id: Mapped[str] = mapped_column(
        String(64),
        ForeignKey("geo.dim_geo.geo_id", ondelete="CASCADE"),
        nullable=False,
    )
    match_method: Mapped[str] = mapped_column(String(100), nullable=False, default="unknown")
    confidence_score: Mapped[Decimal] = mapped_column(Numeric(5, 4), nullable=False)
    is_active: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    notes: Mapped[str | None] = mapped_column(Text, nullable=True)
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
