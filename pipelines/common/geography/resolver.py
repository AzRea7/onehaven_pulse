from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from typing import Any

from sqlalchemy import text
from sqlalchemy.engine import Connection


@dataclass(frozen=True)
class GeographyResolution:
    canonical_geo_id: str
    match_method: str
    confidence_score: Decimal
    source: str | None = None
    source_geo_id: str | None = None
    source_geo_name: str | None = None
    source_geo_type: str | None = None
    notes: str | None = None


def _clean_optional(value: str | None) -> str | None:
    if value is None:
        return None

    stripped = str(value).strip()
    return stripped or None


def _normalize_source(value: str | None) -> str | None:
    cleaned = _clean_optional(value)
    return cleaned.lower() if cleaned else None


def _normalize_state_code(value: str | None) -> str | None:
    cleaned = _clean_optional(value)

    if cleaned is None:
        return None

    cleaned = cleaned.lower()

    if len(cleaned) != 2:
        return None

    if not cleaned.isalpha():
        return None

    return cleaned


def _numeric_code(value: str | int | None, *, width: int) -> str | None:
    if value is None:
        return None

    raw = str(value).strip()

    if not raw.isdigit():
        return None

    padded = raw.zfill(width)

    if len(padded) != width:
        return None

    return padded


class GeographyResolver:
    """Resolve provider geography references to canonical OneHaven geo_id values.

    Resolution order:

    1. geo.geo_crosswalk exact source/source_geo_id match.
    2. direct canonical geo_id match.
    3. state_code lookup.
    4. cbsa_code lookup.
    5. county_fips lookup.
    6. zcta lookup.
    """

    def __init__(self, connection: Connection):
        self.connection = connection

    def resolve(
        self,
        *,
        source: str | None = None,
        source_geo_id: str | int | None = None,
        source_geo_name: str | None = None,
        source_geo_type: str | None = None,
        canonical_geo_id: str | None = None,
        state_code: str | None = None,
        cbsa_code: str | int | None = None,
        county_fips: str | int | None = None,
        place_fips: str | int | None = None,
        zcta: str | int | None = None,
    ) -> GeographyResolution | None:
        source_normalized = _normalize_source(source)
        source_geo_id_clean = _clean_optional(str(source_geo_id)) if source_geo_id is not None else None
        source_geo_name_clean = _clean_optional(source_geo_name)
        source_geo_type_clean = _clean_optional(source_geo_type)

        if source_normalized and source_geo_id_clean:
            crosswalk_match = self.resolve_crosswalk(
                source=source_normalized,
                source_geo_id=source_geo_id_clean,
            )

            if crosswalk_match:
                return crosswalk_match

        if canonical_geo_id:
            direct_match = self.resolve_direct_geo_id(
                canonical_geo_id=canonical_geo_id,
                source=source_normalized,
                source_geo_id=source_geo_id_clean,
                source_geo_name=source_geo_name_clean,
                source_geo_type=source_geo_type_clean,
            )

            if direct_match:
                return direct_match

        # If source_geo_id is already canonical, allow it as a direct fallback.
        if source_geo_id_clean:
            direct_from_source_id = self.resolve_direct_geo_id(
                canonical_geo_id=source_geo_id_clean,
                source=source_normalized,
                source_geo_id=source_geo_id_clean,
                source_geo_name=source_geo_name_clean,
                source_geo_type=source_geo_type_clean,
            )

            if direct_from_source_id:
                return direct_from_source_id

        state_match = self.resolve_state_code(
            state_code=state_code,
            source=source_normalized,
            source_geo_id=source_geo_id_clean,
            source_geo_name=source_geo_name_clean,
            source_geo_type=source_geo_type_clean,
        )

        if state_match:
            return state_match

        cbsa_match = self.resolve_cbsa_code(
            cbsa_code=cbsa_code,
            source=source_normalized,
            source_geo_id=source_geo_id_clean,
            source_geo_name=source_geo_name_clean,
            source_geo_type=source_geo_type_clean,
        )

        if cbsa_match:
            return cbsa_match

        county_match = self.resolve_county_fips(
            county_fips=county_fips,
            source=source_normalized,
            source_geo_id=source_geo_id_clean,
            source_geo_name=source_geo_name_clean,
            source_geo_type=source_geo_type_clean,
        )

        if county_match:
            return county_match

        place_match = self.resolve_place_fips(
            place_fips=place_fips,
            source=source_normalized,
            source_geo_id=source_geo_id_clean,
            source_geo_name=source_geo_name_clean,
            source_geo_type=source_geo_type_clean,
        )

        if place_match:
            return place_match

        zcta_match = self.resolve_zcta(
            zcta=zcta,
            source=source_normalized,
            source_geo_id=source_geo_id_clean,
            source_geo_name=source_geo_name_clean,
            source_geo_type=source_geo_type_clean,
        )

        if zcta_match:
            return zcta_match

        return None

    def resolve_crosswalk(
        self,
        *,
        source: str,
        source_geo_id: str,
    ) -> GeographyResolution | None:
        sql = text(
            """
            SELECT
                x.source,
                x.source_geo_id,
                x.source_geo_name,
                x.source_geo_type,
                x.canonical_geo_id,
                x.match_method,
                x.confidence_score,
                x.notes
            FROM geo.geo_crosswalk x
            JOIN geo.dim_geo g
              ON g.geo_id = x.canonical_geo_id
             AND g.is_active = true
            WHERE lower(x.source) = :source
              AND x.source_geo_id = :source_geo_id
            ORDER BY
                x.confidence_score DESC,
                x.match_method ASC,
                x.canonical_geo_id ASC
            LIMIT 1
            """
        )

        row = self.connection.execute(
            sql,
            {
                "source": source.lower(),
                "source_geo_id": source_geo_id,
            },
        ).mappings().first()

        if not row:
            return None

        return GeographyResolution(
            canonical_geo_id=str(row["canonical_geo_id"]),
            match_method=f"crosswalk_{row['match_method']}",
            confidence_score=Decimal(str(row["confidence_score"])),
            source=str(row["source"]) if row["source"] is not None else None,
            source_geo_id=str(row["source_geo_id"]) if row["source_geo_id"] is not None else None,
            source_geo_name=str(row["source_geo_name"]) if row["source_geo_name"] is not None else None,
            source_geo_type=str(row["source_geo_type"]) if row["source_geo_type"] is not None else None,
            notes=str(row["notes"]) if row["notes"] is not None else None,
        )

    def resolve_direct_geo_id(
        self,
        *,
        canonical_geo_id: str,
        source: str | None = None,
        source_geo_id: str | None = None,
        source_geo_name: str | None = None,
        source_geo_type: str | None = None,
    ) -> GeographyResolution | None:
        sql = text(
            """
            SELECT geo_id
            FROM geo.dim_geo
            WHERE geo_id = :geo_id
              AND is_active = true
            LIMIT 1
            """
        )

        row = self.connection.execute(
            sql,
            {"geo_id": canonical_geo_id.strip()},
        ).mappings().first()

        if not row:
            return None

        return GeographyResolution(
            canonical_geo_id=str(row["geo_id"]),
            match_method="direct_geo_id",
            confidence_score=Decimal("1.0000"),
            source=source,
            source_geo_id=source_geo_id,
            source_geo_name=source_geo_name,
            source_geo_type=source_geo_type,
            notes="Resolved because source value was already a canonical geo_id.",
        )

    def resolve_state_code(
        self,
        *,
        state_code: str | None,
        source: str | None = None,
        source_geo_id: str | None = None,
        source_geo_name: str | None = None,
        source_geo_type: str | None = None,
    ) -> GeographyResolution | None:
        normalized = _normalize_state_code(state_code)

        if not normalized:
            return None

        sql = text(
            """
            SELECT geo_id
            FROM geo.dim_geo
            WHERE geo_type = 'state'
              AND lower(state_code) = :state_code
              AND is_active = true
            ORDER BY geo_id
            LIMIT 1
            """
        )

        row = self.connection.execute(
            sql,
            {"state_code": normalized},
        ).mappings().first()

        if not row:
            return None

        return GeographyResolution(
            canonical_geo_id=str(row["geo_id"]),
            match_method="state_code_exact",
            confidence_score=Decimal("1.0000"),
            source=source,
            source_geo_id=source_geo_id,
            source_geo_name=source_geo_name,
            source_geo_type=source_geo_type,
            notes=f"Resolved by state_code={normalized}.",
        )

    def resolve_cbsa_code(
        self,
        *,
        cbsa_code: str | int | None,
        source: str | None = None,
        source_geo_id: str | None = None,
        source_geo_name: str | None = None,
        source_geo_type: str | None = None,
    ) -> GeographyResolution | None:
        normalized = _numeric_code(cbsa_code, width=5)

        if not normalized:
            return None

        sql = text(
            """
            SELECT geo_id
            FROM geo.dim_geo
            WHERE geo_type = 'metro'
              AND cbsa_code = :cbsa_code
              AND is_active = true
            ORDER BY geo_id
            LIMIT 1
            """
        )

        row = self.connection.execute(
            sql,
            {"cbsa_code": normalized},
        ).mappings().first()

        if not row:
            return None

        return GeographyResolution(
            canonical_geo_id=str(row["geo_id"]),
            match_method="cbsa_code_exact",
            confidence_score=Decimal("1.0000"),
            source=source,
            source_geo_id=source_geo_id,
            source_geo_name=source_geo_name,
            source_geo_type=source_geo_type,
            notes=f"Resolved by cbsa_code={normalized}.",
        )

    def resolve_county_fips(
        self,
        *,
        county_fips: str | int | None,
        source: str | None = None,
        source_geo_id: str | None = None,
        source_geo_name: str | None = None,
        source_geo_type: str | None = None,
    ) -> GeographyResolution | None:
        normalized = _numeric_code(county_fips, width=5)

        if not normalized:
            return None

        sql = text(
            """
            SELECT geo_id
            FROM geo.dim_geo
            WHERE geo_type = 'county'
              AND county_fips = :county_fips
              AND is_active = true
            ORDER BY geo_id
            LIMIT 1
            """
        )

        row = self.connection.execute(
            sql,
            {"county_fips": normalized},
        ).mappings().first()

        if not row:
            return None

        return GeographyResolution(
            canonical_geo_id=str(row["geo_id"]),
            match_method="county_fips_exact",
            confidence_score=Decimal("1.0000"),
            source=source,
            source_geo_id=source_geo_id,
            source_geo_name=source_geo_name,
            source_geo_type=source_geo_type,
            notes=f"Resolved by county_fips={normalized}.",
        )


    def resolve_place_fips(
        self,
        *,
        place_fips: str | int | None,
        source: str | None = None,
        source_geo_id: str | None = None,
        source_geo_name: str | None = None,
        source_geo_type: str | None = None,
    ) -> GeographyResolution | None:
        normalized = _numeric_code(place_fips, width=7)

        if not normalized:
            return None

        sql = text(
            """
            SELECT geo_id
            FROM geo.dim_geo
            WHERE geo_type = 'place'
              AND place_fips = :place_fips
              AND is_active = true
            ORDER BY geo_id
            LIMIT 1
            """
        )

        row = self.connection.execute(
            sql,
            {"place_fips": normalized},
        ).mappings().first()

        if not row:
            return None

        return GeographyResolution(
            canonical_geo_id=str(row["geo_id"]),
            match_method="place_fips_exact",
            confidence_score=Decimal("1.0000"),
            source=source,
            source_geo_id=source_geo_id,
            source_geo_name=source_geo_name,
            source_geo_type=source_geo_type,
            notes=f"Resolved by place_fips={normalized}.",
        )


    def resolve_zcta(
        self,
        *,
        zcta: str | int | None,
        source: str | None = None,
        source_geo_id: str | None = None,
        source_geo_name: str | None = None,
        source_geo_type: str | None = None,
    ) -> GeographyResolution | None:
        normalized = _numeric_code(zcta, width=5)

        if not normalized:
            return None

        sql = text(
            """
            SELECT geo_id
            FROM geo.dim_geo
            WHERE geo_type = 'zcta'
              AND zcta = :zcta
              AND is_active = true
            ORDER BY geo_id
            LIMIT 1
            """
        )

        row = self.connection.execute(
            sql,
            {"zcta": normalized},
        ).mappings().first()

        if not row:
            return None

        return GeographyResolution(
            canonical_geo_id=str(row["geo_id"]),
            match_method="zcta_exact",
            confidence_score=Decimal("1.0000"),
            source=source,
            source_geo_id=source_geo_id,
            source_geo_name=source_geo_name,
            source_geo_type=source_geo_type,
            notes=f"Resolved by zcta={normalized}.",
        )


def resolve_geography(
    connection: Connection,
    **kwargs: Any,
) -> GeographyResolution | None:
    return GeographyResolver(connection).resolve(**kwargs)
