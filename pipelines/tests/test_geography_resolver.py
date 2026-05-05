from decimal import Decimal
from unittest.mock import Mock

from pipelines.common.geography.resolver import GeographyResolver


class FakeResult:
    def __init__(self, row):
        self.row = row

    def mappings(self):
        return self

    def first(self):
        return self.row


class FakeConnection:
    def __init__(self, rows):
        self.rows = list(rows)

    def execute(self, sql, params=None):
        if self.rows:
            return FakeResult(self.rows.pop(0))
        return FakeResult(None)


def test_resolver_uses_crosswalk_first():
    connection = FakeConnection(
        [
            {
                "source": "zillow",
                "source_geo_id": "394532",
                "source_geo_name": "Detroit, MI",
                "source_geo_type": "msa",
                "canonical_geo_id": "metro_19820",
                "match_method": "manual",
                "confidence_score": Decimal("1.0000"),
                "notes": "manual mapping",
            }
        ]
    )

    resolver = GeographyResolver(connection)

    result = resolver.resolve(source="zillow", source_geo_id="394532")

    assert result is not None
    assert result.canonical_geo_id == "metro_19820"
    assert result.match_method == "crosswalk_manual"
    assert result.confidence_score == Decimal("1.0000")


def test_resolver_direct_geo_id_fallback():
    connection = FakeConnection(
        [
            None,
            {
                "geo_id": "metro_19820",
            },
        ]
    )

    resolver = GeographyResolver(connection)

    result = resolver.resolve(source="example", source_geo_id="metro_19820")

    assert result is not None
    assert result.canonical_geo_id == "metro_19820"
    assert result.match_method == "direct_geo_id"


def test_resolver_returns_none_when_no_match():
    connection = FakeConnection([None, None, None, None, None, None])

    resolver = GeographyResolver(connection)

    result = resolver.resolve(source="missing", source_geo_id="missing")

    assert result is None
