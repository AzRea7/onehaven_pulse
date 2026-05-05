from __future__ import annotations

import csv
import re
from dataclasses import dataclass
from decimal import Decimal
from pathlib import Path


STATE_ABBREVIATIONS_BY_FIPS: dict[str, str] = {
    "01": "AL", "02": "AK", "04": "AZ", "05": "AR", "06": "CA",
    "08": "CO", "09": "CT", "10": "DE", "11": "DC", "12": "FL",
    "13": "GA", "15": "HI", "16": "ID", "17": "IL", "18": "IN",
    "19": "IA", "20": "KS", "21": "KY", "22": "LA", "23": "ME",
    "24": "MD", "25": "MA", "26": "MI", "27": "MN", "28": "MS",
    "29": "MO", "30": "MT", "31": "NE", "32": "NV", "33": "NH",
    "34": "NJ", "35": "NM", "36": "NY", "37": "NC", "38": "ND",
    "39": "OH", "40": "OK", "41": "OR", "42": "PA", "44": "RI",
    "45": "SC", "46": "SD", "47": "TN", "48": "TX", "49": "UT",
    "50": "VT", "51": "VA", "53": "WA", "54": "WV", "55": "WI",
    "56": "WY", "60": "AS", "66": "GU", "69": "MP", "72": "PR",
    "78": "VI",
}


STATE_NAMES_BY_FIPS: dict[str, str] = {
    "01": "Alabama", "02": "Alaska", "04": "Arizona", "05": "Arkansas",
    "06": "California", "08": "Colorado", "09": "Connecticut",
    "10": "Delaware", "11": "District of Columbia", "12": "Florida",
    "13": "Georgia", "15": "Hawaii", "16": "Idaho", "17": "Illinois",
    "18": "Indiana", "19": "Iowa", "20": "Kansas", "21": "Kentucky",
    "22": "Louisiana", "23": "Maine", "24": "Maryland",
    "25": "Massachusetts", "26": "Michigan", "27": "Minnesota",
    "28": "Mississippi", "29": "Missouri", "30": "Montana",
    "31": "Nebraska", "32": "Nevada", "33": "New Hampshire",
    "34": "New Jersey", "35": "New Mexico", "36": "New York",
    "37": "North Carolina", "38": "North Dakota", "39": "Ohio",
    "40": "Oklahoma", "41": "Oregon", "42": "Pennsylvania",
    "44": "Rhode Island", "45": "South Carolina", "46": "South Dakota",
    "47": "Tennessee", "48": "Texas", "49": "Utah", "50": "Vermont",
    "51": "Virginia", "53": "Washington", "54": "West Virginia",
    "55": "Wisconsin", "56": "Wyoming", "60": "American Samoa",
    "66": "Guam", "69": "Northern Mariana Islands", "72": "Puerto Rico",
    "78": "U.S. Virgin Islands",
}


@dataclass(frozen=True)
class CensusPlaceRecord:
    state_fips: str
    place_fips: str
    name: str
    latitude: Decimal | None
    longitude: Decimal | None

    @property
    def state_code(self) -> str | None:
        return STATE_ABBREVIATIONS_BY_FIPS.get(self.state_fips)

    @property
    def state_name(self) -> str | None:
        return STATE_NAMES_BY_FIPS.get(self.state_fips)

    @property
    def full_place_fips(self) -> str:
        return f"{self.state_fips}{self.place_fips}"

    @property
    def geo_id(self) -> str:
        return f"place_{self.full_place_fips}"


@dataclass(frozen=True)
class CensusZctaRecord:
    zcta: str
    latitude: Decimal | None
    longitude: Decimal | None

    @property
    def geo_id(self) -> str:
        return f"zcta_{self.zcta}"


def slugify(value: str) -> str:
    value = value.strip().lower()
    value = re.sub(r"[^a-z0-9]+", "-", value)
    value = re.sub(r"-+", "-", value)
    return value.strip("-")


def _clean(value: str | None) -> str | None:
    if value is None:
        return None

    value = str(value).strip()
    return value or None


def _decimal(value: str | None) -> Decimal | None:
    cleaned = _clean(value)

    if cleaned is None:
        return None

    return Decimal(cleaned)


def _normalize_header(value: str) -> str:
    return value.strip().lower().replace(" ", "").replace("_", "")


def _read_delimited(path: Path) -> list[dict[str, str]]:
    text = path.read_text(encoding="utf-8-sig", errors="replace")

    sample = text[:2048]
    delimiter = "\t" if "\t" in sample else ","

    reader = csv.DictReader(text.splitlines(), delimiter=delimiter)

    rows: list[dict[str, str]] = []

    for row in reader:
        rows.append({
            _normalize_header(key): value
            for key, value in row.items()
            if key is not None
        })

    return rows


def _first(row: dict[str, str], keys: list[str]) -> str | None:
    for key in keys:
        value = _clean(row.get(_normalize_header(key)))

        if value is not None:
            return value

    return None


def read_census_places(path: Path) -> list[CensusPlaceRecord]:
    if not path.exists():
        return []

    records: list[CensusPlaceRecord] = []

    for row in _read_delimited(path):
        geoid = _first(row, ["GEOID", "GEO_ID", "geoid"])
        state_fips = _first(row, ["STATE", "STATEFP", "state_fips"])
        place_fips = _first(row, ["PLACE", "PLACEFP", "place_fips"])

        if geoid and geoid.isdigit() and len(geoid) >= 7:
            state_fips = geoid[:2]
            place_fips = geoid[-5:]

        if not state_fips or not place_fips:
            raise ValueError(f"Place row missing state/place fips: {row}")

        state_fips = state_fips.zfill(2)
        place_fips = place_fips.zfill(5)

        name = _first(row, ["NAME", "name", "BASENAME", "basename"])

        if not name:
            raise ValueError(f"Place row missing name: {row}")

        latitude = _decimal(_first(row, ["INTPTLAT", "LATITUDE", "lat", "intptlat"]))
        longitude = _decimal(_first(row, ["INTPTLONG", "LONGITUDE", "lon", "lng", "intptlong"]))

        records.append(
            CensusPlaceRecord(
                state_fips=state_fips,
                place_fips=place_fips,
                name=name,
                latitude=latitude,
                longitude=longitude,
            )
        )

    return records


def read_census_zctas(path: Path) -> list[CensusZctaRecord]:
    if not path.exists():
        return []

    records: list[CensusZctaRecord] = []

    for row in _read_delimited(path):
        zcta = _first(row, ["ZCTA5", "ZCTA", "GEOID", "GEO_ID", "zcta"])

        if not zcta:
            raise ValueError(f"ZCTA row missing zcta/geoid: {row}")

        zcta = zcta.strip().zfill(5)

        if not zcta.isdigit() or len(zcta) != 5:
            raise ValueError(f"Invalid ZCTA value {zcta!r}: {row}")

        latitude = _decimal(_first(row, ["INTPTLAT", "LATITUDE", "lat", "intptlat"]))
        longitude = _decimal(_first(row, ["INTPTLONG", "LONGITUDE", "lon", "lng", "intptlong"]))

        records.append(
            CensusZctaRecord(
                zcta=zcta,
                latitude=latitude,
                longitude=longitude,
            )
        )

    return records
