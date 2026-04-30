from typing import Any

import requests
from requests.exceptions import JSONDecodeError

from pipelines.common.settings import settings
from pipelines.extractors.bls_laus.config import BlsLausDataset


class BlsLausClient:
    def __init__(self, timeout_seconds: int = 90) -> None:
        self.timeout_seconds = timeout_seconds
        self.base_url = settings.bls_api_base_url

    def get_dataset(self, dataset: BlsLausDataset) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "seriesid": [series.series_id for series in dataset.series],
            "startyear": str(dataset.start_year),
            "endyear": str(dataset.end_year),
        }

        if settings.bls_api_key:
            payload["registrationkey"] = settings.bls_api_key

        response = requests.post(
            self.base_url,
            json=payload,
            timeout=self.timeout_seconds,
            headers={
                "Content-Type": "application/json",
                "Accept": "application/json",
                "User-Agent": "OneHavenMarketEngine/0.1",
            },
        )

        if response.status_code == 400:
            raise ValueError(
                "BLS API returned 400. Check series IDs, start year, end year, "
                f"or request payload. BODY={response.text[:1000]}"
            )

        if response.status_code == 403:
            raise ValueError(
                "BLS API returned 403. Check BLS_API_KEY if configured. "
                f"BODY={response.text[:1000]}"
            )

        response.raise_for_status()

        try:
            data = response.json()
        except JSONDecodeError as exc:
            raise ValueError(
                "BLS API returned a non-JSON response. "
                f"BODY={response.text[:1000]}"
            ) from exc

        self._validate_response(data)

        return {
            "request": {
                "seriesid": payload["seriesid"],
                "startyear": payload["startyear"],
                "endyear": payload["endyear"],
                "api_key_configured": bool(settings.bls_api_key),
            },
            "series_metadata": [
                {
                    "series_id": series.series_id,
                    "label": series.label,
                    "geography_level": series.geography_level,
                    "measure": series.measure,
                    "geo_reference": series.geo_reference,
                }
                for series in dataset.series
            ],
            "response": data,
        }

    @staticmethod
    def _validate_response(data: Any) -> None:
        if not isinstance(data, dict):
            raise ValueError("Unexpected BLS API response: payload is not an object")

        status = data.get("status")

        if status != "REQUEST_SUCCEEDED":
            messages = data.get("message", [])
            raise ValueError(
                "BLS API request did not succeed. "
                f"status={status} messages={messages}"
            )

        results = data.get("Results", {})
        series = results.get("series", [])

        if not isinstance(series, list) or len(series) == 0:
            raise ValueError("Unexpected BLS API response: no series returned")

    @staticmethod
    def get_observation_count(payload: dict[str, Any]) -> int:
        response = payload.get("response", {})
        results = response.get("Results", {})
        series_list = results.get("series", [])

        count = 0

        for series in series_list:
            data_points = series.get("data", [])

            if isinstance(data_points, list):
                count += len(data_points)

        return count

    @staticmethod
    def get_latest_period(payload: dict[str, Any]) -> tuple[int | None, str | None]:
        response = payload.get("response", {})
        results = response.get("Results", {})
        series_list = results.get("series", [])

        latest_year: int | None = None
        latest_period: str | None = None

        for series in series_list:
            for observation in series.get("data", []):
                year_raw = observation.get("year")
                period = observation.get("period")

                if not year_raw or not period:
                    continue

                try:
                    year = int(year_raw)
                except ValueError:
                    continue

                if latest_year is None or (year, period) > (latest_year, latest_period or ""):
                    latest_year = year
                    latest_period = period

        return latest_year, latest_period
