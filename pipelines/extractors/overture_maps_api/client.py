from typing import Any
from urllib.parse import urljoin

import requests

from pipelines.common.settings import settings
from pipelines.extractors.overture_maps_api.config import OvertureMapsApiDataset


class OvertureMapsApiClient:
    def __init__(self, timeout_seconds: int = 120) -> None:
        self.timeout_seconds = timeout_seconds
        self.base_url = settings.overture_maps_api_base_url.rstrip("/") + "/"

        if not settings.overture_maps_api_key:
            raise ValueError(
                "OVERTURE_MAPS_API_KEY is missing. Add it to .env before running this extractor."
            )

    def get_places(self, dataset: OvertureMapsApiDataset) -> dict[str, Any]:
        if dataset.endpoint != "places":
            raise ValueError(
                "Only the /places endpoint is supported in Story 3.13. "
                f"Got endpoint={dataset.endpoint}"
            )

        url = urljoin(self.base_url, "places")
        params = self._build_places_params(dataset)

        response = requests.get(
            url,
            params=params,
            headers={
                "x-api-key": settings.overture_maps_api_key,
                "Accept": "application/json",
                "User-Agent": "OneHavenMarketEngine/0.1",
            },
            timeout=self.timeout_seconds,
        )

        if response.status_code == 401:
            raise ValueError("Overture Maps API returned 401. Check OVERTURE_MAPS_API_KEY.")

        if response.status_code == 403:
            raise ValueError("Overture Maps API returned 403. Your API key may not have access.")

        if response.status_code == 400:
            raise ValueError(
                "Overture Maps API returned 400. Check query parameters. "
                f"URL={self._safe_url(response.url)} BODY={response.text[:1000]}"
            )

        if response.status_code == 429:
            raise ValueError(
                "Overture Maps API returned 429 rate limit. Reduce request size or wait before retrying."
            )

        response.raise_for_status()

        payload = response.json()

        if not isinstance(payload, list):
            raise ValueError(
                "Unexpected Overture Maps API response. Expected a JSON array for /places."
            )

        return {
            "request": {
                "endpoint": dataset.endpoint,
                "base_url": dataset.base_url,
                "params": params,
                "api_key_required": True,
            },
            "record_count": len(payload),
            "records": payload,
        }

    @staticmethod
    def _build_places_params(dataset: OvertureMapsApiDataset) -> dict[str, str]:
        params: dict[str, str] = {}

        if dataset.country.strip():
            params["country"] = dataset.country.strip()

        params["lat"] = str(dataset.lat)
        params["lng"] = str(dataset.lng)
        params["radius"] = str(dataset.radius)

        if dataset.categories.strip():
            params["categories"] = dataset.categories.strip()

        if dataset.brand_name.strip():
            params["brand_name"] = dataset.brand_name.strip()

        if dataset.limit > 0:
            params["limit"] = str(dataset.limit)

        return params

    @staticmethod
    def _safe_url(url: str) -> str:
        key = settings.overture_maps_api_key

        if key:
            return url.replace(key, "***REDACTED***")

        return url
