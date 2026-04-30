from typing import Any
from urllib.parse import urljoin

import requests
from requests.exceptions import JSONDecodeError

from pipelines.common.settings import settings
from pipelines.extractors.census_acs.config import CensusAcsDataset


class CensusAcsClient:
    def __init__(self, timeout_seconds: int = 90) -> None:
        self.timeout_seconds = timeout_seconds
        self.base_url = settings.census_acs_base_url.rstrip("/") + "/"

    def get_dataset(self, dataset: CensusAcsDataset) -> list[list[Any]]:
        url = urljoin(self.base_url, dataset.endpoint_path)

        params = {
            "get": ",".join(dataset.variables),
            **dataset.params,
        }

        if settings.census_data_api_key and not settings.census_data_api_key.startswith("replace_with_"):
            params["key"] = settings.census_data_api_key

        response = requests.get(
            url,
            params=params,
            timeout=self.timeout_seconds,
            headers={
                "User-Agent": "OneHavenMarketEngine/0.1",
            },
        )

        safe_url = response.url
        if settings.census_data_api_key:
            safe_url = safe_url.replace(settings.census_data_api_key, "***REDACTED***")

        if "invalid_key.html" in response.url or "Invalid Key" in response.text[:500]:
            raise ValueError(
                "Census rejected CENSUS_DATA_API_KEY as invalid. "
                "Check that the key is copied exactly, has no quotes, has no spaces, "
                "and is not prefixed with 'key='. "
                f"URL={safe_url}"
            )

        if response.status_code == 400:
            raise ValueError(
                "Census ACS API returned 400. This usually means a bad variable, "
                "bad geography predicate, unavailable ACS year, or malformed request. "
                f"URL={safe_url} BODY={response.text[:1000]}"
            )

        if response.status_code == 403:
            raise ValueError(
                "Census ACS API returned 403. Check CENSUS_DATA_API_KEY in .env. "
                f"URL={safe_url} BODY={response.text[:1000]}"
            )

        response.raise_for_status()

        try:
            payload = response.json()
        except JSONDecodeError as exc:
            raise ValueError(
                "Census ACS API returned a non-JSON response. "
                "This usually means a bad variable, unsupported geography, or invalid API key. "
                f"URL={safe_url} BODY={response.text[:1000]}"
            ) from exc

        self._validate_payload(dataset=dataset, payload=payload)

        return payload

    @staticmethod
    def _validate_payload(dataset: CensusAcsDataset, payload: Any) -> None:
        if not isinstance(payload, list):
            raise ValueError(
                f"Unexpected ACS response for geography={dataset.geography_level}: "
                "payload is not a list"
            )

        if len(payload) < 2:
            raise ValueError(
                f"Unexpected ACS response for geography={dataset.geography_level}: "
                "payload has no data rows"
            )

        headers = payload[0]

        if "NAME" not in headers:
            raise ValueError(
                f"Unexpected ACS response for geography={dataset.geography_level}: "
                "missing NAME column"
            )
