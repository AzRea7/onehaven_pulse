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

        census_key = settings.census_data_api_key

        if census_key and not census_key.startswith("replace_with_"):
            params["key"] = census_key

        response = requests.get(
            url,
            params=params,
            timeout=self.timeout_seconds,
            headers={
                "User-Agent": "OneHavenMarketEngine/0.1",
            },
        )

        response_url = str(getattr(response, "url", ""))
        response_text = str(getattr(response, "text", ""))

        safe_url = response_url

        if census_key:
            safe_url = safe_url.replace(census_key, "***REDACTED***")

        if "invalid_key.html" in response_url or "Invalid Key" in response_text[:500]:
            raise ValueError(
                "Census rejected CENSUS_DATA_API_KEY as invalid. "
                "Check that the key is copied exactly, has no quotes, has no spaces, "
                f"and is not prefixed with 'key='. URL={safe_url}"
            )

        response.raise_for_status()

        try:
            payload = response.json()
        except JSONDecodeError as exc:
            raise ValueError(
                "Census ACS API returned a non-JSON response. "
                "This usually means a bad variable, invalid API key, "
                f"or unsupported geography. URL={safe_url} "
                f"BODY={response_text[:1000]}"
            ) from exc

        if not isinstance(payload, list):
            raise ValueError(
                "Unexpected Census ACS response shape. "
                f"Expected list. URL={safe_url}"
            )

        if len(payload) <= 1:
            raise ValueError(
                "Census ACS API returned no data rows. "
                f"URL={safe_url} PAYLOAD={payload}"
            )

        header = payload[0]

        if "NAME" not in header:
            raise ValueError(
                "Unexpected Census ACS response: missing NAME column. "
                f"URL={safe_url} HEADER={header}"
            )

        return payload
