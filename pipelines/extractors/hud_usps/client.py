from typing import Any

import requests
from requests.exceptions import JSONDecodeError

from pipelines.common.settings import settings
from pipelines.extractors.hud_usps.config import HudUspsDataset


class HudUspsClient:
    def __init__(self, timeout_seconds: int = 180) -> None:
        self.timeout_seconds = timeout_seconds
        self.base_url = settings.hud_usps_api_base_url

        if not settings.hud_usps_access_token:
            raise ValueError(
                "HUD_USPS_ACCESS_TOKEN is missing. Add it to .env. "
                "Do not include the 'Bearer ' prefix."
            )

    def get_dataset(self, dataset: HudUspsDataset) -> dict[str, Any]:
        params = {
            "type": str(dataset.api_type),
            "query": dataset.query,
            "year": str(dataset.year),
            "quarter": str(dataset.quarter),
        }

        headers = {
            "Authorization": f"Bearer {settings.hud_usps_access_token}",
            "Accept": "application/json",
            "User-Agent": "OneHavenMarketEngine/0.1",
        }

        response = requests.get(
            self.base_url,
            params=params,
            headers=headers,
            timeout=self.timeout_seconds,
        )

        safe_url = response.url

        if response.status_code == 400:
            raise ValueError(
                "HUD-USPS API returned 400. Invalid query parameter. "
                f"crosswalk_type={dataset.crosswalk_type} URL={safe_url} "
                f"BODY={response.text[:1000]}"
            )

        if response.status_code == 401:
            raise ValueError(
                "HUD-USPS API returned 401. Authentication failed. "
                "Check HUD_USPS_ACCESS_TOKEN in .env."
            )

        if response.status_code == 403:
            raise ValueError(
                "HUD-USPS API returned 403. Your token may not be registered for "
                "the USPS Dataset API."
            )

        if response.status_code == 404:
            raise ValueError(
                "HUD-USPS API returned 404. No data found for the requested "
                f"type/query/year/quarter. crosswalk_type={dataset.crosswalk_type} "
                f"URL={safe_url}"
            )

        if response.status_code == 405:
            raise ValueError("HUD-USPS API returned 405. Only GET is supported.")

        if response.status_code == 406:
            raise ValueError(
                "HUD-USPS API returned 406. Accept header must be application/json."
            )

        response.raise_for_status()

        try:
            payload = response.json()
        except JSONDecodeError as exc:
            raise ValueError(
                "HUD-USPS API returned a non-JSON response. "
                f"crosswalk_type={dataset.crosswalk_type} "
                f"URL={safe_url} BODY={response.text[:1000]}"
            ) from exc

        self._validate_payload(dataset=dataset, payload=payload)

        return payload

    @staticmethod
    def _data_blocks(payload: dict[str, Any]) -> list[dict[str, Any]]:
        data = payload.get("data")

        if isinstance(data, list):
            return [item for item in data if isinstance(item, dict)]

        if isinstance(data, dict):
            return [data]

        return []

    @classmethod
    def get_result_count(cls, payload: dict[str, Any]) -> int:
        count = 0

        for block in cls._data_blocks(payload):
            results = block.get("results", [])

            if isinstance(results, list):
                count += len(results)

        return count

    @classmethod
    def get_response_metadata(cls, payload: dict[str, Any]) -> dict[str, Any]:
        blocks = cls._data_blocks(payload)

        if not blocks:
            return {}

        first = blocks[0]

        return {
            "year": first.get("year"),
            "quarter": first.get("quarter"),
            "query": first.get("query") or first.get("input"),
            "crosswalk_type": first.get("crosswalk_type"),
        }

    @classmethod
    def _validate_payload(cls, dataset: HudUspsDataset, payload: Any) -> None:
        if not isinstance(payload, dict):
            raise ValueError(
                f"Unexpected HUD-USPS response for {dataset.crosswalk_type}: "
                "payload is not a JSON object"
            )

        blocks = cls._data_blocks(payload)

        if not blocks:
            raise ValueError(
                f"Unexpected HUD-USPS response for {dataset.crosswalk_type}: "
                "missing data block"
            )

        result_count = cls.get_result_count(payload)

        if result_count < 1:
            raise ValueError(
                f"Unexpected HUD-USPS response for {dataset.crosswalk_type}: "
                "no results returned"
            )
