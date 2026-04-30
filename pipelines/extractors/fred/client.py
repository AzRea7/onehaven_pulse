from typing import Any

import requests

from pipelines.common.settings import settings


class FredClient:
    def __init__(
        self,
        api_key: str | None = None,
        base_url: str | None = None,
        timeout_seconds: int = 30,
    ) -> None:
        self.api_key = api_key or settings.fred_api_key
        self.base_url = (base_url or settings.fred_base_url).rstrip("/")
        self.timeout_seconds = timeout_seconds

        if not self.api_key or self.api_key == "replace_with_your_fred_api_key":
            raise ValueError(
                "FRED_API_KEY is missing. Add it to .env or pass api_key explicitly."
            )

    def get_series_observations(
        self,
        series_id: str,
        observation_start: str | None = None,
        observation_end: str | None = None,
    ) -> dict[str, Any]:
        url = f"{self.base_url}/series/observations"

        params: dict[str, str] = {
            "series_id": series_id,
            "api_key": self.api_key,
            "file_type": "json",
        }

        if observation_start:
            params["observation_start"] = observation_start

        if observation_end:
            params["observation_end"] = observation_end

        response = requests.get(
            url,
            params=params,
            timeout=self.timeout_seconds,
        )
        response.raise_for_status()

        payload = response.json()

        if "observations" not in payload:
            raise ValueError(
                f"Unexpected FRED response for series_id={series_id}: missing observations"
            )

        return payload
