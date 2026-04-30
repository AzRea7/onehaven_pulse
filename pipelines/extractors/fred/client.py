import json
import time
from typing import Any

import requests
from requests import HTTPError, Response

from pipelines.common.settings import settings


class FredClient:
    def __init__(
        self,
        timeout_seconds: int = 90,
        max_retries: int = 4,
        retry_sleep_seconds: float = 2.0,
    ) -> None:
        self.timeout_seconds = timeout_seconds
        self.max_retries = max_retries
        self.retry_sleep_seconds = retry_sleep_seconds
        self.base_url = settings.fred_base_url.rstrip("/")

        if not settings.fred_api_key:
            raise ValueError(
                "FRED_API_KEY is missing. Add it to .env before running the FRED extractor."
            )

    def get_series_observations(self, series_id: str) -> dict[str, Any]:
        url = f"{self.base_url}/series/observations"

        params = {
            "series_id": series_id,
            "api_key": settings.fred_api_key,
            "file_type": "json",
        }

        last_response: Response | None = None
        last_error: Exception | None = None

        for attempt in range(1, self.max_retries + 1):
            try:
                response = requests.get(
                    url,
                    params=params,
                    timeout=self.timeout_seconds,
                    headers={"User-Agent": "OneHavenMarketEngine/0.1"},
                )
                last_response = response

                if response.status_code in {500, 502, 503, 504}:
                    if attempt < self.max_retries:
                        time.sleep(self.retry_sleep_seconds * attempt)
                        continue

                    raise ValueError(
                        "FRED API server error after retries. "
                        f"series_id={series_id} "
                        f"status_code={response.status_code} "
                        f"url={self._safe_url(response.url)} "
                        f"body={response.text[:1000]}"
                    )

                if response.status_code == 400:
                    raise ValueError(
                        f"FRED API returned 400 for series_id={series_id}. "
                        f"url={self._safe_url(response.url)} "
                        f"body={response.text[:1000]}"
                    )

                if response.status_code == 403:
                    raise ValueError(
                        f"FRED API returned 403 for series_id={series_id}. "
                        "Check FRED_API_KEY in .env."
                    )

                response.raise_for_status()

                payload = response.json()

                if "observations" not in payload:
                    raise ValueError(
                        f"Unexpected FRED response for series_id={series_id}: "
                        "missing observations"
                    )

                return payload

            except HTTPError as exc:
                last_error = exc

                if attempt < self.max_retries:
                    time.sleep(self.retry_sleep_seconds * attempt)
                    continue

                raise ValueError(
                    "FRED HTTP error after retries. "
                    f"series_id={series_id} "
                    f"url={self._safe_url(last_response.url) if last_response else url}"
                ) from exc

            except requests.RequestException as exc:
                last_error = exc

                if attempt < self.max_retries:
                    time.sleep(self.retry_sleep_seconds * attempt)
                    continue

                raise ValueError(
                    "FRED request failed after retries. "
                    f"series_id={series_id} error={exc}"
                ) from exc

        raise ValueError(
            "FRED request failed unexpectedly. "
            f"series_id={series_id} last_error={last_error}"
        )

    @staticmethod
    def dumps(payload: dict[str, Any]) -> str:
        return json.dumps(payload, indent=2, sort_keys=False)

    @staticmethod
    def _safe_url(url: str) -> str:
        api_key = settings.fred_api_key

        if api_key:
            return url.replace(api_key, "***REDACTED***")

        return url
