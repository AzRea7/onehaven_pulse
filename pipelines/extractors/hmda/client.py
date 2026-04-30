from urllib.parse import urljoin

import requests

from pipelines.common.settings import settings
from pipelines.extractors.hmda.config import HmdaDataset


class HmdaClient:
    def __init__(self, timeout_seconds: int = 600) -> None:
        self.timeout_seconds = timeout_seconds
        self.base_url = settings.hmda_api_base_url.rstrip("/") + "/"

    def get_modified_lar_csv(self, dataset: HmdaDataset) -> bytes:
        url = urljoin(self.base_url, "csv")
        params = self._build_csv_params(dataset)

        response = requests.get(
            url,
            params=params,
            timeout=self.timeout_seconds,
            headers={
                "Accept": "text/csv",
                "User-Agent": "OneHavenMarketEngine/0.1",
            },
            stream=True,
        )

        if response.status_code == 400:
            raise ValueError(
                "HMDA API returned 400. Check year, geography, and filter parameters. "
                "The v2 Data Browser API currently supports 2018-2023 and may allow "
                "only two or fewer HMDA data filter criteria per CSV request. "
                f"URL={response.url} BODY={response.text[:1000]}"
            )

        if response.status_code == 404:
            raise ValueError(
                "HMDA API returned 404. The requested endpoint or data was not found. "
                f"URL={response.url}"
            )

        response.raise_for_status()

        content = response.content
        self._validate_csv_response(content=content, source_url=response.url)

        return content

    @staticmethod
    def _build_csv_params(dataset: HmdaDataset) -> dict[str, str]:
        if dataset.geography_type not in {"states", "msamds", "counties", "leis"}:
            raise ValueError(
                "Invalid HMDA_GEOGRAPHY_TYPE. Expected states, msamds, counties, or leis. "
                f"Got: {dataset.geography_type}"
            )

        params = {
            "years": str(dataset.year),
            dataset.geography_type: dataset.geography_values,
        }

        optional_filters = {
            "actions_taken": dataset.actions_taken,
            "loan_purposes": dataset.loan_purposes,
            "loan_types": dataset.loan_types,
            "lien_statuses": dataset.lien_statuses,
        }

        active_filter_count = 0

        for key, value in optional_filters.items():
            clean_value = value.strip() if value else ""

            if clean_value:
                params[key] = clean_value
                active_filter_count += 1

        if active_filter_count < 1:
            raise ValueError(
                "HMDA API requires at least one HMDA data filter. "
                "Set at least one of HMDA_ACTIONS_TAKEN, HMDA_LOAN_PURPOSES, "
                "HMDA_LOAN_TYPES, or HMDA_LIEN_STATUSES."
            )

        if active_filter_count > 2:
            raise ValueError(
                "HMDA Data Browser API allows two or fewer HMDA data filters for this request. "
                f"Active filter count={active_filter_count}. "
                "Clear some of HMDA_ACTIONS_TAKEN, HMDA_LOAN_PURPOSES, "
                "HMDA_LOAN_TYPES, or HMDA_LIEN_STATUSES."
            )

        return params

    @staticmethod
    def _validate_csv_response(content: bytes, source_url: str) -> None:
        if not content:
            raise ValueError(f"HMDA API returned an empty CSV. URL={source_url}")

        preview = content[:4000].decode("utf-8-sig", errors="ignore").lower()

        if "<html" in preview or "<!doctype html" in preview:
            raise ValueError(f"HMDA API returned HTML instead of CSV. URL={source_url}")

        if "errortype" in preview and "message" in preview:
            raise ValueError(f"HMDA API returned an error payload. URL={source_url} BODY={preview[:1000]}")

        looks_like_csv = b"," in content[:4000] or b"\n" in content[:4000]

        if not looks_like_csv:
            raise ValueError(f"Unexpected HMDA response. Expected CSV. URL={source_url}")
