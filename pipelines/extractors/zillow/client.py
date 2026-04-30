from pathlib import Path

import requests

from pipelines.common.settings import settings
from pipelines.extractors.zillow.config import ZillowDataset


class ZillowClient:
    def __init__(self, timeout_seconds: int = 60) -> None:
        self.timeout_seconds = timeout_seconds

    def get_dataset_content(self, dataset: ZillowDataset) -> bytes:
        if settings.zillow_source_mode == "local":
            return self._read_local_dataset(dataset)

        if settings.zillow_source_mode == "url":
            return self._download_dataset(dataset)

        raise ValueError(
            "Invalid ZILLOW_SOURCE_MODE. Expected 'local' or 'url'. "
            f"Got: {settings.zillow_source_mode}"
        )

    def _read_local_dataset(self, dataset: ZillowDataset) -> bytes:
        if not dataset.local_path:
            raise ValueError(f"Missing local path for Zillow dataset={dataset.dataset}")

        path = Path(dataset.local_path)

        if not path.is_absolute():
            path = settings.project_root / path

        if not path.exists():
            raise FileNotFoundError(
                f"Zillow local file does not exist for dataset={dataset.dataset}: {path}"
            )

        content = path.read_bytes()
        self._validate_csv_content(dataset=dataset, content=content, source_hint=str(path))

        return content

    def _download_dataset(self, dataset: ZillowDataset) -> bytes:
        if not dataset.url or dataset.url.startswith("replace_with_"):
            raise ValueError(
                f"Missing Zillow URL for dataset={dataset.dataset}. "
                "Set ZILLOW_ZHVI_URL and ZILLOW_ZORI_URL in .env."
            )

        response = requests.get(
            dataset.url,
            timeout=self.timeout_seconds,
            headers={
                "User-Agent": "OneHavenMarketEngine/0.1",
            },
        )
        response.raise_for_status()

        content = response.content
        self._validate_csv_content(dataset=dataset, content=content, source_hint=dataset.url)

        return content

    @staticmethod
    def _validate_csv_content(dataset: ZillowDataset, content: bytes, source_hint: str) -> None:
        if not content:
            raise ValueError(f"Zillow response was empty for dataset={dataset.dataset}")

        header_preview = content[:2000].decode("utf-8-sig", errors="ignore").lower()

        required_tokens = [
            "regionid",
            "regionname",
        ]

        missing_tokens = [token for token in required_tokens if token not in header_preview]

        if missing_tokens:
            raise ValueError(
                f"Unexpected Zillow CSV for dataset={dataset.dataset}. "
                f"Missing header tokens={missing_tokens}. Source={source_hint}"
            )
