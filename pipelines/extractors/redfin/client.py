from pathlib import Path

import requests

from pipelines.common.settings import settings
from pipelines.extractors.redfin.config import RedfinDataset


class RedfinClient:
    def __init__(self, timeout_seconds: int = 90) -> None:
        self.timeout_seconds = timeout_seconds

    def get_dataset_content(self, dataset: RedfinDataset) -> bytes:
        if settings.redfin_source_mode == "local":
            return self._read_local_dataset(dataset)

        if settings.redfin_source_mode == "url":
            return self._download_dataset(dataset)

        raise ValueError(
            "Invalid REDFIN_SOURCE_MODE. Expected 'local' or 'url'. "
            f"Got: {settings.redfin_source_mode}"
        )

    def _read_local_dataset(self, dataset: RedfinDataset) -> bytes:
        if not dataset.local_path:
            raise ValueError(f"Missing local path for Redfin dataset={dataset.dataset}")

        path = Path(dataset.local_path)

        if not path.is_absolute():
            path = settings.project_root / path

        if not path.exists():
            raise FileNotFoundError(
                f"Redfin local file does not exist for dataset={dataset.dataset}: {path}"
            )

        content = path.read_bytes()
        self._validate_csv_content(dataset=dataset, content=content, source_hint=str(path))

        return content

    def _download_dataset(self, dataset: RedfinDataset) -> bytes:
        if not dataset.url or dataset.url.startswith("replace_with_"):
            raise ValueError(
                f"Missing Redfin URL for dataset={dataset.dataset}. "
                "Set REDFIN_MARKET_TRACKER_URL in .env if using URL mode."
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
    def _validate_csv_content(dataset: RedfinDataset, content: bytes, source_hint: str) -> None:
        if not content:
            raise ValueError(f"Redfin response was empty for dataset={dataset.dataset}")

        header_preview = content[:5000].decode("utf-8-sig", errors="ignore").lower()

        if "<html" in header_preview or "<!doctype html" in header_preview:
            raise ValueError(
                f"Unexpected Redfin HTML response for dataset={dataset.dataset}. "
                f"Source={source_hint}"
            )

        if "," not in header_preview:
            raise ValueError(
                f"Unexpected Redfin CSV for dataset={dataset.dataset}. "
                f"No comma-delimited header detected. Source={source_hint}"
            )
