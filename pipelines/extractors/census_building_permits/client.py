from pathlib import Path

import requests

from pipelines.common.settings import settings
from pipelines.extractors.census_building_permits.config import CensusBpsDataset


class CensusBpsClient:
    def __init__(self, timeout_seconds: int = 90) -> None:
        self.timeout_seconds = timeout_seconds

    def get_dataset_content(self, dataset: CensusBpsDataset) -> bytes | None:
        if settings.census_bps_source_mode == "local":
            return self._read_local_dataset(dataset)

        if settings.census_bps_source_mode == "url":
            return self._download_dataset(dataset)

        raise ValueError(
            "Invalid CENSUS_BPS_SOURCE_MODE. Expected 'local' or 'url'. "
            f"Got: {settings.census_bps_source_mode}"
        )

    def _read_local_dataset(self, dataset: CensusBpsDataset) -> bytes | None:
        if not dataset.local_path:
            if dataset.required:
                raise ValueError(
                    f"Missing local path for required Census BPS file: {dataset.filename}"
                )
            return None

        path = Path(dataset.local_path)

        if not path.is_absolute():
            path = settings.project_root / path

        if not path.exists():
            if dataset.required:
                raise FileNotFoundError(
                    "Required Census BPS local file does not exist for "
                    f"{dataset.geography_level}/{dataset.period_type}: {path}"
                )
            print(
                "Skipping optional Census BPS file because it does not exist: "
                f"{dataset.geography_level}/{dataset.period_type} -> {path}"
            )
            return None

        content = path.read_bytes()
        self._validate_file_content(dataset=dataset, content=content, source_hint=str(path))

        return content

    def _download_dataset(self, dataset: CensusBpsDataset) -> bytes | None:
        if not dataset.url or dataset.url.startswith("replace_with_"):
            if dataset.required:
                raise ValueError(
                    "Missing Census BPS URL for required file "
                    f"{dataset.geography_level}/{dataset.period_type}."
                )
            return None

        response = requests.get(
            dataset.url,
            timeout=self.timeout_seconds,
            headers={
                "User-Agent": "OneHavenMarketEngine/0.1",
            },
        )
        response.raise_for_status()

        content = response.content
        self._validate_file_content(dataset=dataset, content=content, source_hint=dataset.url)

        return content

    @staticmethod
    def _validate_file_content(
        dataset: CensusBpsDataset,
        content: bytes,
        source_hint: str,
    ) -> None:
        if not content:
            raise ValueError(
                "Census BPS file was empty for "
                f"{dataset.geography_level}/{dataset.period_type}"
            )

        preview = content[:1000].decode("utf-8", errors="ignore").lower()

        if "<html" in preview or "<!doctype html" in preview:
            raise ValueError(
                "Unexpected Census BPS HTML response for "
                f"{dataset.geography_level}/{dataset.period_type}. Source={source_hint}"
            )

        looks_like_xlsx = content.startswith(b"PK")
        looks_like_xls = content.startswith(b"\xd0\xcf\x11\xe0\xa1\xb1\x1a\xe1")
        looks_like_csv_or_tsv = b"," in content[:4000] or b"\t" in content[:4000]

        if not looks_like_xlsx and not looks_like_xls and not looks_like_csv_or_tsv:
            raise ValueError(
                "Unexpected Census BPS file content for "
                f"{dataset.geography_level}/{dataset.period_type}. "
                f"Expected XLS, XLSX, CSV, or TSV. Source={source_hint}"
            )
