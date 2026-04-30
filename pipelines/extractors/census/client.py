import requests

from pipelines.extractors.census.config import CensusGeographyDataset


class CensusClient:
    def __init__(self, timeout_seconds: int = 120) -> None:
        self.timeout_seconds = timeout_seconds

    def download_dataset(self, dataset: CensusGeographyDataset) -> bytes:
        response = requests.get(
            dataset.url,
            timeout=self.timeout_seconds,
            headers={
                "User-Agent": "OneHavenMarketEngine/0.1",
            },
        )
        response.raise_for_status()

        content = response.content

        if not content:
            raise ValueError(f"Census response was empty for dataset={dataset.dataset}")

        # ZIP magic bytes: PK
        if not content.startswith(b"PK"):
            preview = content[:200].decode("utf-8", errors="ignore")
            raise ValueError(
                f"Unexpected Census response for dataset={dataset.dataset}. "
                f"Expected ZIP bytes. Preview={preview}"
            )

        return content
