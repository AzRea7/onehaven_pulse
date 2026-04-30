import requests

from pipelines.extractors.fhfa.config import FhfaDataset


class FhfaClient:
    def __init__(self, timeout_seconds: int = 60) -> None:
        self.timeout_seconds = timeout_seconds

    def download_dataset(self, dataset: FhfaDataset) -> bytes:
        response = requests.get(
            dataset.url,
            timeout=self.timeout_seconds,
            headers={
                "User-Agent": "OneHavenMarketEngine/0.1"
            },
        )
        response.raise_for_status()

        content_type = response.headers.get("content-type", "").lower()
        content = response.content

        if not content:
            raise ValueError(f"FHFA response was empty for dataset={dataset.dataset}")

        if b"hpi_type" not in content[:500].lower() and "csv" not in content_type:
            raise ValueError(
                f"Unexpected FHFA response for dataset={dataset.dataset}. "
                f"content_type={content_type}"
            )

        return content
