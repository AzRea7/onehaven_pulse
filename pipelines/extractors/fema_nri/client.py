from typing import Any

import requests

from pipelines.extractors.fema_nri.config import FemaNriDataset


ARCGIS_ITEM_URL = "https://www.arcgis.com/sharing/rest/content/items/{item_id}"


class FemaNriClient:
    def __init__(self, timeout_seconds: int = 180) -> None:
        self.timeout_seconds = timeout_seconds

    def get_dataset(self, dataset: FemaNriDataset) -> dict[str, Any]:
        if dataset.source_mode != "arcgis":
            raise ValueError(
                "Invalid FEMA_NRI_SOURCE_MODE for this extractor. Expected 'arcgis'. "
                f"Got: {dataset.source_mode}"
            )

        service_url, item_metadata = self._resolve_service_url(dataset)
        layer_url = self._layer_url(service_url=service_url, layer_id=dataset.arcgis_layer_id)
        layer_metadata = self._get_layer_metadata(layer_url)

        page_size = self._effective_page_size(
            configured_page_size=dataset.arcgis_page_size,
            layer_metadata=layer_metadata,
        )

        all_records: list[dict[str, Any]] = []
        pages: list[dict[str, Any]] = []
        offset = 0
        page_number = 1

        while True:
            payload, request_url = self._query_layer_page(
                layer_url=layer_url,
                where=dataset.arcgis_where,
                out_fields=dataset.arcgis_out_fields,
                page_size=page_size,
                offset=offset,
            )

            features = payload.get("features", [])

            if not isinstance(features, list):
                raise ValueError("Unexpected ArcGIS response: features is not a list.")

            records = []

            for feature in features:
                if isinstance(feature, dict):
                    attributes = feature.get("attributes", {})
                    if isinstance(attributes, dict):
                        records.append(attributes)

            all_records.extend(records)

            pages.append(
                {
                    "page_number": page_number,
                    "result_offset": offset,
                    "result_record_count": page_size,
                    "record_count": len(records),
                    "exceeded_transfer_limit": payload.get("exceededTransferLimit"),
                    "url": request_url,
                }
            )

            if len(records) < page_size:
                break

            offset += page_size
            page_number += 1

        return {
            "request": {
                "source_mode": dataset.source_mode,
                "arcgis_item_id": dataset.arcgis_item_id,
                "service_url": service_url,
                "layer_url": layer_url,
                "layer_id": dataset.arcgis_layer_id,
                "page_size": page_size,
                "where": dataset.arcgis_where,
                "out_fields": dataset.arcgis_out_fields,
                "return_geometry": False,
                "api_key_required": False,
            },
            "item_metadata": item_metadata,
            "layer_metadata": layer_metadata,
            "page_count": len(pages),
            "record_count": len(all_records),
            "pages": pages,
            "records": all_records,
        }

    def _resolve_service_url(self, dataset: FemaNriDataset) -> tuple[str, dict[str, Any]]:
        if dataset.arcgis_service_url.strip():
            return dataset.arcgis_service_url.rstrip("/"), {}

        item_url = ARCGIS_ITEM_URL.format(item_id=dataset.arcgis_item_id)

        response = requests.get(
            item_url,
            params={"f": "json"},
            timeout=self.timeout_seconds,
            headers={"User-Agent": "OneHavenMarketEngine/0.1"},
        )

        response.raise_for_status()
        payload = response.json()

        if payload.get("error"):
            raise ValueError(f"ArcGIS item metadata error: {payload['error']}")

        service_url = payload.get("url")

        if not service_url:
            raise ValueError(
                "ArcGIS item metadata did not include a service URL. "
                "Set FEMA_NRI_ARCGIS_SERVICE_URL manually."
            )

        return service_url.rstrip("/"), payload

    def _get_layer_metadata(self, layer_url: str) -> dict[str, Any]:
        response = requests.get(
            layer_url,
            params={"f": "json"},
            timeout=self.timeout_seconds,
            headers={"User-Agent": "OneHavenMarketEngine/0.1"},
        )

        response.raise_for_status()
        payload = response.json()

        if payload.get("error"):
            raise ValueError(f"ArcGIS layer metadata error: {payload['error']}")

        return payload

    def _query_layer_page(
        self,
        layer_url: str,
        where: str,
        out_fields: str,
        page_size: int,
        offset: int,
    ) -> tuple[dict[str, Any], str]:
        params = {
            "f": "json",
            "where": where,
            "outFields": out_fields,
            "returnGeometry": "false",
            "resultOffset": str(offset),
            "resultRecordCount": str(page_size),
        }

        response = requests.get(
            f"{layer_url}/query",
            params=params,
            timeout=self.timeout_seconds,
            headers={"User-Agent": "OneHavenMarketEngine/0.1"},
        )

        response.raise_for_status()
        payload = response.json()

        if payload.get("error"):
            raise ValueError(f"ArcGIS query error: {payload['error']}")

        return payload, response.url

    @staticmethod
    def _layer_url(service_url: str, layer_id: int) -> str:
        return f"{service_url.rstrip('/')}/{layer_id}"

    @staticmethod
    def _effective_page_size(
        configured_page_size: int,
        layer_metadata: dict[str, Any],
    ) -> int:
        if configured_page_size < 1:
            raise ValueError("FEMA_NRI_ARCGIS_PAGE_SIZE must be at least 1.")

        max_record_count = layer_metadata.get("maxRecordCount")

        if isinstance(max_record_count, int) and max_record_count > 0:
            return min(configured_page_size, max_record_count)

        return configured_page_size
