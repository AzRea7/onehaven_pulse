from fastapi.testclient import TestClient

from app.main import app

client = TestClient(app)


def test_admin_pipeline_runs_contract():
    response = client.get("/admin/pipeline-runs?limit=10")

    assert response.status_code == 200

    payload = response.json()

    assert set(payload.keys()) == {"summary", "items"}

    assert {
        "total",
        "running",
        "success",
        "failed",
        "other",
        "records_extracted",
        "records_loaded",
        "records_failed",
        "unmatched_count",
        "latest_started_at",
    } <= set(payload["summary"].keys())

    assert isinstance(payload["items"], list)

    if not payload["items"]:
        return

    item = payload["items"][0]

    assert {
        "id",
        "pipeline_name",
        "source",
        "dataset",
        "status",
        "started_at",
        "finished_at",
        "duration_seconds",
        "records_extracted",
        "records_loaded",
        "records_failed",
        "unmatched_count",
        "error_message",
        "metadata_json",
    } <= set(item.keys())


def test_admin_pipeline_runs_summary_contract():
    response = client.get("/admin/pipeline-runs/summary")

    assert response.status_code == 200

    payload = response.json()

    assert {
        "total",
        "running",
        "success",
        "failed",
        "other",
        "records_extracted",
        "records_loaded",
        "records_failed",
        "unmatched_count",
        "latest_started_at",
    } <= set(payload.keys())


def test_admin_pipeline_run_detail_404_contract():
    response = client.get("/admin/pipeline-runs/run_does_not_exist")

    assert response.status_code == 404

    payload = response.json()

    assert payload["error"]["code"] == "pipeline_run_not_found"
    assert payload["error"]["request_id"] is not None
