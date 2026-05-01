from unittest.mock import Mock, patch

from pipelines.orchestration.raw_pipeline_runner import RawPipelineRunner


def _completed_process(returncode: int = 0, stdout: str = "ok", stderr: str = ""):
    completed = Mock()
    completed.returncode = returncode
    completed.stdout = stdout
    completed.stderr = stderr
    return completed


def test_raw_pipeline_runner_success(monkeypatch, tmp_path):
    from pipelines.common import settings as settings_module

    monkeypatch.setattr(settings_module.settings, "manifest_dir", tmp_path)

    with patch(
        "pipelines.orchestration.raw_pipeline_runner.subprocess.run",
        return_value=_completed_process(),
    ):
        runner = RawPipelineRunner()
        summary = runner.run(["fred"])

    assert summary.status == "success"
    assert summary.success_count == 1
    assert summary.failure_count == 0
    assert summary.results[0].name == "fred"
    assert summary.summary_path is not None


def test_raw_pipeline_runner_failure_stops(monkeypatch, tmp_path):
    from pipelines.common import settings as settings_module

    monkeypatch.setattr(settings_module.settings, "manifest_dir", tmp_path)

    with patch(
        "pipelines.orchestration.raw_pipeline_runner.subprocess.run",
        return_value=_completed_process(returncode=1, stderr="failed"),
    ):
        runner = RawPipelineRunner(stop_on_failure=True)
        summary = runner.run(["fred", "fema_nri"])

    assert summary.status == "failed"
    assert summary.success_count == 0
    assert summary.failure_count == 1
    assert len(summary.results) == 1
    assert summary.results[0].name == "fred"


def test_raw_pipeline_runner_continue_on_failure(monkeypatch, tmp_path):
    from pipelines.common import settings as settings_module

    monkeypatch.setattr(settings_module.settings, "manifest_dir", tmp_path)

    with patch(
        "pipelines.orchestration.raw_pipeline_runner.subprocess.run",
        side_effect=[
            _completed_process(returncode=1, stderr="failed"),
            _completed_process(returncode=0, stdout="ok"),
        ],
    ):
        runner = RawPipelineRunner(stop_on_failure=False)
        summary = runner.run(["fred", "fema_nri"])

    assert summary.status == "failed"
    assert summary.success_count == 1
    assert summary.failure_count == 1
    assert len(summary.results) == 2
    assert summary.results[0].name == "fred"
    assert summary.results[1].name == "fema_nri"
