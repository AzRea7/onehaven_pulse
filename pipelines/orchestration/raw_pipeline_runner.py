import json
import os
import subprocess
import sys
import time
from dataclasses import asdict, dataclass
from datetime import UTC, datetime

from pipelines.common.settings import settings
from pipelines.orchestration.raw_pipeline_registry import (
    RawPipelineDefinition,
    get_pipeline_definition,
    resolve_pipeline_names,
)


@dataclass(frozen=True)
class RawPipelineRunResult:
    name: str
    module: str
    source: str
    dataset: str
    status: str
    started_at: str
    finished_at: str
    duration_seconds: float
    return_code: int
    stdout_preview: str
    stderr_preview: str


@dataclass(frozen=True)
class RawPipelineRunSummary:
    run_id: str
    status: str
    started_at: str
    finished_at: str
    duration_seconds: float
    requested_pipelines: list[str]
    stop_on_failure: bool
    success_count: int
    failure_count: int
    results: list[RawPipelineRunResult]
    summary_path: str | None = None


class RawPipelineRunner:
    def __init__(
        self,
        stop_on_failure: bool = True,
        stdout_preview_chars: int = 4000,
        stderr_preview_chars: int = 4000,
    ) -> None:
        self.stop_on_failure = stop_on_failure
        self.stdout_preview_chars = stdout_preview_chars
        self.stderr_preview_chars = stderr_preview_chars

    def run(self, pipeline_names: list[str] | None = None) -> RawPipelineRunSummary:
        requested = resolve_pipeline_names(pipeline_names)
        run_id = self._new_run_id()
        started_at_dt = datetime.now(UTC)
        results: list[RawPipelineRunResult] = []

        for name in requested:
            definition = get_pipeline_definition(name)
            result = self._run_one(definition)
            results.append(result)

            if result.status == "failed" and self.stop_on_failure:
                break

        finished_at_dt = datetime.now(UTC)
        failure_count = sum(1 for result in results if result.status == "failed")
        success_count = sum(1 for result in results if result.status == "success")

        summary_status = "success" if failure_count == 0 else "failed"

        summary_without_path = RawPipelineRunSummary(
            run_id=run_id,
            status=summary_status,
            started_at=started_at_dt.isoformat(),
            finished_at=finished_at_dt.isoformat(),
            duration_seconds=round(
                (finished_at_dt - started_at_dt).total_seconds(),
                3,
            ),
            requested_pipelines=requested,
            stop_on_failure=self.stop_on_failure,
            success_count=success_count,
            failure_count=failure_count,
            results=results,
            summary_path=None,
        )

        summary_path = self.write_summary(summary_without_path)

        return RawPipelineRunSummary(
            run_id=summary_without_path.run_id,
            status=summary_without_path.status,
            started_at=summary_without_path.started_at,
            finished_at=summary_without_path.finished_at,
            duration_seconds=summary_without_path.duration_seconds,
            requested_pipelines=summary_without_path.requested_pipelines,
            stop_on_failure=summary_without_path.stop_on_failure,
            success_count=summary_without_path.success_count,
            failure_count=summary_without_path.failure_count,
            results=summary_without_path.results,
            summary_path=summary_path,
        )

    def _run_one(self, definition: RawPipelineDefinition) -> RawPipelineRunResult:
        started_at_dt = datetime.now(UTC)
        started_monotonic = time.monotonic()

        env = os.environ.copy()
        env["PYTHONPATH"] = "."

        command = [
            sys.executable,
            "-m",
            definition.module,
        ]

        completed = subprocess.run(
            command,
            check=False,
            capture_output=True,
            text=True,
            env=env,
        )

        finished_at_dt = datetime.now(UTC)
        duration_seconds = round(time.monotonic() - started_monotonic, 3)

        status = "success" if completed.returncode == 0 else "failed"

        return RawPipelineRunResult(
            name=definition.name,
            module=definition.module,
            source=definition.source,
            dataset=definition.dataset,
            status=status,
            started_at=started_at_dt.isoformat(),
            finished_at=finished_at_dt.isoformat(),
            duration_seconds=duration_seconds,
            return_code=completed.returncode,
            stdout_preview=completed.stdout[-self.stdout_preview_chars :],
            stderr_preview=completed.stderr[-self.stderr_preview_chars :],
        )

    def write_summary(self, summary: RawPipelineRunSummary) -> str:
        summary_dir = settings.manifest_dir / "orchestration" / "raw_pipeline_runs"
        summary_dir.mkdir(parents=True, exist_ok=True)

        summary_path = summary_dir / f"{summary.run_id}.json"

        payload = asdict(summary)

        summary_path.write_text(
            json.dumps(payload, indent=2, sort_keys=False),
            encoding="utf-8",
        )

        return str(summary_path)

    @staticmethod
    def _new_run_id() -> str:
        timestamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
        return f"raw_pipeline_run_{timestamp}"
