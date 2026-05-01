import argparse
import sys

from pipelines.orchestration.raw_pipeline_registry import (
    RAW_PIPELINES,
    resolve_pipeline_names,
)
from pipelines.orchestration.raw_pipeline_runner import RawPipelineRunner


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Run OneHaven raw data extractors.",
    )

    parser.add_argument(
        "pipelines",
        nargs="*",
        help=(
            "Pipeline names to run. Use 'all' or omit to run all. "
            "Use --list to show available names."
        ),
    )

    parser.add_argument(
        "--list",
        action="store_true",
        help="List available raw pipelines and exit.",
    )

    parser.add_argument(
        "--continue-on-failure",
        action="store_true",
        help="Continue running later extractors after a failure.",
    )

    return parser


def print_pipeline_list() -> None:
    print("Available raw pipelines:")

    for pipeline in RAW_PIPELINES:
        heavy_label = " heavy" if pipeline.is_heavy else ""
        key_label = " requires_key" if pipeline.requires_api_key else ""
        print(
            f"- {pipeline.name}"
            f" [{pipeline.source}/{pipeline.dataset}{heavy_label}{key_label}]"
            f" {pipeline.description}"
        )


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()

    if args.list:
        print_pipeline_list()
        return 0

    try:
        requested = resolve_pipeline_names(args.pipelines)
    except ValueError as exc:
        print(str(exc), file=sys.stderr)
        return 2

    runner = RawPipelineRunner(
        stop_on_failure=not args.continue_on_failure,
    )

    print("Running raw pipelines:")
    for name in requested:
        print(f"- {name}")

    summary = runner.run(requested)

    print()
    print(f"Raw pipeline run status: {summary.status}")
    print(f"Success count: {summary.success_count}")
    print(f"Failure count: {summary.failure_count}")
    print(f"Summary path: {summary.summary_path}")

    print()
    print("Results:")

    for result in summary.results:
        print(
            f"- {result.name}: {result.status} "
            f"return_code={result.return_code} "
            f"duration_seconds={result.duration_seconds}"
        )

        if result.status == "failed":
            if result.stderr_preview:
                print("  stderr preview:")
                print(result.stderr_preview)

            if result.stdout_preview:
                print("  stdout preview:")
                print(result.stdout_preview)

    return 0 if summary.status == "success" else 1


if __name__ == "__main__":
    raise SystemExit(main())
