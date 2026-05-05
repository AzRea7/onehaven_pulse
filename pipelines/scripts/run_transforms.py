from __future__ import annotations

import argparse
import inspect

from pipelines.common.transform_options import build_transform_options
from pipelines.transforms.registry import transform_registry


def _get_transform_registry():
    for name in (
        "TRANSFORM_REGISTRY",
        "TRANSFORMS",
        "TRANSFORM_DEFINITIONS",
        "REGISTRY",
    ):
        value = getattr(transform_registry, name, None)

        if isinstance(value, dict):
            return value

    candidates = [
        name
        for name in dir(transform_registry)
        if name.isupper() and isinstance(getattr(transform_registry, name), dict)
    ]

    raise RuntimeError(
        "Could not find transform registry dict in "
        "pipelines.transforms.registry.transform_registry. "
        f"Uppercase dict candidates: {candidates}"
    )


def _definition_name(definition, fallback: str) -> str:
    return getattr(definition, "name", fallback)


def _definition_target_table(definition) -> str:
    return getattr(definition, "target_table", "")


def _definition_description(definition) -> str:
    return getattr(definition, "description", "")


def _definition_runner(definition):
    runner = getattr(definition, "runner", None)

    if runner is None:
        raise RuntimeError(f"Transform definition has no runner: {definition!r}")

    return runner


def list_transforms() -> None:
    registry = _get_transform_registry()

    for name, definition in sorted(registry.items()):
        print(
            f"{name}\t{_definition_target_table(definition)}\t{_definition_description(definition)}"
        )


def _run_transform(
    name: str,
    *,
    mode: str,
    start_date: str | None,
    recent_months: int | None,
) -> None:
    registry = _get_transform_registry()

    try:
        definition = registry[name]
    except KeyError as exc:
        allowed = ", ".join(sorted(registry))
        raise ValueError(f"Unknown transform '{name}'. Allowed transforms: {allowed}") from exc

    transform_name = _definition_name(definition, name)
    runner = _definition_runner(definition)

    print(f"Running transform: {transform_name}")

    options = build_transform_options(
        mode=mode,
        start_date=start_date,
        recent_months=recent_months,
    )

    runner_signature = inspect.signature(runner)

    if "options" in runner_signature.parameters:
        runner(options=options)
    else:
        if options.is_incremental:
            raise ValueError(
                f"Transform {transform_name} does not support incremental options yet."
            )

        runner()

    print(f"Finished transform: {transform_name}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Run OneHaven market transforms.")
    parser.add_argument("--list", action="store_true", help="List available transforms.")
    parser.add_argument(
        "--mode",
        choices=["full", "recent", "since"],
        default="full",
        help="Transform run mode. Defaults to full for backward compatibility.",
    )
    parser.add_argument(
        "--start-date",
        default=None,
        help="Inclusive YYYY-MM-DD period filter for --mode since.",
    )
    parser.add_argument(
        "--recent-months",
        type=int,
        default=None,
        help="Number of recent months to transform for --mode recent.",
    )
    parser.add_argument(
        "transforms",
        nargs="*",
        help="Transform names to run. If omitted, all transforms run.",
    )

    args = parser.parse_args()

    if args.list:
        list_transforms()
        return

    registry = _get_transform_registry()
    transform_names = args.transforms or sorted(registry)

    for name in transform_names:
        _run_transform(
            name,
            mode=args.mode,
            start_date=args.start_date,
            recent_months=args.recent_months,
        )


if __name__ == "__main__":
    main()
