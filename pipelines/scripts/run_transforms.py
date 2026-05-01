import argparse

from pipelines.transforms.registry.transform_registry import (
    get_transform_definition,
    list_transform_names,
    resolve_transform_names,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run OneHaven market transforms.")
    parser.add_argument(
        "transforms",
        nargs="*",
        default=["all"],
        help="Transform names to run. Use 'all' to run every registered transform.",
    )
    parser.add_argument(
        "--list",
        action="store_true",
        help="List registered transforms and exit.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    if args.list:
        for name in list_transform_names():
            definition = get_transform_definition(name)
            print(f"{definition.name}\t{definition.target_table}\t{definition.description}")
        return

    transform_names = resolve_transform_names(args.transforms)

    for name in transform_names:
        definition = get_transform_definition(name)
        print(f"Running transform: {definition.name}")
        definition.runner()
        print(f"Finished transform: {definition.name}")


if __name__ == "__main__":
    main()
