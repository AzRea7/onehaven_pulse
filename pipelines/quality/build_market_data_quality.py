from __future__ import annotations

import subprocess
import sys
from pathlib import Path


SQL_PATH = Path("pipelines/sql/build_market_data_quality.sql")


def main() -> int:
    if not SQL_PATH.exists():
        raise SystemExit(f"Missing SQL file: {SQL_PATH}")

    command = [
        "docker",
        "compose",
        "exec",
        "-T",
        "postgres",
        "psql",
        "-U",
        "onehaven",
        "-d",
        "onehaven_market",
    ]

    with SQL_PATH.open("rb") as file:
        result = subprocess.run(command, stdin=file)

    return result.returncode


if __name__ == "__main__":
    raise SystemExit(main())
