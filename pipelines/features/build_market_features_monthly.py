from __future__ import annotations

import subprocess
from pathlib import Path


SQL_PATH = Path("pipelines/sql/story_11_1_market_features_monthly.sql")


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
        "-v",
        "ON_ERROR_STOP=1",
        "-U",
        "onehaven",
        "-d",
        "onehaven_market",
        "-f",
        "-",
    ]

    result = subprocess.run(
        command,
        input=SQL_PATH.read_text(encoding="utf-8"),
        text=True,
        check=False,
    )

    if result.returncode != 0:
        raise SystemExit(result.returncode)

    print("Built analytics.market_features_monthly feature_version=v1")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
