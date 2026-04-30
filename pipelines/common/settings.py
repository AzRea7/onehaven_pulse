from pathlib import Path

from pydantic_settings import BaseSettings, SettingsConfigDict


class PipelineSettings(BaseSettings):
    project_root: Path = Path(__file__).resolve().parents[2]
    local_data_dir: Path = project_root / "data"
    raw_data_dir: Path = local_data_dir / "raw"
    manifest_dir: Path = local_data_dir / "manifests"
    storage_backend: str = "local"

    database_url: str = (
        "postgresql+psycopg2://onehaven:onehaven_dev_password"
        "@localhost:5432/onehaven_market"
    )

    fred_api_key: str | None = None
    fred_base_url: str = "https://api.stlouisfed.org/fred"

    model_config = SettingsConfigDict(
        env_file=".env",
        extra="ignore",
    )


settings = PipelineSettings()
