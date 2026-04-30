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

    zillow_source_mode: str = "local"
    zillow_zhvi_local_path: str = "data/import/zillow/zhvi.csv"
    zillow_zori_local_path: str = "data/import/zillow/zori.csv"
    zillow_zhvi_url: str | None = None
    zillow_zori_url: str | None = None

    redfin_source_mode: str = "local"
    redfin_market_tracker_local_path: str = "data/import/redfin/market_tracker.csv"
    redfin_market_tracker_url: str | None = None

    model_config = SettingsConfigDict(
        env_file=".env",
        extra="ignore",
    )


settings = PipelineSettings()
