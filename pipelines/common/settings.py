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

    census_year: int = 2025
    census_state_url: str = (
        "https://www2.census.gov/geo/tiger/GENZ2025/shp/cb_2025_us_state_500k.zip"
    )
    census_cbsa_url: str = (
        "https://www2.census.gov/geo/tiger/TIGER2025/CBSA/tl_2025_us_cbsa.zip"
    )

    census_bps_source_mode: str = "local"

    census_bps_state_monthly_local_path: str = (
        "data/import/census_building_permits/state_jan_2026.xls"
    )
    census_bps_cbsa_monthly_local_path: str = (
        "data/import/census_building_permits/cbsa_jan_2026.xls"
    )
    census_bps_state_annual_local_path: str = (
        "data/import/census_building_permits/state_annual_2025.xls"
    )
    census_bps_cbsa_annual_local_path: str = (
        "data/import/census_building_permits/cbsa_annual_2025.xls"
    )

    census_bps_state_monthly_url: str | None = None
    census_bps_cbsa_monthly_url: str | None = None
    census_bps_state_annual_url: str | None = None
    census_bps_cbsa_annual_url: str | None = None

    model_config = SettingsConfigDict(
        env_file=".env",
        extra="ignore",
    )


settings = PipelineSettings()
