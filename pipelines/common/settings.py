from pathlib import Path

from pydantic import AliasChoices, Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class PipelineSettings(BaseSettings):
    project_root: Path = Path(__file__).resolve().parents[2]
    local_data_dir: Path = project_root / "data"
    raw_data_dir: Path = local_data_dir / "raw"
    manifest_dir: Path = local_data_dir / "manifests"
    storage_backend: str = "local"

    database_url: str = Field(
        default=(
            "postgresql+psycopg2://onehaven:onehaven_dev_password"
            "@localhost:5432/onehaven_market"
        ),
        validation_alias="DATABASE_URL",
    )

    fred_api_key: str | None = Field(default=None, validation_alias="FRED_API_KEY")
    fred_base_url: str = Field(
        default="https://api.stlouisfed.org/fred",
        validation_alias="FRED_BASE_URL",
    )

    zillow_source_mode: str = Field(default="local", validation_alias="ZILLOW_SOURCE_MODE")
    zillow_zhvi_local_path: str = Field(
        default="data/import/zillow/zhvi.csv",
        validation_alias="ZILLOW_ZHVI_LOCAL_PATH",
    )
    zillow_zori_local_path: str = Field(
        default="data/import/zillow/zori.csv",
        validation_alias="ZILLOW_ZORI_LOCAL_PATH",
    )
    zillow_zhvi_url: str | None = Field(default=None, validation_alias="ZILLOW_ZHVI_URL")
    zillow_zori_url: str | None = Field(default=None, validation_alias="ZILLOW_ZORI_URL")

    redfin_source_mode: str = Field(default="local", validation_alias="REDFIN_SOURCE_MODE")
    redfin_market_tracker_local_path: str = Field(
        default="data/import/redfin/market_tracker.csv",
        validation_alias="REDFIN_MARKET_TRACKER_LOCAL_PATH",
    )
    redfin_market_tracker_url: str | None = Field(
        default=None,
        validation_alias="REDFIN_MARKET_TRACKER_URL",
    )

    census_year: int = Field(default=2025, validation_alias="CENSUS_YEAR")
    census_state_url: str = Field(
        default="https://www2.census.gov/geo/tiger/GENZ2025/shp/cb_2025_us_state_500k.zip",
        validation_alias="CENSUS_STATE_URL",
    )
    census_cbsa_url: str = Field(
        default="https://www2.census.gov/geo/tiger/TIGER2025/CBSA/tl_2025_us_cbsa.zip",
        validation_alias="CENSUS_CBSA_URL",
    )

    census_bps_source_mode: str = Field(
        default="local",
        validation_alias="CENSUS_BPS_SOURCE_MODE",
    )
    census_bps_state_monthly_local_path: str = Field(
        default="data/import/census_building_permits/state_jan_2026.xls",
        validation_alias="CENSUS_BPS_STATE_MONTHLY_LOCAL_PATH",
    )
    census_bps_cbsa_monthly_local_path: str = Field(
        default="data/import/census_building_permits/cbsa_jan_2026.xls",
        validation_alias="CENSUS_BPS_CBSA_MONTHLY_LOCAL_PATH",
    )
    census_bps_state_annual_local_path: str = Field(
        default="data/import/census_building_permits/state_annual_2025.xls",
        validation_alias="CENSUS_BPS_STATE_ANNUAL_LOCAL_PATH",
    )
    census_bps_cbsa_annual_local_path: str = Field(
        default="data/import/census_building_permits/cbsa_annual_2025.xls",
        validation_alias="CENSUS_BPS_CBSA_ANNUAL_LOCAL_PATH",
    )
    census_bps_state_monthly_url: str | None = Field(
        default=None,
        validation_alias="CENSUS_BPS_STATE_MONTHLY_URL",
    )
    census_bps_cbsa_monthly_url: str | None = Field(
        default=None,
        validation_alias="CENSUS_BPS_CBSA_MONTHLY_URL",
    )
    census_bps_state_annual_url: str | None = Field(
        default=None,
        validation_alias="CENSUS_BPS_STATE_ANNUAL_URL",
    )
    census_bps_cbsa_annual_url: str | None = Field(
        default=None,
        validation_alias="CENSUS_BPS_CBSA_ANNUAL_URL",
    )

    census_acs_year: int = Field(default=2024, validation_alias="CENSUS_ACS_YEAR")
    census_acs_base_url: str = Field(
        default="https://api.census.gov/data",
        validation_alias="CENSUS_ACS_BASE_URL",
    )
    census_data_api_key: str | None = Field(
        default=None,
        validation_alias=AliasChoices(
            "CENSUS_DATA_API_KEY",
            "census_data_api_key",
            "CENSUS_API_KEY",
            "census_api_key",
        ),
    )

    hud_usps_api_base_url: str = Field(
        default="https://www.huduser.gov/hudapi/public/usps",
        validation_alias="HUD_USPS_API_BASE_URL",
    )
    hud_usps_access_token: str | None = Field(
        default=None,
        validation_alias="HUD_USPS_ACCESS_TOKEN",
    )
    hud_usps_year: int = Field(default=2025, validation_alias="HUD_USPS_YEAR")
    hud_usps_quarter: int = Field(default=4, validation_alias="HUD_USPS_QUARTER")
    hud_usps_query: str = Field(default="All", validation_alias="HUD_USPS_QUERY")

    @field_validator("census_data_api_key")
    @classmethod
    def strip_census_data_api_key(cls, value: str | None) -> str | None:
        if value is None:
            return None

        clean = value.strip()

        if not clean:
            return None

        if clean.lower().startswith("key="):
            clean = clean.split("=", 1)[1].strip()

        if clean.startswith("replace_with_"):
            return None

        return clean

    @field_validator("hud_usps_access_token")
    @classmethod
    def strip_hud_usps_access_token(cls, value: str | None) -> str | None:
        if value is None:
            return None

        clean = value.strip()

        if not clean:
            return None

        if clean.lower().startswith("bearer "):
            clean = clean.split(" ", 1)[1].strip()

        if clean.startswith("replace_with_"):
            return None

        return clean

    model_config = SettingsConfigDict(
        env_file=".env",
        extra="ignore",
    )


settings = PipelineSettings()
