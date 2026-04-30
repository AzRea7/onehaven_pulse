from pathlib import Path

from pydantic_settings import BaseSettings, SettingsConfigDict


class PipelineSettings(BaseSettings):
    project_root: Path = Path(__file__).resolve().parents[2]
    local_data_dir: Path = project_root / "data"
    raw_data_dir: Path = local_data_dir / "raw"
    manifest_dir: Path = local_data_dir / "manifests"
    storage_backend: str = "local"

    model_config = SettingsConfigDict(
        env_file=".env",
        extra="ignore",
    )


settings = PipelineSettings()
