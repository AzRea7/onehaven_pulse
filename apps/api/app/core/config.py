from functools import lru_cache

from pydantic import Field, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    app_name: str = "OneHaven Market Engine API"
    app_version: str = "0.1.0"
    environment: str = Field(default="local", validation_alias="ENVIRONMENT")
    log_level: str = Field(default="INFO", validation_alias="LOG_LEVEL")

    database_url: str = Field(
        default=(
            "postgresql+psycopg2://onehaven:onehaven_dev_password"
            "@localhost:5432/onehaven_market"
        ),
        validation_alias="DATABASE_URL",
    )

    frontend_origin: str = Field(
        default="http://localhost:3000",
        validation_alias="FRONTEND_ORIGIN",
    )

    # Keep this as string because pydantic-settings tries to JSON-decode list[str]
    # values from env. Comma-separated env vars are simpler and deployment-friendly.
    cors_allow_origins: str = Field(
        default="http://localhost:3000,http://127.0.0.1:3000",
        validation_alias="CORS_ALLOW_ORIGINS",
    )

    cors_allow_credentials: bool = Field(
        default=True,
        validation_alias="CORS_ALLOW_CREDENTIALS",
    )

    gzip_minimum_size: int = Field(default=1000, validation_alias="GZIP_MINIMUM_SIZE")
    api_request_timeout_seconds: int = Field(
        default=30,
        validation_alias="API_REQUEST_TIMEOUT_SECONDS",
    )

    db_pool_size: int = Field(default=5, validation_alias="DB_POOL_SIZE")
    db_max_overflow: int = Field(default=10, validation_alias="DB_MAX_OVERFLOW")
    db_pool_timeout: int = Field(default=30, validation_alias="DB_POOL_TIMEOUT")
    db_pool_recycle: int = Field(default=1800, validation_alias="DB_POOL_RECYCLE")

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    @model_validator(mode="after")
    def validate_production_safety(self) -> "Settings":
        environment = self.environment.strip().lower()

        if environment in {"production", "prod"}:
            if "*" in self.allowed_origins:
                raise ValueError("Wildcard CORS origins are not allowed in production.")

            local_origins = {
                "http://localhost:3000",
                "http://127.0.0.1:3000",
                "http://localhost:8000",
                "http://127.0.0.1:8000",
            }

            if any(origin in local_origins for origin in self.allowed_origins):
                raise ValueError("Localhost CORS origins are not allowed in production.")

            if "onehaven_dev_password" in self.database_url:
                raise ValueError("Development database password is not allowed in production.")

            if "@localhost:" in self.database_url or "@127.0.0.1:" in self.database_url:
                raise ValueError("Production DATABASE_URL must not point at localhost.")

        return self

    @property
    def allowed_origins(self) -> list[str]:
        origins = {
            origin.strip()
            for origin in self.cors_allow_origins.split(",")
            if origin.strip()
        }

        if self.frontend_origin:
            origins.add(self.frontend_origin.strip())

        return sorted(origins)

    @property
    def is_production(self) -> bool:
        return self.environment.strip().lower() in {"production", "prod"}


@lru_cache
def get_settings() -> Settings:
    return Settings()


settings = get_settings()
