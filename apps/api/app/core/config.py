from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    database_url: str = (
        "postgresql+psycopg2://onehaven:onehaven_dev_password"
        "@localhost:5432/onehaven_market"
    )
    frontend_origin: str = "http://localhost:3000"

    class Config:
        env_file = ".env"
        extra = "ignore"


settings = Settings()
