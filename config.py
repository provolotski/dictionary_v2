"""
Просто конф
"""
from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    log_level: str = "INFO"
    log_file: str = "dict.log"
    log_format: str ="%(asctime)s %(name)-30s %(levelname)-8s %(message)s"
    log_date: str = "%Y-%m-%d %H:%M:%S"

    postgres_user: str = "postgres"
    postgres_password: str = "postgres"
    postgres_host: str = "127.0.0.1"
    postgres_port: str = "5432"
    postgres_schema: str = "postgres"

    class Config:
        env_file = ".env"


settings = Settings()
