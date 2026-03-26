from functools import lru_cache

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    database_url: str

    celery_broker_url: str
    celery_result_backend: str

    s3_endpoint_url: str
    s3_access_key: str
    s3_secret_key: str
    s3_bucket_name: str

    ingestion_interval_seconds: int

    tasks_api_url: str
    metrics_api_url: str


@lru_cache
def get_settings() -> Settings:
    """Возвращает кешированный экземпляр настроек приложения.

    Returns:
        Settings: Единственный экземпляр конфигурации, загруженный из .env файла.
    """
    return Settings()
