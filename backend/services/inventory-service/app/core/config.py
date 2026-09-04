from pydantic_settings import BaseSettings, SettingsConfigDict

class Settings(BaseSettings):
    INVENTORY_DB_URL: str
    INVENTORY_PORT: int = 8003
    LOG_LEVEL: str = "INFO"
    REDIS_URL: str = "redis://localhost:6379/0"
    ENABLE_RATE_LIMITING: bool = True

    model_config = SettingsConfigDict(
        env_file="../../.env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

settings = Settings()
