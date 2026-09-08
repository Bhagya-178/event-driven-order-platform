from pydantic_settings import BaseSettings, SettingsConfigDict

class Settings(BaseSettings):
    PAYMENT_DB_URL: str = "sqlite+aiosqlite:///:memory:"
    PAYMENT_PORT: int = 8001
    LOG_LEVEL: str = "INFO"
    REDIS_URL: str = "redis://localhost:6379/0"
    ENABLE_RATE_LIMITING: bool = True
    KAFKA_BOOTSTRAP_SERVERS: str = "localhost:9092"

    model_config = SettingsConfigDict(
        env_file="../../.env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

settings = Settings()
