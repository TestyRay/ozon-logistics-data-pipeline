from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    kafka_bootstrap: str = "redpanda:9092"
    kafka_topic_postings: str = "ozon.postings"
    kafka_topic_products: str = "ozon.products"
    kafka_group_id: str = "shr-consumer"

    clickhouse_host: str = "clickhouse"
    clickhouse_port: int = 8123
    clickhouse_db: str = "ozon"
    clickhouse_user: str = "default"
    clickhouse_password: str = ""

    batch_size: int = 500
    batch_timeout_ms: int = 2000

    model_config = SettingsConfigDict(env_file=".env", case_sensitive=False, extra="ignore")


settings = Settings()