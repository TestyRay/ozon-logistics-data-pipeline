from __future__ import annotations

import json
from pathlib import Path

from pydantic import BaseModel, Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Account(BaseModel):
    name: str = Field(min_length=1)
    client_id: str = Field(min_length=1)
    api_key: str = Field(min_length=1)


class Settings(BaseSettings):
    ozon_accounts_file: str = "/app/accounts.json"
    ozon_base_url: str = "https://api-seller.ozon.ru"

    poll_interval_seconds: int = 300
    initial_lookback_days: int = 7
    products_refresh_interval_seconds: int = 86400

    kafka_bootstrap: str = "redpanda:9092"
    kafka_topic_postings: str = "ozon.postings"
    kafka_topic_products: str = "ozon.products"

    minio_endpoint: str = "minio:9000"
    minio_access_key: str = "minioadmin"
    minio_secret_key: str = "minioadmin"
    minio_bucket: str = "ozon-raw"

    model_config = SettingsConfigDict(env_file=".env", case_sensitive=False, extra="ignore")

    def load_accounts(self) -> list[Account]:
        path = Path(self.ozon_accounts_file)
        if not path.exists():
            raise FileNotFoundError(
                f"Файл с аккаунтами не найден: {path}. "
                f"Создай accounts.json по образцу accounts.example.json."
            )
        raw = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(raw, list) or not raw:
            raise ValueError(f"{path}: ожидался непустой список объектов с ключами name/client_id/api_key")
        accounts = [Account.model_validate(item) for item in raw]

        names = [a.name for a in accounts]
        if len(set(names)) != len(names):
            raise ValueError(f"Имена аккаунтов в {path} должны быть уникальными: {names}")
        return accounts


settings = Settings()
