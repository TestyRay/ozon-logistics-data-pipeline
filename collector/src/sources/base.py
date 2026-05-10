from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime


@dataclass
class FetchResult:
    source: str
    raw_records: list[dict]      # to MinIO (scrubbed of PII)
    normalized: list[dict]       # to Kafka (already shaped for downstream)
    next_cursor: datetime | None # high-watermark to persist for next run


class Source(ABC):
    name: str

    @abstractmethod
    async def fetch(self, since: datetime, to: datetime) -> FetchResult: ...