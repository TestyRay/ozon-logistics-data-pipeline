from __future__ import annotations

from datetime import datetime
from typing import Any

import structlog

from ..ozon_client import OzonClient
from .base import FetchResult, Source

log = structlog.get_logger()


class ProductsSource(Source):
    name = "product"

    def __init__(self, client: OzonClient):
        self._client = client

    async def fetch(self, since: datetime, to: datetime) -> FetchResult:
        # Список SKU продавца постранично
        last_id = ""
        limit = 1000
        skus: list[int] = []
        while True:
            data = await self._client.post(
                "/v3/product/list",
                {"filter": {"visibility": "ALL"}, "last_id": last_id, "limit": limit},
            )
            result = data.get("result") or {}
            items = result.get("items") or []
            for it in items:
                sku = it.get("product_id") or it.get("sku")
                if sku:
                    skus.append(int(sku))
            last_id = result.get("last_id") or ""
            log.info("ozon.products.page", got=len(items), total_so_far=len(skus))
            if not last_id or len(items) < limit:
                break

        if not skus:
            return FetchResult(source=self.name, raw_records=[], normalized=[], next_cursor=None)

        # Детали по SKU пачками по 1000
        raw_all: list[dict] = []
        normalized_all: list[dict] = []
        for chunk in _chunks(skus, 1000):
            data = await self._client.post(
                "/v3/product/info/list", {"product_id": chunk}
            )
            for item in (data.get("items") or data.get("result", {}).get("items") or []):
                raw_all.append(item)
                normalized_all.append(self._normalize(item))

        return FetchResult(
            source=self.name,
            raw_records=raw_all,
            normalized=normalized_all,
            next_cursor=to,
        )

    @staticmethod
    def _normalize(p: dict[str, Any]) -> dict[str, Any]:
        return {
            "sku": int(p.get("id") or p.get("sku") or 0),
            "offer_id": str(p.get("offer_id") or ""),
            "name": str(p.get("name") or ""),
            "category_id": int(p.get("description_category_id") or p.get("category_id") or 0),
            "price": str(p.get("price") or "0"),
            "currency": str(p.get("currency_code") or "RUB"),
            "visible": 1 if p.get("is_visible") else 0,
            "archived": 1 if p.get("is_archived") else 0,
            "updated_at": p.get("updated_at") or p.get("created_at") or "",
        }


def _chunks(seq: list, n: int):
    for i in range(0, len(seq), n):
        yield seq[i : i + n]
