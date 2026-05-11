from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

import structlog

from ..ozon_client import OzonClient
from .base import FetchResult, Source

log = structlog.get_logger()


class PostingsSource(Source):
    """Universal collector for Ozon postings — works for both FBS and FBO.

    `scheme` selects the API path. The shape of postings is similar enough that
    one normalizer handles both with small per-scheme tweaks.
    """

    PATHS = {
        "fbs": "/v3/posting/fbs/list",
        "fbo": "/v2/posting/fbo/list",
    }

    def __init__(self, client: OzonClient, scheme: str):
        assert scheme in self.PATHS
        self._client = client
        self.scheme = scheme
        self.name = scheme

    async def fetch(self, since: datetime, to: datetime) -> FetchResult:
        path = self.PATHS[self.scheme]
        offset = 0
        limit = 1000
        raw_all: list[dict] = []
        normalized_all: list[dict] = []
        max_created: datetime | None = None

        while True:
            body = {
                "dir": "ASC",
                "filter": {
                    "since": _iso(since),
                    "to": _iso(to),
                    "status": "",
                },
                "limit": limit,
                "offset": offset,
                "with": {"analytics_data": True, "financial_data": True},
            }
            data = await self._client.post(path, body)
            result = data.get("result") or {}
            postings = result.get("postings") if isinstance(result, dict) else result
            postings = postings or []

            for p in postings:
                raw_all.append(p)
                norm = self._normalize(p)
                normalized_all.append(norm)
                created = _parse_dt(norm.get("created_at"))
                if created and (max_created is None or created > max_created):
                    max_created = created

            log.info(
                "ozon.postings.page",
                scheme=self.scheme, offset=offset, fetched=len(postings),
            )
            if len(postings) < limit:
                break
            offset += limit

        return FetchResult(
            source=self.scheme,
            raw_records=raw_all,
            normalized=normalized_all,
            next_cursor=max_created,
        )

    def _normalize(self, p: dict[str, Any]) -> dict[str, Any]:
        # delivery_method может быть dict или None
        dm = p.get("delivery_method") or {}
        if not isinstance(dm, dict):
            dm = {}
        analytics = p.get("analytics_data") or {}
        financial = p.get("financial_data") or {}

        # FBO: created_at часто во вложенности; FBS: in_process_at = момент попадания в обработку
        created_at = p.get("created_at") or p.get("in_process_at")

        items = []
        for prod in p.get("products") or []:
            items.append(
                {
                    "sku": _to_int(prod.get("sku")),
                    "offer_id": str(prod.get("offer_id") or ""),
                    "name": str(prod.get("name") or ""),
                    "quantity": int(prod.get("quantity") or 0),
                    "price": str(prod.get("price") or "0"),
                    "currency": prod.get("currency_code") or "RUB",
                }
            )

        return {
            "posting_number": str(p.get("posting_number") or ""),
            "scheme": self.scheme,
            "status": str(p.get("status") or ""),
            "created_at": created_at,
            "in_process_at": p.get("in_process_at"),
            "shipment_date": p.get("shipment_date"),
            "delivering_date": p.get("delivering_date"),
            "region": str(analytics.get("region") or ""),
            "city": str(analytics.get("city") or ""),
            "warehouse_id": _to_int(dm.get("warehouse_id")),
            "warehouse_name": str(dm.get("warehouse") or dm.get("name") or ""),
            "delivery_method": str(dm.get("name") or analytics.get("delivery_type") or ""),
            "tpl_provider": str(dm.get("tpl_provider") or analytics.get("tpl_provider") or ""),
            "items_count": sum(it["quantity"] for it in items),
            "total_price": str(financial.get("total_price") or sum(_to_float(it["price"]) * it["quantity"] for it in items)),
            "currency": (items[0]["currency"] if items else "RUB"),
            "items": items,
        }


def _iso(dt: datetime) -> str:
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")


def _parse_dt(s: Any) -> datetime | None:
    if not s or not isinstance(s, str):
        return None
    try:
        return datetime.fromisoformat(s.replace("Z", "+00:00"))
    except ValueError:
        return None


def _to_int(v: Any) -> int | None:
    try:
        return int(v) if v is not None else None
    except (TypeError, ValueError):
        return None


def _to_float(v: Any) -> float:
    try:
        return float(v)
    except (TypeError, ValueError):
        return 0.0
