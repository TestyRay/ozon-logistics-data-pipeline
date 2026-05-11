from __future__ import annotations

from typing import Any


POSTING_GEO_COLUMNS = [
    "account",
    "posting_number",
    "scheme",
    "region",
    "city",
    "address_country",
    "address_region",
    "address_city",
    "address_district",
    "address_tail",
    "zip_code",
    "latitude",
    "longitude",
    "pvz_code",
    "provider_pvz_code",
    "warehouse_id",
    "warehouse_name",
    "delivery_method",
    "tpl_provider",
    "geo_source",
]


def build_posting_geo_row(
    account: str,
    source: str,
    data: dict[str, Any],
    raw_payload: dict[str, Any],
) -> tuple:
    raw = raw_payload if isinstance(raw_payload, dict) else {}
    analytics = _dict(raw.get("analytics_data"))
    delivery_method = _dict(raw.get("delivery_method"))
    address = _dict(_dict(raw.get("customer")).get("address"))

    latitude = _to_float(address.get("latitude"))
    longitude = _to_float(address.get("longitude"))
    geo_source = ""
    if latitude is not None or longitude is not None:
        geo_source = "customer.address"
    else:
        latitude = _to_float(_find_first_key(raw, {"latitude", "lat"}))
        longitude = _to_float(_find_first_key(raw, {"longitude", "lon"}))
        if latitude is not None or longitude is not None:
            geo_source = "raw_payload"
        elif address:
            geo_source = "customer.address"
        elif analytics:
            geo_source = "analytics_data"

    return (
        account,
        str(raw.get("posting_number") or data.get("posting_number") or ""),
        str(data.get("scheme") or source or ""),
        _text(analytics.get("region") or address.get("region") or data.get("region")),
        _text(analytics.get("city") or address.get("city") or data.get("city")),
        _text(address.get("country")),
        _text(address.get("region")),
        _text(address.get("city")),
        _text(address.get("district")),
        _text(address.get("address_tail")),
        _text(address.get("zip_code")),
        latitude,
        longitude,
        _text(address.get("pvz_code")),
        _text(address.get("provider_pvz_code")),
        _to_int(delivery_method.get("warehouse_id") or analytics.get("warehouse_id") or data.get("warehouse_id")),
        _text(
            delivery_method.get("warehouse")
            or delivery_method.get("name")
            or analytics.get("warehouse")
            or analytics.get("warehouse_name")
            or data.get("warehouse_name")
        ),
        _text(delivery_method.get("name") or analytics.get("delivery_type") or data.get("delivery_method")),
        _text(delivery_method.get("tpl_provider") or analytics.get("tpl_provider") or data.get("tpl_provider")),
        geo_source,
    )


def _dict(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _text(value: Any) -> str:
    return "" if value is None else str(value)


def _to_int(value: Any) -> int | None:
    try:
        return int(value) if value is not None and value != "" else None
    except (TypeError, ValueError):
        return None


def _to_float(value: Any) -> float | None:
    try:
        return float(value) if value is not None and value != "" else None
    except (TypeError, ValueError):
        return None


def _find_first_key(value: Any, keys: set[str]) -> Any:
    if isinstance(value, dict):
        for key, item in value.items():
            if key.lower() in keys:
                return item
        for item in value.values():
            found = _find_first_key(item, keys)
            if found is not None:
                return found
    elif isinstance(value, list):
        for item in value:
            found = _find_first_key(item, keys)
            if found is not None:
                return found
    return None
