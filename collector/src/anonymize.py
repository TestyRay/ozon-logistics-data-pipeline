import hashlib
from typing import Any


PII_KEYS = {
    "name", "phone", "email", "patronymic", "last_name", "first_name",
    "address", "addressee", "customer", "comment", "barcode",
    "address_tail", "zip_code", "house", "apartment", "building",
}


def buyer_hash(salt: str, *parts: Any) -> str:
    h = hashlib.sha256()
    h.update(salt.encode("utf-8"))
    for p in parts:
        if p is None:
            continue
        h.update(b"|")
        h.update(str(p).encode("utf-8"))
    return h.hexdigest()


def scrub_pii(obj: Any) -> Any:
    """Recursively drop known PII keys before persisting raw payload."""
    if isinstance(obj, dict):
        return {
            k: scrub_pii(v)
            for k, v in obj.items()
            if k.lower() not in PII_KEYS
        }
    if isinstance(obj, list):
        return [scrub_pii(v) for v in obj]
    return obj