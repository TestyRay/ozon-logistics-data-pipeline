from __future__ import annotations

import io
from datetime import datetime, timezone

import orjson
import structlog
from minio import Minio
from minio.error import S3Error

log = structlog.get_logger()

STATE_KEY = "_state/cursors.json"


def _key(account: str, source: str) -> str:
    return f"{account}::{source}"


class CursorStore:
    """Persists per-(account, source) high-watermark timestamps in MinIO."""

    def __init__(self, client: Minio, bucket: str):
        self._client = client
        self._bucket = bucket

    def load(self) -> dict[tuple[str, str], datetime]:
        try:
            resp = self._client.get_object(self._bucket, STATE_KEY)
            data = orjson.loads(resp.read())
            resp.close()
            resp.release_conn()
            out: dict[tuple[str, str], datetime] = {}
            for k, v in data.items():
                if "::" not in k:
                    continue  # старый формат — пропускаем, считаем cold start
                account, source = k.split("::", 1)
                out[(account, source)] = datetime.fromisoformat(v).replace(tzinfo=timezone.utc)
            return out
        except S3Error as e:
            if e.code == "NoSuchKey":
                return {}
            log.warning("state.load_failed", error=str(e))
            return {}

    def save(self, cursors: dict[tuple[str, str], datetime]) -> None:
        body = orjson.dumps(
            {
                _key(account, source): dt.astimezone(timezone.utc).isoformat()
                for (account, source), dt in cursors.items()
            }
        )
        self._client.put_object(
            self._bucket,
            STATE_KEY,
            io.BytesIO(body),
            length=len(body),
            content_type="application/json",
        )
