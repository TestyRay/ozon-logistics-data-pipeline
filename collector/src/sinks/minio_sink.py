import io
from datetime import datetime, timezone

import orjson
import structlog
from minio import Minio
from minio.error import S3Error

log = structlog.get_logger()


class MinioRawSink:
    """Stores raw API responses as NDJSON in MinIO, partitioned by date and source."""

    def __init__(self, endpoint: str, access_key: str, secret_key: str, bucket: str, secure: bool = False):
        self._client = Minio(endpoint, access_key=access_key, secret_key=secret_key, secure=secure)
        self._bucket = bucket
        self._ensure_bucket()

    def _ensure_bucket(self) -> None:
        try:
            if not self._client.bucket_exists(self._bucket):
                self._client.make_bucket(self._bucket)
                log.info("minio.bucket_created", bucket=self._bucket)
        except S3Error as e:
            log.error("minio.bucket_check_failed", error=str(e))
            raise

    def put_batch(self, account: str, source: str, records: list[dict]) -> str | None:
        if not records:
            return None
        now = datetime.now(timezone.utc)
        key = (
            f"accounts/{account}/{source}/"
            f"dt={now.strftime('%Y-%m-%d')}/"
            f"hr={now.strftime('%H')}/"
            f"{now.strftime('%Y%m%dT%H%M%S%f')}.ndjson"
        )
        body = b"\n".join(orjson.dumps(r) for r in records)
        data = io.BytesIO(body)
        self._client.put_object(
            self._bucket,
            key,
            data,
            length=len(body),
            content_type="application/x-ndjson",
        )
        log.info("minio.batch_stored", key=key, count=len(records), bytes=len(body))
        return key