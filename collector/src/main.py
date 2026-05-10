from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timedelta, timezone

import structlog
from minio import Minio

from .config import Account, settings
from .ozon_client import OzonClient, OzonPermissionError
from .sinks.kafka_sink import KafkaSink
from .sinks.minio_sink import MinioRawSink
from .sources.base import Source
from .sources.postings import PostingsSource
from .sources.products import ProductsSource
from .state import CursorStore

structlog.configure(
    processors=[
        structlog.processors.add_log_level,
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.JSONRenderer(),
    ],
    wrapper_class=structlog.make_filtering_bound_logger(logging.INFO),
)
log = structlog.get_logger()


async def detect_sources(client: OzonClient, account: str) -> list[Source]:
    """Probe FBS/FBO endpoints; keep only those the API key has access to."""
    sources: list[Source] = []
    probe_window = {
        "filter": {
            "since": (datetime.now(timezone.utc) - timedelta(days=1))
                .isoformat().replace("+00:00", "Z"),
            "to": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
            "status": "",
        },
        "limit": 1,
        "offset": 0,
    }
    for scheme, path in PostingsSource.PATHS.items():
        try:
            await client.post(path, probe_window)
            sources.append(PostingsSource(client, scheme))
            log.info("source.enabled", account=account, source=scheme)
        except OzonPermissionError as e:
            log.warning("source.disabled_no_permission", account=account, source=scheme, detail=str(e))
        except Exception as e:
            log.warning("source.probe_failed", account=account, source=scheme, detail=str(e))
    sources.append(ProductsSource(client))
    return sources


async def collect_account(
    account: Account,
    kafka: KafkaSink,
    raw: MinioRawSink,
    cursors: dict[tuple[str, str], datetime],
) -> None:
    client = OzonClient(
        base_url=settings.ozon_base_url,
        client_id=account.client_id,
        api_key=account.api_key,
    )
    try:
        sources = await detect_sources(client, account.name)
        if not sources:
            log.warning("account.no_sources", account=account.name)
            return

        now = datetime.now(timezone.utc)
        default_since = now - timedelta(days=settings.initial_lookback_days)

        for source in sources:
            since = cursors.get((account.name, source.name), default_since)
            try:
                result = await source.fetch(since=since, to=now)
            except Exception as e:
                log.error("source.fetch_failed", account=account.name, source=source.name, error=str(e))
                continue

            raw.put_batch(account.name, source.name, result.raw_records)

            topic = (
                settings.kafka_topic_products
                if source.name == "product"
                else settings.kafka_topic_postings
            )
            for rec in result.normalized:
                key = f"{account.name}:{rec.get('posting_number') or rec.get('sku') or ''}"
                kafka.send(
                    topic,
                    key,
                    {"account": account.name, "source": source.name, "data": rec},
                )

            log.info(
                "source.completed",
                account=account.name,
                source=source.name,
                raw=len(result.raw_records),
                normalized=len(result.normalized),
            )

            if result.next_cursor:
                cursors[(account.name, source.name)] = result.next_cursor
    finally:
        await client.aclose()


async def main() -> None:
    accounts = settings.load_accounts()
    log.info(
        "collector.starting",
        interval=settings.poll_interval_seconds,
        accounts=[a.name for a in accounts],
    )

    minio = Minio(
        settings.minio_endpoint,
        access_key=settings.minio_access_key,
        secret_key=settings.minio_secret_key,
        secure=False,
    )
    raw = MinioRawSink(
        endpoint=settings.minio_endpoint,
        access_key=settings.minio_access_key,
        secret_key=settings.minio_secret_key,
        bucket=settings.minio_bucket,
    )
    kafka = KafkaSink(settings.kafka_bootstrap)
    state = CursorStore(minio, settings.minio_bucket)

    cursors = state.load()
    log.info(
        "collector.cursors_loaded",
        cursors={f"{a}::{s}": v.isoformat() for (a, s), v in cursors.items()},
    )

    while True:
        for account in accounts:
            try:
                await collect_account(account, kafka, raw, cursors)
            except Exception as e:
                log.exception("account.cycle_failed", account=account.name, error=str(e))
        kafka.flush()
        try:
            state.save(cursors)
        except Exception as e:
            log.error("state.save_failed", error=str(e))
        await asyncio.sleep(settings.poll_interval_seconds)


if __name__ == "__main__":
    asyncio.run(main())
