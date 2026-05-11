from __future__ import annotations

import logging
import time
from datetime import datetime, timezone
from typing import Any

import clickhouse_connect
import orjson
import structlog
from kafka import KafkaConsumer

from .config import settings
from .geo_extract import POSTING_GEO_COLUMNS, build_posting_geo_row

structlog.configure(
    processors=[
        structlog.processors.add_log_level,
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.JSONRenderer(),
    ],
    wrapper_class=structlog.make_filtering_bound_logger(logging.INFO),
)
log = structlog.get_logger()


def parse_dt(s: Any) -> datetime | None:
    if not s:
        return None
    if isinstance(s, datetime):
        return s
    if not isinstance(s, str):
        return None
    try:
        return datetime.fromisoformat(s.replace("Z", "+00:00"))
    except ValueError:
        return None


def to_decimal_str(v: Any) -> str:
    try:
        return f"{float(v):.2f}"
    except (TypeError, ValueError):
        return "0.00"


def build_consumer() -> KafkaConsumer:
    return KafkaConsumer(
        settings.kafka_topic_postings,
        settings.kafka_topic_products,
        bootstrap_servers=[s.strip() for s in settings.kafka_bootstrap.split(",")],
        group_id=settings.kafka_group_id,
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        api_version=(2, 0, 0),
        max_poll_records=settings.batch_size,
        session_timeout_ms=10000,
    )


def build_clickhouse():
    return clickhouse_connect.get_client(
        host=settings.clickhouse_host,
        port=settings.clickhouse_port,
        username=settings.clickhouse_user,
        password=settings.clickhouse_password,
        database=settings.clickhouse_db,
    )


def flush_postings(ch, postings_rows: list, items_rows: list, geo_rows: list, raw_rows: list) -> None:
    if postings_rows:
        ch.insert(
            "postings",
            postings_rows,
            column_names=[
                "account", "posting_number", "scheme", "status", "created_at", "in_process_at",
                "shipment_date", "delivering_date", "region", "city",
                "warehouse_id", "warehouse_name", "delivery_method", "tpl_provider",
                "items_count", "total_price", "currency",
            ],
        )
    if items_rows:
        ch.insert(
            "posting_items",
            items_rows,
            column_names=[
                "account", "posting_number", "sku", "offer_id", "name",
                "quantity", "price", "currency",
            ],
        )
    if geo_rows:
        ch.insert("posting_geo", geo_rows, column_names=POSTING_GEO_COLUMNS)
    if raw_rows:
        ch.insert("raw_events", raw_rows, column_names=["account", "source", "entity_id", "payload"])


def flush_products(ch, product_rows: list, raw_rows: list) -> None:
    if product_rows:
        ch.insert(
            "products",
            product_rows,
            column_names=[
                "account", "sku", "offer_id", "name", "category_id",
                "price", "currency", "visible", "archived", "updated_at",
            ],
        )
    if raw_rows:
        ch.insert("raw_events", raw_rows, column_names=["account", "source", "entity_id", "payload"])


def handle_posting(msg_value: dict, postings_rows, items_rows, geo_rows, raw_rows) -> None:
    account = msg_value.get("account") or "unknown"
    source = msg_value.get("source") or "fbs"
    data = msg_value.get("data") or {}
    raw_payload = msg_value.get("raw") or data

    posting_number = data.get("posting_number") or ""
    created_at = parse_dt(data.get("created_at")) or datetime.now(timezone.utc)
    region = data.get("region") or ""
    city = data.get("city") or ""

    postings_rows.append(
        (
            account,
            posting_number,
            data.get("scheme") or source,
            data.get("status") or "",
            created_at,
            parse_dt(data.get("in_process_at")),
            parse_dt(data.get("shipment_date")),
            parse_dt(data.get("delivering_date")),
            region,
            city,
            data.get("warehouse_id"),
            data.get("warehouse_name") or "",
            data.get("delivery_method") or "",
            data.get("tpl_provider") or "",
            int(data.get("items_count") or 0),
            to_decimal_str(data.get("total_price")),
            data.get("currency") or "RUB",
        )
    )

    for it in data.get("items") or []:
        items_rows.append(
            (
                account,
                posting_number,
                int(it.get("sku") or 0),
                str(it.get("offer_id") or ""),
                str(it.get("name") or ""),
                int(it.get("quantity") or 0),
                to_decimal_str(it.get("price")),
                str(it.get("currency") or "RUB"),
            )
        )

    geo_rows.append(build_posting_geo_row(account, source, data, raw_payload))
    raw_rows.append((account, source, posting_number, orjson.dumps(raw_payload).decode()))


def handle_product(msg_value: dict, product_rows, raw_rows) -> None:
    account = msg_value.get("account") or "unknown"
    data = msg_value.get("data") or {}
    sku = int(data.get("sku") or 0)
    product_rows.append(
        (
            account,
            sku,
            str(data.get("offer_id") or ""),
            str(data.get("name") or ""),
            int(data.get("category_id") or 0),
            to_decimal_str(data.get("price")),
            str(data.get("currency") or "RUB"),
            int(data.get("visible") or 0),
            int(data.get("archived") or 0),
            parse_dt(data.get("updated_at")) or datetime.now(timezone.utc),
        )
    )
    raw_payload = msg_value.get("raw") or data
    raw_rows.append((account, "product", str(sku), orjson.dumps(raw_payload).decode()))


def main() -> None:
    log.info("consumer.starting")
    consumer = build_consumer()
    ch = build_clickhouse()

    postings_rows: list = []
    items_rows: list = []
    geo_rows: list = []
    product_rows: list = []
    raw_rows_p: list = []
    raw_rows_pr: list = []

    last_flush = time.monotonic()

    while True:
        records = consumer.poll(timeout_ms=1000, max_records=settings.batch_size)

        should_flush = (
            (len(postings_rows) + len(product_rows)) >= settings.batch_size
            or (time.monotonic() - last_flush) * 1000 >= settings.batch_timeout_ms
        )

        if not records:
            if should_flush and (postings_rows or product_rows):
                flush_postings(ch, postings_rows, items_rows, geo_rows, raw_rows_p)
                flush_products(ch, product_rows, raw_rows_pr)
                consumer.commit()
                postings_rows.clear(); items_rows.clear(); geo_rows.clear(); raw_rows_p.clear()
                product_rows.clear(); raw_rows_pr.clear()
                last_flush = time.monotonic()
            continue

        for messages in records.values():
            for msg in messages:
                try:
                    value = orjson.loads(msg.value)
                except Exception as e:
                    log.error("decode.failed", topic=msg.topic, offset=msg.offset, error=str(e))
                    continue

                if msg.topic == settings.kafka_topic_postings:
                    handle_posting(value, postings_rows, items_rows, geo_rows, raw_rows_p)
                elif msg.topic == settings.kafka_topic_products:
                    handle_product(value, product_rows, raw_rows_pr)

        if should_flush:
            flush_postings(ch, postings_rows, items_rows, geo_rows, raw_rows_p)
            flush_products(ch, product_rows, raw_rows_pr)
            consumer.commit()
            log.info(
                "batch.flushed",
                postings=len(postings_rows),
                items=len(items_rows),
                geo=len(geo_rows),
                products=len(product_rows),
            )
            postings_rows.clear(); items_rows.clear(); geo_rows.clear(); raw_rows_p.clear()
            product_rows.clear(); raw_rows_pr.clear()
            last_flush = time.monotonic()


if __name__ == "__main__":
    main()
