from __future__ import annotations

import clickhouse_connect
import orjson

from .config import settings
from .geo_extract import POSTING_GEO_COLUMNS, build_posting_geo_row


def main() -> None:
    ch = clickhouse_connect.get_client(
        host=settings.clickhouse_host,
        port=settings.clickhouse_port,
        username=settings.clickhouse_user,
        password=settings.clickhouse_password,
        database=settings.clickhouse_db,
    )

    rows_by_key: dict[tuple[str, str, str], tuple] = {}
    result = ch.query(
        """
        SELECT account, source, payload
        FROM raw_events
        WHERE source IN ('fbs', 'fbo')
        ORDER BY ingested_at ASC
        """
    )

    for account, source, payload in result.result_rows:
        try:
            raw_payload = orjson.loads(payload)
        except orjson.JSONDecodeError:
            continue
        row = build_posting_geo_row(account, source, {}, raw_payload)
        key = (row[0], row[1], row[2])
        rows_by_key[key] = row

    rows = list(rows_by_key.values())
    if rows:
        ch.insert("posting_geo", rows, column_names=POSTING_GEO_COLUMNS)

    print(f"posting_geo_backfilled={len(rows)}")


if __name__ == "__main__":
    main()
