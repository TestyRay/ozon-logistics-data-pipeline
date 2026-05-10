CREATE DATABASE IF NOT EXISTS ozon;

-- Сырые события из Ozon API (как есть, в JSON), для аудита и переигровки
CREATE TABLE IF NOT EXISTS ozon.raw_events
(
    ingested_at  DateTime64(3) DEFAULT now64(3),
    account      LowCardinality(String),       -- логическое имя кабинета продавца
    source       LowCardinality(String),       -- 'fbs' | 'fbo' | 'product'
    entity_id    String,                       -- posting_number / sku
    payload      String CODEC(ZSTD(3))         -- сырой JSON (без ПДн)
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(ingested_at)
ORDER BY (account, source, ingested_at, entity_id);

-- Заказы / отправления (postings) — обезличенные
CREATE TABLE IF NOT EXISTS ozon.postings
(
    account          LowCardinality(String),
    posting_number   String,
    scheme           LowCardinality(String),   -- 'fbs' | 'fbo'
    status           LowCardinality(String),
    created_at       DateTime64(3),
    in_process_at    Nullable(DateTime64(3)),
    shipment_date    Nullable(DateTime64(3)),
    delivering_date  Nullable(DateTime64(3)),
    region           LowCardinality(String),
    city             LowCardinality(String),
    warehouse_id     Nullable(UInt64),
    warehouse_name   LowCardinality(String),
    delivery_method  LowCardinality(String),
    tpl_provider     LowCardinality(String),
    items_count      UInt32,
    total_price      Decimal(18, 2),
    currency         LowCardinality(String),
    buyer_hash       String,
    ingested_at      DateTime64(3) DEFAULT now64(3)
)
ENGINE = ReplacingMergeTree(ingested_at)
PARTITION BY toYYYYMM(created_at)
ORDER BY (account, created_at, posting_number);

-- Позиции внутри отправлений
CREATE TABLE IF NOT EXISTS ozon.posting_items
(
    account         LowCardinality(String),
    posting_number  String,
    sku             UInt64,
    offer_id        String,
    name            String,
    quantity        UInt32,
    price           Decimal(18, 2),
    currency        LowCardinality(String),
    ingested_at     DateTime64(3) DEFAULT now64(3)
)
ENGINE = ReplacingMergeTree(ingested_at)
ORDER BY (account, posting_number, sku);

-- Каталог товаров продавца
CREATE TABLE IF NOT EXISTS ozon.products
(
    account       LowCardinality(String),
    sku           UInt64,
    offer_id      String,
    name          String,
    category_id   UInt64,
    price         Decimal(18, 2),
    currency      LowCardinality(String),
    visible       UInt8,
    archived      UInt8,
    updated_at    DateTime64(3),
    ingested_at   DateTime64(3) DEFAULT now64(3)
)
ENGINE = ReplacingMergeTree(ingested_at)
ORDER BY (account, sku);

-- Метрики работы collector'а
CREATE TABLE IF NOT EXISTS ozon.collector_runs
(
    run_id          UUID,
    account         LowCardinality(String),
    started_at      DateTime64(3),
    finished_at     DateTime64(3),
    source          LowCardinality(String),
    records_fetched UInt32,
    records_sent    UInt32,
    error           String
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(started_at)
ORDER BY (account, started_at, source);
