# Система сбора логистических данных Ozon

Учебный программный комплекс для практической работы по теме сбора больших пространственных данных. Система подключается к Ozon Seller API, забирает данные по отправлениям FBS/FBO и каталогу товаров, сохраняет полный сырой слой, нормализует данные в ClickHouse и показывает аналитику в Grafana.

Проект рассчитан на демонстрацию полного процесса сбора данных:

- получение данных из внешнего API;
- хранение сырого слоя без изменений;
- потоковая передача событий через Kafka-совместимый брокер;
- нормализация данных в аналитические таблицы;
- хранение геоданных, если они реально пришли из API;
- визуализация в Grafana;
- работа в Docker Compose как набор отдельных сервисов.

## Важное предупреждение

Сейчас проект сохраняет ответы Ozon API как есть. Данные не обезличиваются, не шифруются и не вырезаются.

Это значит, что в MinIO могут попадать реальные данные из личного кабинета продавца, включая адресные поля, телефоны, имена получателей и другие чувствительные значения, если Ozon возвращает их в ответе API.

## Архитектура

Общая схема:

```text
Ozon Seller API
    |
    v
collector
    |\
    | \-> MinIO: полный сырой слой в NDJSON
    |
    v
Redpanda: Kafka-совместимые топики
    |
    v
consumer
    |
    v
ClickHouse
    |
    v
Grafana
```

Смысл компонентов:

- `collector` - Python-сервис, который обращается к Ozon Seller API.
- `Redpanda` - Kafka-совместимый брокер сообщений между сервисами `collector` и `consumer`.
- `consumer` - Python-сервис, который читает сообщения из Redpanda и пишет нормализованные строки в ClickHouse.
- `ClickHouse` - аналитическая БД для быстрых запросов и панелей дашборда.
- `MinIO` - объектное хранилище для полного сырого слоя Ozon API.
- `Grafana` - интерфейс визуализации и проверки результата.

Почему так сделано:

- сырой слой нужен, чтобы можно было перепроверить исходный ответ API и переобработать данные;
- Kafka/Redpanda показывает потоковую архитектуру, а не прямую запись из API в БД;
- ClickHouse подходит для аналитических запросов по большому числу строк;
- Grafana позволяет быстро показать преподавателю результат в виде дашборда;
- Docker Compose позволяет поднять всю систему одной командой.

## Какие Ozon API методы используются

Система использует только методы чтения Ozon Seller API. Физически запросы идут через HTTP POST, но это нормальный формат Ozon API для методов чтения.

Методы для отправлений:

```text
/v3/posting/fbs/list
/v2/posting/fbo/list
```

Методы для каталога товаров:

```text
/v3/product/list
/v3/product/info/list
```

## Что такое FBS и FBO

`FBS` - схема, при которой продавец сам хранит товар и передаёт заказ в доставку Ozon.

`FBO` - схема, при которой товар хранится на складе Ozon, а Ozon сам собирает и доставляет заказ.

В системе оба источника сохраняются в одной логической модели отправлений, но поле `scheme` показывает источник:

```text
fbs
fbo
```

## Какие данные собираются

### Отправления

Источник:

```text
/v3/posting/fbs/list
/v2/posting/fbo/list
```

Нормализованная таблица:

```text
ozon.postings
```

Поля:

```text
account
posting_number
scheme
status
created_at
in_process_at
shipment_date
delivering_date
region
city
warehouse_id
warehouse_name
delivery_method
tpl_provider
items_count
total_price
currency
ingested_at
```

Что означает:

- `account` - логическое имя кабинета из `accounts.json`.
- `posting_number` - номер отправления Ozon.
- `scheme` - схема, `fbs` или `fbo`.
- `status` - статус отправления.
- `created_at` - дата создания или попадания в обработку.
- `in_process_at` - дата начала обработки.
- `shipment_date` - дата отгрузки.
- `delivering_date` - дата доставки, если есть.
- `region` и `city` - регион и город из `analytics_data`.
- `warehouse_id` и `warehouse_name` - склад, если Ozon вернул эти данные.
- `delivery_method` - способ доставки.
- `tpl_provider` - логистический провайдер.
- `items_count` - количество товаров внутри отправления.
- `total_price` - сумма по отправлению.
- `currency` - валюта.
- `ingested_at` - когда строка попала в ClickHouse.

### Товары внутри отправлений

Источник - блок `products` внутри отправлений FBS/FBO.

Нормализованная таблица:

```text
ozon.posting_items
```

Поля:

```text
account
posting_number
sku
offer_id
name
quantity
price
currency
ingested_at
```

Что означает:

- `posting_number` связывает позицию с отправлением.
- `sku` - Ozon SKU.
- `offer_id` - артикул продавца.
- `name` - название товара.
- `quantity` - количество единиц товара в отправлении.
- `price` - цена позиции.
- `currency` - валюта.

### Геоданные отправлений

Источник - реальные поля из сырого JSON Ozon API.

Нормализованная таблица:

```text
ozon.posting_geo
```

Поля:

```text
account
posting_number
scheme
region
city
address_country
address_region
address_city
address_district
address_tail
zip_code
latitude
longitude
pvz_code
provider_pvz_code
warehouse_id
warehouse_name
delivery_method
tpl_provider
geo_source
ingested_at
```

Что важно:

- `latitude` и `longitude` сохраняются только если Ozon API реально вернул координаты.
- `address_tail`, `zip_code`, `pvz_code` и другие адресные поля сохраняются только если есть в сыром JSON.
- `geo_source` показывает, откуда взяты геополя: например `customer.address`, `analytics_data` или исходный объект ответа API.

Практический вывод:

- `posting_geo` может содержать строку для каждого отправления;
- карта в Grafana отображает только строки, где есть `latitude` и `longitude`;
- таблица `Геоданные отправлений из Ozon API` сейчас тоже отфильтрована по наличию координат.

### Каталог товаров

Источники:

```text
/v3/product/list
/v3/product/info/list
```

Нормализованная таблица:

```text
ozon.products
```

Поля:

```text
account
sku
offer_id
name
category_id
price
currency
visible
archived
updated_at
ingested_at
```

Что означает:

- `sku` - идентификатор товара.
- `offer_id` - артикул продавца.
- `name` - название товара.
- `category_id` - категория.
- `price` - цена.
- `visible` - видимость товара.
- `archived` - архивный товар или нет.
- `updated_at` - дата обновления из Ozon или дата создания, если обновления нет.

### Сырой слой

Сырой слой сохраняется в двух местах.

В MinIO:

```text
ozon-raw/accounts/<account>/<source>/dt=<YYYY-MM-DD>/hr=<HH>/<timestamp>.ndjson
```

В ClickHouse:

```text
ozon.raw_events
```

Поля `ozon.raw_events`:

```text
ingested_at
account
source
entity_id
payload
```

Что означает:

- `source` - `fbs`, `fbo` или `product`.
- `entity_id` - номер отправления или SKU.
- `payload` - полный JSON, полученный от Ozon API.

Формат NDJSON:

```text
одна строка = один JSON-объект
```

Пример проверки через ClickHouse без вывода самих персональных данных:

```sql
SELECT
    source,
    count() AS rows,
    avg(length(payload)) AS avg_payload_bytes
FROM ozon.raw_events
GROUP BY source
ORDER BY source;
```

## Как работает сервис сбора данных

Сервис `collector` выполняет полный цикл сбора.

Шаги:

1. Загружает список кабинетов из `accounts.json`.
2. Для каждого кабинета создаёт клиент Ozon API.
3. Проверяет доступность методов FBS и FBO.
4. Берёт курсор последней обработки из MinIO.
5. Если курсор отсутствует, использует `INITIAL_LOOKBACK_DAYS`.
6. Загружает FBS отправления постранично.
7. Загружает FBO отправления постранично.
8. Сохраняет полный сырой JSON в MinIO.
9. Отправляет нормализованные записи и сырой JSON в топик Kafka.
10. Загружает каталог товаров через `/v3/product/list` и `/v3/product/info/list`.
11. Сохраняет курсоры в MinIO `_state/cursors.json`.
12. Засыпает на `POLL_INTERVAL_SECONDS` и повторяет цикл.

Состояние курсоров хранится здесь:

```text
ozon-raw/_state/cursors.json
```

Важный момент про `INITIAL_LOOKBACK_DAYS`:

- параметр применяется только при запуске проекта, когда курсор отсутствует;
- если `_state/cursors.json` уже существует, `collector` продолжит с последней сохранённой точки;
- чтобы новый `INITIAL_LOOKBACK_DAYS` реально применился, нужно очистить состояние в MinIO.

## Как работает сервис обработки данных

Сервис `consumer` читает сообщения из Redpanda и пишет данные в ClickHouse.

Топики Kafka:

```text
ozon.postings
ozon.products
```

Сервис `consumer` делает следующие действия:

1. Читает пакет сообщений из Redpanda.
2. Декодирует JSON через `orjson`.
3. Для отправлений пишет строки в `ozon.postings`.
4. Для товаров внутри отправлений пишет строки в `ozon.posting_items`.
5. Из сырого JSON извлекает геоданные и пишет `ozon.posting_geo`.
6. Полный сырой JSON пишет в `ozon.raw_events`.
7. Для каталога товаров пишет `ozon.products`.
8. После успешной записи подтверждает обработку сообщений в Kafka.

Параметры пакетной обработки:

```text
BATCH_SIZE
BATCH_TIMEOUT_MS
```

Они находятся в `consumer/src/config.py` и могут задаваться через `.env`, если нужно.

## Таблицы ClickHouse

### `ozon.raw_events`

Назначение - хранение полного JSON от Ozon API.

Используется для:

- аудита;
- проверки исходного ответа;
- повторной обработки данных;
- поиска новых полей API;
- демонстрации архитектуры с сырым слоем данных.

### `ozon.postings`

Назначение - основная аналитическая таблица отправлений.

Используется для:

- счётчика отправлений;
- статусов;
- регионов;
- FBS/FBO разреза;
- таблицы последних отправлений;
- расчёта потока поступления данных.

### `ozon.posting_items`

Назначение - детализация товаров внутри отправлений.

Используется для анализа состава заказов.

### `ozon.posting_geo`

Назначение - нормализованные пространственные и адресные данные отправлений.

Используется для:

- таблицы геоданных;
- карты реальных координат из Ozon API;
- анализа регионов, городов, ПВЗ и складов.

### `ozon.products`

Назначение - каталог товаров продавца.

Используется для:

- счётчика товаров;
- проверки полноты каталога;
- дальнейшей товарной аналитики.

### `ozon.collector_runs`

Таблица заложена в схему для метрик запусков сервиса `collector`. В текущей реализации основной процесс пишет данные в остальные таблицы, а `collector_runs` оставлена как задел под расширение.

## Дашборд Grafana

Дашборд доступен по адресу:

```text
http://localhost:3000/d/shr-ozon/ozon-e28094-sistema-sbora-dannyh
```

Логин и пароль:

```text
admin / admin
```

Дашборд использует источник данных `ClickHouse`.

Переменная:

```text
account
```

Она позволяет выбрать один кабинет, несколько кабинетов или все сразу.

### Всего собрано отправлений

Показывает:

```sql
SELECT count() FROM ozon.postings
```

Это количество нормализованных FBS/FBO отправлений.

### Товаров в каталоге

Показывает:

```sql
SELECT count() FROM ozon.products
```

Это количество товаров, полученных из Ozon API для товаров.

Запрос использует `FINAL`, чтобы ClickHouse показывал актуальное состояние каталога после повторных загрузок:

```sql
SELECT count()
FROM ozon.products FINAL
```

### Топ товаров по количеству в отправлениях

Таблица показывает товары, которые чаще всего встречались внутри собранных отправлений.

Источник данных:

```text
ozon.posting_items
```

Поля панели:

```text
sku
offer_id
name
sold_qty
postings
revenue
currency
```

Что означают расчётные поля:

- `sold_qty` - суммарное количество единиц товара в отправлениях;
- `postings` - количество уникальных отправлений с этим товаром;
- `revenue` - сумма `price * quantity` по позициям товара.

Ограничение:

```text
LIMIT 20
```

### Топ товаров по выручке в отправлениях

Таблица показывает товары с наибольшей суммой по позициям внутри отправлений.

Источник данных:

```text
ozon.posting_items
```

Расчёт:

```sql
sum(price * quantity)
```

Это аналитическая сумма по тем ценам и количествам, которые пришли в блоке товаров внутри отправлений.

Ограничение:

```text
LIMIT 20
```

### Статус каталога товаров

Таблица показывает распределение товаров из каталога по состоянию:

```text
archived
visible
not_visible
```

Правило расчёта:

```sql
multiIf(archived = 1, 'archived', visible = 1, 'visible', 'not_visible')
```

Если Ozon API вернул `visible = 0` для всех товаров, панель покажет один статус `not_visible`. Это не ошибка Grafana, а прямое отражение данных из API.

### Каталог товаров из Ozon API

Таблица показывает последние 1000 товаров из нормализованного каталога.

Поля панели:

```text
sku
offer_id
name
category_id
price
currency
visible
archived
updated_at
ingested_at
```

Ограничение:

```text
LIMIT 1000
```

Полный каталог хранится в таблице:

```text
ozon.products
```

### Лаг сбора

Показывает, сколько секунд прошло с последней записи в `ozon.postings`.

Важно:

- это не задержка Ozon API;
- это задержка между текущим временем и последним `ingested_at` в ClickHouse.

### Сырых событий

Показывает число строк в `ozon.raw_events`.

Сюда входят:

```text
fbs
fbo
product
```

### Поток поступления отправлений

Показывает, сколько отправлений было записано в ClickHouse по минутам.

Используемое время:

```text
ingested_at
```

Это время поступления данных в систему, а не время создания заказа в Ozon.

Если после очистки и первого запуска график показывает высокий пик, это нормально: система за один прогон загрузила историческое окно данных.

### Топ регионов

Показывает топ регионов по числу отправлений.

Используется поле:

```text
region
```

Панель ограничена:

```text
LIMIT 20
```

### Распределение статусов

Круговая диаграмма по статусам отправлений.

Используется запрос:

```sql
SELECT status, count() AS value
FROM ozon.postings
GROUP BY status
ORDER BY value DESC
```

В панели включена трансформация `rowsToFields`, чтобы каждый статус отображался отдельным сектором, а не как один столбец `value`.

### Последние отправления

Таблица последних отправлений.

Ограничение:

```text
LIMIT 1000
```

Это не все строки, а последние 1000 по `created_at`.

Всего отправлений может быть больше. Полный набор данных находится в `ozon.postings`.

### Сводка по аккаунтам

Показывает по каждому аккаунту:

```text
postings
regions
cities
last_seen
```

Нужна для проверки нескольких кабинетов.

### Геоданные отправлений из Ozon API

Таблица геоданных, где есть реальные координаты.

Фильтр:

```sql
latitude IS NOT NULL
AND longitude IS NOT NULL
```

Ограничение:

```text
LIMIT 1000
```

Если Ozon вернул координаты только у части отправлений, таблица будет меньше общего числа отправлений.

### Карта реальных координат из Ozon API

Показывает точки только по строкам, где есть:

```text
latitude
longitude
```

Координаты берутся только из Ozon API.

Если у нескольких отправлений одинаковые координаты, точки могут визуально накладываться друг на друга.

## Подготовка к запуску

Требования:

- Linux или WSL2;
- Docker;
- плагин Docker Compose;
- доступ к Ozon Seller API;
- `client_id` и `api_key` продавца Ozon.

Создайте файлы настроек:

```bash
cp .env.example .env
cp accounts.example.json accounts.json
```

Заполните `accounts.json`:

```json
[
  {
    "name": "kant",
    "client_id": "ВАШ_CLIENT_ID",
    "api_key": "ВАШ_API_KEY"
  }
]
```

Поле `name` можно выбрать любое. Оно будет отображаться в Grafana и ClickHouse как `account`.

Заполните `.env` при необходимости:

```env
INITIAL_LOOKBACK_DAYS=7
POLL_INTERVAL_SECONDS=300
PRODUCTS_REFRESH_INTERVAL_SECONDS=86400
```

## Основные настройки `.env`

### `INITIAL_LOOKBACK_DAYS`

На сколько дней назад `collector` смотрит при первом запуске без сохранённого состояния курсоров.

Пример:

```env
INITIAL_LOOKBACK_DAYS=7
```

Если состояние курсоров уже есть, изменение этого параметра не перезапустит историческую загрузку. Нужно очистить в MinIO файл `_state/cursors.json`.

### `POLL_INTERVAL_SECONDS`

Пауза между циклами сервиса `collector`.

Пример:

```env
POLL_INTERVAL_SECONDS=300
```

Это значит, что `collector` повторяет сбор примерно раз в 5 минут.

### `PRODUCTS_REFRESH_INTERVAL_SECONDS`

Как часто обновлять полный каталог товаров.

Пример:

```env
PRODUCTS_REFRESH_INTERVAL_SECONDS=86400
```

Это значит, что каталог товаров обновляется примерно раз в сутки. Это сделано потому, что загрузка каталога тяжёлая и может возвращать десятки тысяч товаров.

### `MINIO_BUCKET`

Бакет для сырого слоя.

По умолчанию:

```env
MINIO_BUCKET=ozon-raw
```

## Запуск

Запустить весь проект:

```bash
docker compose up -d --build
```

Проверить контейнеры:

```bash
docker compose ps
```

Ожидаемые сервисы:

```text
shr-redpanda
shr-clickhouse
shr-minio
shr-collector
shr-consumer
shr-grafana
```

Посмотреть логи сервиса `collector`:

```bash
docker compose logs -f collector
```

Посмотреть логи сервиса `consumer`:

```bash
docker compose logs -f consumer
```

Открыть Grafana:

```text
http://localhost:3000
```

Открыть MinIO:

```text
http://localhost:9091
```

Открыть веб-интерфейс ClickHouse Play:

```text
http://localhost:18123/play
```

## Как понять, что загрузка завершилась

В логах сервиса `collector` должны появиться строки вида:

```text
source.completed source=fbs
source.completed source=fbo
source.completed source=product
```

Для товаров важно увидеть:

```text
source.completed product raw=<N> normalized=<N>
```

Проверить количество строк в ClickHouse:

```bash
docker compose exec -T clickhouse clickhouse-client --query "
SELECT 'raw_events' AS table, count() FROM ozon.raw_events
UNION ALL SELECT 'postings', count() FROM ozon.postings
UNION ALL SELECT 'posting_items', count() FROM ozon.posting_items
UNION ALL SELECT 'posting_geo', count() FROM ozon.posting_geo
UNION ALL SELECT 'products', count() FROM ozon.products
"
```

Проверить отставание потребителя Kafka:

```bash
docker compose exec -T redpanda rpk group describe shr-consumer
```

Если `TOTAL-LAG` равен `0`, сервис `consumer` догнал очередь.

## Как смотреть сырые данные

### Через интерфейс MinIO

Откройте:

```text
http://localhost:9091
```

Логин и пароль:

```text
minioadmin / minioadmin
```

Путь к файлам сырого слоя:

```text
ozon-raw/accounts/<account>/<source>/dt=<YYYY-MM-DD>/hr=<HH>/...
```

Примеры:

```text
ozon-raw/accounts/kant/fbs/dt=2026-05-11/hr=14/...
ozon-raw/accounts/kant/fbo/dt=2026-05-11/hr=14/...
ozon-raw/accounts/kant/product/dt=2026-05-11/hr=14/...
```

Скачанный файл имеет формат `.ndjson`.

### Почему на диске `.ndjson` выглядит как папка

Если открыть `data/minio/...` напрямую в файловой системе, MinIO может показывать объект как директорию с файлами:

```text
xl.meta
part.1
```

Это внутренний формат хранения MinIO. Правильно смотреть файлы сырого слоя через интерфейс MinIO или через MinIO API, а не редактировать `part.1` руками.

### Через ClickHouse

Посмотреть последние сырые события без вывода полного JSON:

```sql
SELECT
    ingested_at,
    account,
    source,
    entity_id,
    length(payload) AS payload_bytes
FROM ozon.raw_events
ORDER BY ingested_at DESC
LIMIT 20;
```

Посмотреть один полный JSON:

```sql
SELECT payload
FROM ozon.raw_events
ORDER BY ingested_at DESC
LIMIT 1;
```

## Полезные SQL-запросы

Количество строк по основным таблицам:

```sql
SELECT 'raw_events' AS table, count() FROM ozon.raw_events
UNION ALL SELECT 'postings', count() FROM ozon.postings
UNION ALL SELECT 'posting_items', count() FROM ozon.posting_items
UNION ALL SELECT 'posting_geo', count() FROM ozon.posting_geo
UNION ALL SELECT 'products', count() FROM ozon.products;
```

Количество отправлений по FBS/FBO:

```sql
SELECT scheme, count() AS postings
FROM ozon.postings
GROUP BY scheme
ORDER BY postings DESC;
```

Распределение статусов:

```sql
SELECT status, count() AS postings
FROM ozon.postings
GROUP BY status
ORDER BY postings DESC;
```

Топ регионов:

```sql
SELECT region, count() AS postings
FROM ozon.postings
WHERE region != ''
GROUP BY region
ORDER BY postings DESC
LIMIT 20;
```

Сколько строк имеют координаты:

```sql
SELECT
    count() AS geo_rows,
    countIf(latitude IS NOT NULL AND longitude IS NOT NULL) AS with_coordinates,
    countIf(address_tail != '') AS with_address_tail,
    countIf(pvz_code != '') AS with_pvz
FROM ozon.posting_geo;
```

Проверить последние отправления:

```sql
SELECT
    created_at,
    posting_number,
    scheme,
    status,
    region,
    city,
    total_price
FROM ozon.postings
ORDER BY created_at DESC
LIMIT 20;
```

## Очистка и повторный сбор

### Остановить сервисы без удаления данных

```bash
docker compose down
```

Данные останутся в `data/`.

### Полностью удалить все тома Docker проекта

В проекте используются подключённые директории `data/`, поэтому для полной очистки нужно удалить эту директорию:

```bash
docker compose down
rm -rf data/
```

После этого можно поднять проект заново:

```bash
docker compose up -d --build
```

### Очистить только ClickHouse таблицы

```bash
docker compose exec -T clickhouse clickhouse-client --multiquery --query "
TRUNCATE TABLE IF EXISTS ozon.raw_events;
TRUNCATE TABLE IF EXISTS ozon.postings;
TRUNCATE TABLE IF EXISTS ozon.posting_items;
TRUNCATE TABLE IF EXISTS ozon.posting_geo;
TRUNCATE TABLE IF EXISTS ozon.products;
TRUNCATE TABLE IF EXISTS ozon.collector_runs;
"
```

Но этого недостаточно для новой исторической загрузки, потому что курсоры останутся в MinIO.

### Очистить сырой слой и курсоры в MinIO

Если нужно, чтобы `INITIAL_LOOKBACK_DAYS` применился заново, нужно удалить объекты MinIO, включая `_state/cursors.json`.

Команда:

```bash
docker compose run --rm --no-deps collector python - <<'PY'
from minio import Minio
from minio.error import S3Error
from src.config import settings

client = Minio(
    settings.minio_endpoint,
    access_key=settings.minio_access_key,
    secret_key=settings.minio_secret_key,
    secure=False,
)
try:
    objects = list(client.list_objects(settings.minio_bucket, recursive=True))
except S3Error as e:
    if e.code == 'NoSuchBucket':
        print('bucket_missing=1')
        raise SystemExit(0)
    raise
for obj in objects:
    client.remove_object(settings.minio_bucket, obj.object_name)
print(f'removed_objects={len(objects)}')
PY
```

### Правильный сценарий нового прогона с другим `INITIAL_LOOKBACK_DAYS`

1. Изменить `.env`:

```env
INITIAL_LOOKBACK_DAYS=30
```

2. Остановить сервисы, которые пишут данные:

```bash
docker compose stop collector consumer
```

3. Очистить ClickHouse таблицы.

4. Очистить бакет MinIO вместе с `_state/cursors.json`.

5. Пересоздать сервисы `collector` и `consumer`:

```bash
docker compose up -d --force-recreate collector consumer
```

6. Смотреть логи:

```bash
docker compose logs -f collector consumer
```