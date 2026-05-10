import orjson
import structlog
from confluent_kafka import Producer

log = structlog.get_logger()


class KafkaSink:
    def __init__(self, bootstrap: str):
        self._producer = Producer(
            {
                "bootstrap.servers": bootstrap,
                "client.id": "shr-collector",
                "linger.ms": 50,
                "compression.type": "zstd",
                "enable.idempotence": True,
            }
        )

    def send(self, topic: str, key: str, value: dict) -> None:
        self._producer.produce(
            topic=topic,
            key=key.encode("utf-8"),
            value=orjson.dumps(value),
            on_delivery=self._on_delivery,
        )
        self._producer.poll(0)

    def flush(self, timeout: float = 10.0) -> None:
        self._producer.flush(timeout)

    @staticmethod
    def _on_delivery(err, msg) -> None:
        if err is not None:
            log.error("kafka.delivery_failed", error=str(err), topic=msg.topic())