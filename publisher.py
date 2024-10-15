from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka import Producer
from confluent_kafka.serializing_producer import SerializationContext
from confluent_kafka.serialization import MessageField
from uuid import uuid4
import time

KAFKA_URL = ""
KAFKA_PRODUCERS = ""
ENABLE_CA_VALIDATION = False
SASL_MECHANISM = ""
SECURITY_PROTOCOL = ""
KAFKA_PRODUCER_SASL_USERNAME = ""
KAFKA_PRODUCER_SASL_PASSWORD = ""
KAFKA_RETRIES = ""
KAFKA_RETRIES_BACKOFF = ""
KAFKA_MESSAGE_TIMEOUT = ""
KAFKA_REQUEST_TIMEOUT = ""
SCHEMA = ""
MESSAGE_TO_PUBLISH = {}
TOPIC_NAME = ""


def __callback(err, msg):
    print("[Callback report]")
    print(f"[err]: {err}")
    print(f"[msg]: {msg}")


if __name__ == "__main__":
    schema_registry = SchemaRegistryClient({"url": KAFKA_URL})
    producer_conf = {
        "bootstrap.servers": KAFKA_PRODUCERS,
        "enable.ssl.certificate.verification": ENABLE_CA_VALIDATION,
        "sasl.mechanisms": SASL_MECHANISM,
        "security.protocol": SECURITY_PROTOCOL,
        "sasl.username": KAFKA_PRODUCER_SASL_USERNAME,
        "sasl.password": KAFKA_PRODUCER_SASL_PASSWORD,
        "retries": KAFKA_RETRIES,
        "retry.backoff.ms": KAFKA_RETRIES_BACKOFF,
        "message.timeout.ms": KAFKA_MESSAGE_TIMEOUT,
        "request.timeout.ms": KAFKA_REQUEST_TIMEOUT,
    }
    producer = Producer(producer_conf)
    serializer = AvroSerializer(
        schema_registry_client=schema_registry,
        schema_str=SCHEMA,
        conf={"auto.register.schemas": False, "use.latest.version": True},
    )

    headers = {
        "cid": uuid4(),
        "version": "0.1",
        "endOfLife": None,
        "type": "data",
    }

    message_serialized = serializer(
        MESSAGE_TO_PUBLISH, SerializationContext(TOPIC_NAME, MessageField.VALUE)
    )

    producer.produce(
        topic=TOPIC_NAME,
        key=None,
        value=message_serialized,
        headers=list(headers.items()),
        callback=__callback,
    )
    producer.poll(0)
    start = time.time()
    producer.flush()
    end = time.time()

    print(f"[Flush time]: {end - start}")
