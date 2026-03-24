"""Produces mapped Shopify events to Kafka with Avro serialization.

Connects to Kafka and Schema Registry via localhost port-forwards:
  kubectl port-forward -n streaming svc/kafka 9092:9092
  kubectl port-forward -n streaming svc/schema-registry 8081:8081

Reuses the same Avro schema and serialization approach as the synthetic producer.
"""

import os
import sys
from pathlib import Path
from typing import Optional

from confluent_kafka import Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import (
    MessageField,
    SerializationContext,
    StringSerializer,
)

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
SCHEMA_REGISTRY_URL = os.getenv("SCHEMA_REGISTRY_URL", "http://localhost:8081")
TOPIC = os.getenv("KAFKA_TOPIC", "customer-events")

# Path to the shared Avro schema
SCHEMA_PATH = Path(__file__).resolve().parent.parent / "producer" / "schemas" / "customer_event.avsc"


def _event_to_dict(event, ctx):
    return event


class ShopifyKafkaProducer:
    """Kafka producer for Shopify-mapped events."""

    def __init__(self):
        self._producer = None
        self._avro_serializer = None
        self._string_serializer = None
        self._connected = False
        self._events_produced = 0
        self._errors = 0

    def connect(self) -> bool:
        """Initialize Kafka producer and Avro serializer."""
        try:
            with open(SCHEMA_PATH) as f:
                schema_str = f.read()

            registry = SchemaRegistryClient({"url": SCHEMA_REGISTRY_URL})
            self._avro_serializer = AvroSerializer(registry, schema_str, _event_to_dict)
            self._string_serializer = StringSerializer("utf_8")

            self._producer = Producer({
                "bootstrap.servers": KAFKA_BOOTSTRAP,
                "client.id": "klaflow-shopify-webhook",
                "linger.ms": 10,
                "socket.timeout.ms": 5000,
                "message.timeout.ms": 10000,
            })

            # Test connectivity by requesting metadata
            metadata = self._producer.list_topics(timeout=5)
            if TOPIC not in metadata.topics:
                print(f"  [WARN] Topic '{TOPIC}' not found. Available: {list(metadata.topics.keys())[:10]}")

            self._connected = True
            print(f"  [KAFKA] Connected to {KAFKA_BOOTSTRAP}")
            print(f"  [KAFKA] Schema Registry: {SCHEMA_REGISTRY_URL}")
            print(f"  [KAFKA] Topic: {TOPIC}")
            return True

        except Exception as e:
            print(f"  [KAFKA] Connection failed: {e}")
            self._connected = False
            return False

    def produce(self, event: dict) -> bool:
        """Produce a single mapped event to Kafka.

        The event dict must match the CustomerEvent Avro schema:
        {event_id, customer_id, account_id, event_type, timestamp, properties}
        """
        if not self._connected:
            return False

        try:
            self._producer.produce(
                topic=TOPIC,
                key=self._string_serializer(event["customer_id"]),
                value=self._avro_serializer(
                    event, SerializationContext(TOPIC, MessageField.VALUE)
                ),
                on_delivery=self._delivery_callback,
            )
            self._producer.poll(0)  # trigger callbacks
            return True
        except Exception as e:
            self._errors += 1
            print(f"  [KAFKA] Produce error: {e}")
            return False

    def flush(self, timeout: float = 5.0):
        """Flush pending messages."""
        if self._connected and self._producer:
            self._producer.flush(timeout)

    def _delivery_callback(self, err, msg):
        if err:
            self._errors += 1
            print(f"  [KAFKA] Delivery failed: {err}")
        else:
            self._events_produced += 1

    @property
    def stats(self) -> dict:
        return {
            "connected": self._connected,
            "events_produced": self._events_produced,
            "errors": self._errors,
            "bootstrap": KAFKA_BOOTSTRAP,
            "schema_registry": SCHEMA_REGISTRY_URL,
            "topic": TOPIC,
        }

    def disconnect(self):
        if self._connected and self._producer:
            self._producer.flush(10)
            self._connected = False
