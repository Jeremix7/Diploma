from kafka import KafkaConsumer
import json
from src.configs import settings
from typing import List


def get_kafka_data() -> List[int]:
    """
    Reads new messages from the Kafka topic and extracts user IDs.
    """
    consumer = KafkaConsumer(
        settings.TOPIC_NAME,
        bootstrap_servers=[settings.KAFKA_BROKER],
        group_id=settings.GROUP_ID,
        enable_auto_commit=True,
        auto_offset_reset="none",
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
    )

    ids = []
    messages = consumer.poll(timeout_ms=2000)

    for _, msg in messages.items():
        for message in msg:
            loan_data = message.value
            user_id = loan_data.get("id")
            if user_id is not None:
                ids.append(user_id)

    consumer.close()
    return list(ids)
