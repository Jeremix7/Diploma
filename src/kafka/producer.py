import json
import random
from kafka import KafkaProducer
from src.configs import settings


def generate_loan_request() -> dict:
    """Generate a random loan request with user ID, loan amount, and loan purpose."""
    purpose = [
        "moving",
        "house",
        "medical",
        "vacation",
        "debt_consolidation",
        "credit_card",
        "other",
        "renewable_energy",
        "major_purchase",
        "home_improvement",
        "small_business",
        "car",
    ]
    return {
        "id": random.randint(1, 111878),
        "loan_amnt": random.randint(1000, 40000),
        "purpose": random.choice(purpose),
    }


def send_loan_requests() -> None:
    """Send a random number of loan requests to a Kafka topic."""
    producer = KafkaProducer(
        bootstrap_servers=settings.KAFKA_BROKER,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )

    count = random.randint(40, 200)
    for _ in range(count):
        loan_request = generate_loan_request()
        producer.send(settings.TOPIC_NAME, value=loan_request)

    producer.flush()
    producer.close()
