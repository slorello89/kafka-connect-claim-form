"""Synthetic insurance claim producer.

Stands in for a downstream service that extracts structured claim facts from
agent/customer transcripts. A single conversation produces many Kafka messages
over its lifetime — each carrying a small subset of the claim form fields as
they get extracted. All messages from the same conversation share a
``conversation_id``; every individual emission has its own ``message_id``.

The sink uses ``conversation_id`` as the Redis hash key, so successive messages
HSET additional fields onto the same hash, building the claim form up
incrementally.
"""

import json
import os
import random
import time
import uuid
from datetime import datetime, timezone

from faker import Faker
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

fake = Faker()

BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "kafka:9092")
TOPIC = os.environ.get("TOPIC", "claim-form")
INTERVAL = float(os.environ.get("INTERVAL_SECONDS", "3"))
NEW_CONVERSATION_PROB = float(os.environ.get("NEW_CONVERSATION_PROB", "0.25"))

INCIDENT_TYPES = [
    "rear-end collision",
    "side-swipe",
    "hail damage",
    "kitchen fire",
    "water leak from upstairs unit",
    "slip and fall in parking lot",
    "tree fell on garage",
    "theft of personal property",
]

# Facts are emitted in groups, simulating progressive extraction from the
# transcript. Each group becomes one Kafka message.
FACT_GROUPS = [
    ["claimant_name", "contact_phone"],
    ["date_of_birth", "address"],
    ["policy_number"],
    ["incident_type", "date_of_incident"],
    ["incident_description", "claim_amount"],
]


def connect_producer() -> KafkaProducer:
    while True:
        try:
            return KafkaProducer(
                bootstrap_servers=BOOTSTRAP,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                acks="all",
                linger_ms=50,
            )
        except NoBrokersAvailable:
            print(f"Kafka not ready at {BOOTSTRAP}, retrying in 3s...", flush=True)
            time.sleep(3)


def generate_full_claim() -> dict:
    incident = random.choice(INCIDENT_TYPES)
    return {
        "claimant_name": fake.name(),
        "date_of_birth": fake.date_of_birth(
            minimum_age=18, maximum_age=85
        ).isoformat(),
        "date_of_incident": fake.date_between(
            start_date="-1y", end_date="today"
        ).isoformat(),
        "incident_type": incident,
        "incident_description": (
            f"Caller reports {incident}. {fake.sentence(nb_words=18)}"
        ),
        "policy_number": f"POL-{fake.bothify('##########')}",
        "claim_amount": round(random.uniform(500, 50000), 2),
        "contact_phone": fake.phone_number(),
        "address": fake.address().replace("\n", ", "),
    }


def start_conversation(active: dict) -> str:
    conv_id = str(uuid.uuid4())
    active[conv_id] = {
        "full_claim": generate_full_claim(),
        "next_group": 0,
    }
    return conv_id


def pick_conversation(active: dict) -> str:
    """Return an existing conversation id, or start a new one."""
    if not active or random.random() < NEW_CONVERSATION_PROB:
        return start_conversation(active)
    return random.choice(list(active.keys()))


def build_message(active: dict, conv_id: str) -> dict:
    state = active[conv_id]
    fields = FACT_GROUPS[state["next_group"]]
    payload = {field: state["full_claim"][field] for field in fields}
    payload["conversation_id"] = conv_id
    payload["message_id"] = str(uuid.uuid4())
    payload["captured_at"] = datetime.now(timezone.utc).isoformat()
    state["next_group"] += 1
    if state["next_group"] >= len(FACT_GROUPS):
        del active[conv_id]
    return payload


def main() -> None:
    producer = connect_producer()
    print(
        f"Producing to topic '{TOPIC}' on {BOOTSTRAP} every {INTERVAL}s "
        f"(new-conversation prob={NEW_CONVERSATION_PROB})",
        flush=True,
    )
    active: dict[str, dict] = {}
    while True:
        conv_id = pick_conversation(active)
        message = build_message(active, conv_id)
        producer.send(TOPIC, value=message)
        producer.flush()
        fields = [k for k in message if k not in {"conversation_id", "message_id", "captured_at"}]
        print(
            f"conv={conv_id[:8]} message_id={message['message_id'][:8]} "
            f"fields={fields}",
            flush=True,
        )
        time.sleep(INTERVAL)


if __name__ == "__main__":
    main()
