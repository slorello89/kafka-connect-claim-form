"""Synthetic insurance claim producer.

Stands in for a downstream service that extracts structured claim facts from
agent/customer transcripts. A single conversation produces many Kafka messages
over its lifetime — each carrying a small subset of the claim form fields as
they get extracted. All messages from the same conversation share a
``conversation_id``; every individual emission has its own ``message_id``.

The sink uses ``conversation_id`` as the Redis hash key, so successive messages
HSET additional fields onto the same hash, building the claim form up
incrementally.

Each conversation is independently scheduled: phases emit roughly every
``PHASE_INTERVAL_SECONDS`` seconds (with jitter), so a full 5-phase form
hydrates in about ``4 * PHASE_INTERVAL`` seconds — bounded and predictable,
regardless of how many conversations run concurrently.
"""

import json
import os
import random
import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone

from faker import Faker
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

fake = Faker()

BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "kafka:9092")
TOPIC = os.environ.get("TOPIC", "claim-form")
NEW_CONVERSATION_INTERVAL = float(
    os.environ.get("NEW_CONVERSATION_INTERVAL_SECONDS", "4")
)
PHASE_INTERVAL = float(os.environ.get("PHASE_INTERVAL_SECONDS", "6"))
PHASE_JITTER = float(os.environ.get("PHASE_JITTER_SECONDS", "1"))
TICK_SECONDS = float(os.environ.get("TICK_SECONDS", "0.5"))

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


@dataclass
class Conversation:
    conv_id: str
    full_claim: dict
    next_group: int = 0
    next_emit_at: float = field(default=0.0)


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


def emit_phase(producer: KafkaProducer, conv: Conversation) -> None:
    fields = FACT_GROUPS[conv.next_group]
    payload = {f: conv.full_claim[f] for f in fields}
    payload["conversation_id"] = conv.conv_id
    payload["message_id"] = str(uuid.uuid4())
    payload["captured_at"] = datetime.now(timezone.utc).isoformat()
    producer.send(TOPIC, value=payload)
    producer.flush()
    print(
        f"conv={conv.conv_id[:8]} phase={conv.next_group + 1}/{len(FACT_GROUPS)} "
        f"fields={fields}",
        flush=True,
    )
    conv.next_group += 1


def main() -> None:
    producer = connect_producer()
    expected_total = PHASE_INTERVAL * (len(FACT_GROUPS) - 1)
    print(
        f"Producing to topic '{TOPIC}' on {BOOTSTRAP}. "
        f"new conv every {NEW_CONVERSATION_INTERVAL}s; "
        f"phase every {PHASE_INTERVAL}±{PHASE_JITTER}s; "
        f"expected per-conversation hydration ≈ {expected_total:.0f}s",
        flush=True,
    )
    active: list[Conversation] = []
    next_new_at = time.monotonic()

    while True:
        now = time.monotonic()

        if now >= next_new_at:
            conv = Conversation(
                conv_id=str(uuid.uuid4()),
                full_claim=generate_full_claim(),
                next_emit_at=now,
            )
            active.append(conv)
            next_new_at = now + NEW_CONVERSATION_INTERVAL

        still_active: list[Conversation] = []
        for conv in active:
            while (
                conv.next_group < len(FACT_GROUPS)
                and time.monotonic() >= conv.next_emit_at
            ):
                emit_phase(producer, conv)
                conv.next_emit_at = time.monotonic() + PHASE_INTERVAL + random.uniform(
                    -PHASE_JITTER, PHASE_JITTER
                )
            if conv.next_group < len(FACT_GROUPS):
                still_active.append(conv)
        active = still_active

        time.sleep(TICK_SECONDS)


if __name__ == "__main__":
    main()
