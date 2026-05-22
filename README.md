# redis-kafka-connect claim-form sandbox

A minimal sandbox to exercise [redis-kafka-connect](https://github.com/redis-field-engineering/redis-kafka-connect).

## What runs

| Component | Role |
|-----------|------|
| `producer` | Python service that publishes synthetic insurance claim JSON to the `claim-form` Kafka topic every few seconds. A single **conversation** produces multiple messages over time; each one carries a small subset of the claim fields (claimant name + phone, then DOB + address, then policy #, then incident details, etc.) — standing in for facts as they get extracted from an ongoing transcript. Every message carries a shared `conversation_id` plus its own unique `message_id`. |
| `kafka` | Apache Kafka 3.7 in KRaft (single-node, no zookeeper). |
| `connect` | Confluent Kafka Connect with the `redis/redis-kafka-connect` plugin installed at image build time. |
| `redis` | `redis/redis-stack` as the sink — also exposes RedisInsight on `:8001`. |
| `frontend` | FastAPI + a single-page UI on `:8080` that lists conversations and lets you click into one to see its claim form fill out in near-real-time. |

## How the message becomes a hash

The producer dribbles out per-conversation messages like this — same `conversation_id`, different `message_id`, a couple of new fields each time:

```json
{ "conversation_id": "a1b2…", "message_id": "5f9c…", "claimant_name": "Jane Doe", "contact_phone": "555-…" }
{ "conversation_id": "a1b2…", "message_id": "771e…", "date_of_birth": "1984-02-11", "address": "…" }
{ "conversation_id": "a1b2…", "message_id": "9c0d…", "policy_number": "POL-…" }
{ "conversation_id": "a1b2…", "message_id": "ff23…", "incident_type": "kitchen fire", "date_of_incident": "2026-03-04" }
{ "conversation_id": "a1b2…", "message_id": "2200…", "incident_description": "…", "claim_amount": 12345.67 }
```

The Kafka record key is left empty. The sink config promotes `conversation_id` from the value into the record key via two Single-Message Transforms:

1. `org.apache.kafka.connect.transforms.ValueToKey` — copies the `conversation_id` field from the value into a new struct/map key.
2. `org.apache.kafka.connect.transforms.ExtractField$Key` — flattens that struct down to the scalar `conversation_id` string.

The Redis sink then concatenates `redis.key` (the template `${topic}`) + `redis.separator` (`:`) + record-key, producing one hash per conversation:

```
claim-form:a1b2…
```

`redis.command=HSET` tells the connector to map every top-level field in the JSON value to a hash field on that key. Because subsequent messages from the same conversation hit the same hash key, each `HSET` merges its new fields in — so you can watch the claim form fill out in Redis as the transcript progresses.

## Usage

```bash
./run.sh
```

That builds the Connect image (one-time plugin install), starts everything, waits for Connect to be healthy, and registers `connector/claim-form-sink.json`.

### Verify

```bash
# tail the producer
docker compose logs -f producer

# list claim hashes
docker exec -it redis redis-cli --scan --pattern 'claim-form:*' | head

# inspect one
docker exec -it redis redis-cli HGETALL claim-form:<message_id>

# connector status
curl -s localhost:8083/connectors/claim-form-sink/status | jq .
```

Or open the frontend at <http://localhost:8080> to browse conversations interactively, or RedisInsight at <http://localhost:8001>.

### Tear down

```bash
docker compose down -v
```

## Tuning

- `INTERVAL_SECONDS` on the `producer` service controls publish cadence.
- Edit `connector/claim-form-sink.json` and re-run `./run.sh` to push config updates (the script does a `PUT /config` when the connector already exists).
