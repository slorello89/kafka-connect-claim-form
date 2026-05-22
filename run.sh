#!/usr/bin/env bash
# Bring up the stack and register the Redis sink connector.
set -euo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
CONNECT_URL="${CONNECT_URL:-http://localhost:8083}"
CONNECTOR_CONFIG="${HERE}/connector/claim-form-sink.json"

echo ">> Building and starting docker compose stack..."
docker compose -f "${HERE}/docker-compose.yml" up -d --build

echo ">> Waiting for Kafka Connect REST API at ${CONNECT_URL}..."
for i in $(seq 1 60); do
  if curl -fsS "${CONNECT_URL}/connectors" >/dev/null 2>&1; then
    echo "   Connect is up."
    break
  fi
  sleep 2
  if [[ "$i" -eq 60 ]]; then
    echo "!! Connect REST API did not come up in time" >&2
    exit 1
  fi
done

echo ">> Verifying redis-kafka-connect plugin is installed..."
if ! curl -fsS "${CONNECT_URL}/connector-plugins" \
  | grep -q "com.redis.kafka.connect.RedisSinkConnector"; then
  echo "!! RedisSinkConnector plugin not found" >&2
  curl -fsS "${CONNECT_URL}/connector-plugins" || true
  exit 1
fi
echo "   Plugin present."

echo ">> Registering claim-form sink connector..."
if curl -fsS "${CONNECT_URL}/connectors/claim-form-sink" >/dev/null 2>&1; then
  echo "   Connector already exists; updating config."
  CONFIG_ONLY=$(jq '.config' "${CONNECTOR_CONFIG}")
  curl -fsS -X PUT -H "Content-Type: application/json" \
    --data "${CONFIG_ONLY}" \
    "${CONNECT_URL}/connectors/claim-form-sink/config" >/dev/null
else
  curl -fsS -X POST -H "Content-Type: application/json" \
    --data @"${CONNECTOR_CONFIG}" \
    "${CONNECT_URL}/connectors" >/dev/null
fi

echo ">> Connector status:"
curl -fsS "${CONNECT_URL}/connectors/claim-form-sink/status" | jq .

cat <<'EOF'

>> All set. Try:
   docker compose logs -f producer
   docker exec -it redis redis-cli KEYS 'claim-form:*'
   docker exec -it redis redis-cli HGETALL claim-form:<message-id>
EOF
