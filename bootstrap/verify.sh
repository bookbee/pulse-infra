#!/usr/bin/env bash
# pulse-infra verification suite.
#
# Runs against a live `full` stack and proves the contract the consumer repos
# code against. Every check prints what it actually observed.
#
#   1 stack is up and healthy
#   2 create a topic; produce and consume a message
#   3 write an object to the bucket at the agreed path; read it back
#   4 kill one broker; confirm the cluster stays writable
#   5 gateway accepts a telemetry event and it lands in Redis
#
# Cold start, reset, and reproducibility (brief steps 1, 5, 6) are lifecycle
# operations driven from the Makefile — see README "Verification".
set -uo pipefail

cd "$(dirname "$0")/.."

KAFKA_IMAGE='apache/kafka@sha256:77e3df9054047a88b520d0cc46e16696d3b22022e1d580aeccd2632df6532837'
NET=pulse-infra
BOOTSTRAP=kafka-1:9092,kafka-2:9092,kafka-3:9092
BUCKET=pulse-staging-local
GCS=http://fake-gcs:4443

pass=0; fail=0
ok()   { printf '  \033[32mPASS\033[0m  %s\n' "$*"; pass=$((pass+1)); }
no()   { printf '  \033[31mFAIL\033[0m  %s\n' "$*"; fail=$((fail+1)); }
head_() { printf '\n\033[1m%s\033[0m\n' "$*"; }

kexec() { docker run --rm --network "$NET" "$KAFKA_IMAGE" "$@"; }
wget_() { docker run --rm --network "$NET" --entrypoint wget "$KAFKA_IMAGE" "$@"; }

# ─── 1. health ───────────────────────────────────────────────────────────────
head_ "1. Stack health"
docker compose -f compose/compose.yaml --profile full ps \
  --format '{{.Name}}\t{{.State}}\t{{.Status}}' | sort | sed 's/^/  /'
unhealthy=$(docker compose -f compose/compose.yaml --profile full ps \
  --format '{{.Name}} {{.Status}}' | grep -c 'unhealthy' || true)
if [ "${unhealthy:-0}" -eq 0 ]; then ok "no unhealthy containers"; else no "$unhealthy unhealthy container(s)"; fi

# ─── 2. topic create + produce + consume ─────────────────────────────────────
head_ "2. Topic create, produce, consume"
T="verify-$(date +%s)"
if kexec /opt/kafka/bin/kafka-topics.sh --bootstrap-server "$BOOTSTRAP" \
     --create --topic "$T" --partitions 3 --replication-factor 3 >/dev/null 2>&1; then
  ok "created topic $T (partitions=3 rf=3)"
else
  no "could not create topic $T"
fi

MSG="pulse-verify-payload-$(date +%s)"
if printf '%s\n' "$MSG" | docker run --rm -i --network "$NET" "$KAFKA_IMAGE" \
     /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server "$BOOTSTRAP" \
     --topic "$T" >/dev/null 2>&1; then
  ok "produced 1 message"
else
  no "produce failed"
fi

got=$(kexec /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server "$BOOTSTRAP" \
        --topic "$T" --from-beginning --max-messages 1 --timeout-ms 20000 2>/dev/null | tr -d '\r\n')
if [ "$got" = "$MSG" ]; then ok "consumed the same message back"; else no "consume mismatch: got '${got}'"; fi

echo "  --- topic describe ---"
kexec /opt/kafka/bin/kafka-topics.sh --bootstrap-server "$BOOTSTRAP" --describe --topic "$T" 2>/dev/null | sed 's/^/  /'
kexec /opt/kafka/bin/kafka-topics.sh --bootstrap-server "$BOOTSTRAP" --delete --topic "$T" >/dev/null 2>&1

# ─── 3. bucket write + read at the agreed path ───────────────────────────────
head_ "3. GCS staging object at the agreed path"
OBJ="ingestion-events/dt=$(date -u +%Y-%m-%d)/0-0-99.parquet"
ENC=$(printf '%s' "$OBJ" | sed 's|/|%2F|g')
if wget_ -q -O- --header 'Content-Type: application/octet-stream' \
     --post-data 'PAR1-fake-parquet-bytes-for-verification' \
     "${GCS}/upload/storage/v1/b/${BUCKET}/o?uploadType=media&name=${ENC}" >/dev/null 2>&1; then
  ok "wrote gs://${BUCKET}/${OBJ}"
else
  no "write failed for gs://${BUCKET}/${OBJ}"
fi

back=$(wget_ -q -O- "${GCS}/storage/v1/b/${BUCKET}/o/${ENC}?alt=media" 2>/dev/null || true)
if [ "$back" = 'PAR1-fake-parquet-bytes-for-verification' ]; then
  ok "read the same bytes back"
else
  no "read-back mismatch: got '${back}'"
fi
echo "  --- objects in bucket ---"
wget_ -q -O- "${GCS}/storage/v1/b/${BUCKET}/o" 2>/dev/null | sed 's/,/,\n/g' | grep -E '"name"|"size"' | sed 's/^/  /' || true

# ─── 4. broker loss ──────────────────────────────────────────────────────────
head_ "4. Broker loss — cluster stays writable"
echo "  stopping pulse-kafka-3"
docker stop pulse-kafka-3 >/dev/null 2>&1
T2="verify-degraded-$(date +%s)"
# RF=3 min.insync.replicas=2: 2 of 3 replicas still in sync, so writes continue.
kexec /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka-1:9092,kafka-2:9092 \
  --create --topic "$T2" --partitions 3 --replication-factor 3 \
  --config min.insync.replicas=2 >/dev/null 2>&1
if printf 'written-with-one-broker-down\n' | docker run --rm -i --network "$NET" "$KAFKA_IMAGE" \
     /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server kafka-1:9092,kafka-2:9092 \
     --producer-property acks=all --topic "$T2" >/dev/null 2>&1; then
  ok "produced with acks=all while one broker is down"
else
  no "produce with acks=all failed while one broker is down"
fi
got2=$(docker run --rm --network "$NET" "$KAFKA_IMAGE" \
        /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server kafka-1:9092,kafka-2:9092 \
        --topic "$T2" --from-beginning --max-messages 1 --timeout-ms 20000 2>/dev/null | tr -d '\r\n')
if [ "$got2" = 'written-with-one-broker-down' ]; then ok "read it back from the degraded cluster"; else no "degraded read failed: '${got2}'"; fi
echo "  --- under-replicated partitions (expected: some) ---"
kexec /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka-1:9092,kafka-2:9092 \
  --describe --under-replicated-partitions 2>/dev/null | sed 's/^/  /' | head -8
echo "  restarting pulse-kafka-3"
docker start pulse-kafka-3 >/dev/null 2>&1
kexec /opt/kafka/bin/kafka-topics.sh --bootstrap-server "$BOOTSTRAP" --delete --topic "$T2" >/dev/null 2>&1 || true

# ─── 5. gateway → Redis ──────────────────────────────────────────────────────
head_ "5. Gateway ingest lands in Redis"
if [ -n "$(docker ps -q -f name=pulse-gateway)" ]; then
  # API-key auth needs BOTH x-api-key AND x-client-id: the client id keys the
  # store, and a missing one is a flat 401. See docs/stack-contract.md.
  EVENT='{"event_id":"01JVERIFY000000000000001","timestamp":"2026-01-01T00:00:00.000Z","type":"track","event":"pulse_infra_verify","user":{"user_id":"verify-user"},"context":{"app":{"name":"pulse-infra-verify","version":"1.0.0"}},"properties":{"source":"pulse-infra-verify"}}'
  before=$(docker exec pulse-redis redis-cli -a pulse-local-not-a-secret --no-auth-warning \
             XLEN ingestion-events 2>/dev/null | tr -d '\r')
  code=$(curl -s -o /tmp/pulse-verify-resp -w '%{http_code}' \
    -X POST http://localhost:8080/telemetry/events/v1 \
    -H 'Content-Type: application/json' \
    -H 'x-client-id: pulse_infra_smoke' \
    -H 'x-api-key: local-not-a-secret-smoke-key' \
    -d "$EVENT" 2>/dev/null)
  echo "  POST /telemetry/events/v1 (api key) -> HTTP $code"
  echo "  response: $(cat /tmp/pulse-verify-resp 2>/dev/null | head -c 200)"
  if [ "$code" = "202" ]; then ok "gateway accepted the event (api key)"; else no "gateway returned $code (expected 202)"; fi

  # JWT path: same event, and the envelope should now carry event_header.
  TOKEN=$(./bootstrap/mint-dev-jwt.sh 600 pulse-infra-verify 2>/dev/null)
  jcode=$(curl -s -o /tmp/pulse-verify-jwt -w '%{http_code}' \
    -X POST http://localhost:8080/telemetry/events/v1 \
    -H 'Content-Type: application/json' \
    -H "Authorization: Bearer ${TOKEN}" \
    -d "$EVENT" 2>/dev/null)
  echo "  POST /telemetry/events/v1 (jwt)     -> HTTP $jcode"
  if [ "$jcode" = "202" ]; then ok "gateway accepted the event (minted JWT)"; else no "JWT request returned $jcode (expected 202)"; fi
  sleep 1
  depth=$(docker exec pulse-redis redis-cli -a pulse-local-not-a-secret --no-auth-warning \
            XLEN ingestion-events 2>/dev/null | tr -d '\r')
  echo "  XLEN ingestion-events: ${before:-0} -> ${depth:-<none>}"
  if [ "$(( ${depth:-0} - ${before:-0} ))" -ge 2 ]; then
    ok "both events landed in the Redis stream"
  else
    no "expected 2 new entries in ingestion-events, saw $(( ${depth:-0} - ${before:-0} ))"
  fi

  # event_header is populated for JWT requests and ABSENT for api-key ones.
  # That asymmetry is contract, not a bug — see docs/stack-contract.md.
  newest=$(docker exec pulse-redis redis-cli -a pulse-local-not-a-secret --no-auth-warning \
             XREVRANGE ingestion-events + - COUNT 2 2>/dev/null)
  if printf '%s' "$newest" | grep -q 'event_header'; then
    ok "JWT envelope carries event_header"
  else
    no "no event_header on the JWT envelope"
  fi
  echo "  --- newest stream entry ---"
  printf '%s\n' "$newest" | sed 's/^/  /' | head -8
  echo "  --- gateway readiness ---"
  curl -s http://localhost:8080/readyz | sed 's/^/  /'; echo
else
  echo "  gateway not running (profile core/lite) — skipping"
fi

head_ "Summary"
printf '  %d passed, %d failed\n\n' "$pass" "$fail"
[ "$fail" -eq 0 ]
