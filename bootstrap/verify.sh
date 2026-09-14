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
#   6 every port the stack contract marks `live` actually answers
#   7 an undeliverable payload is retained in the DLQ, not lost
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

# ─── 6. contract ports match reality ─────────────────────────────────────────
# The Ports table in docs/stack-contract.md is what four other repos resolve
# addresses from, and until now nothing stopped it drifting from the stack it
# describes. This check reads the table and probes it, in both directions:
#
#   live                          must answer, or the suite FAILS
#   live (lite) / internal        skipped, with the reason printed
#   contracted-not-yet-listening  skipped, but if it DOES answer that is
#                                 reported as drift — the listener landed and
#                                 the row was never flipped to `live`
#
# Skipping is never silent: a row that is not checked says why it was not.
# See "Changing this contract" in docs/stack-contract.md.
head_ "6. Contract ports match the running stack"

CONTRACT=docs/stack-contract.md
TAB=$(printf '\t')

# service <TAB> in-network <TAB> host <TAB> status, one line per Ports row.
# ANY heading closes the section, "###" included: the prose subsections under
# "## Ports" contain tables of their own (the consumer address-swap table, for
# one) and those rows are not ports. Only the table directly under the heading
# is the contract.
rows=$(awk -F'|' '
  /^## Ports/ { in_s=1; next }
  /^#/        { in_s=0 }
  !in_s       { next }
  /^\|/ {
    svc=$2; net=$3; host=$4; st=$5
    gsub(/`|\*/, "", svc); gsub(/`|\*/, "", net)
    gsub(/`|\*/, "", host); gsub(/`|\*/, "", st)
    gsub(/^[ \t]+|[ \t]+$/, "", svc); gsub(/^[ \t]+|[ \t]+$/, "", net)
    gsub(/^[ \t]+|[ \t]+$/, "", host); gsub(/^[ \t]+|[ \t]+$/, "", st)
    if (svc == "" || svc == "Service" || svc ~ /^-+$/) next
    printf "%s\t%s\t%s\t%s\n", svc, net, host, st
  }' "$CONTRACT")

if [ -z "$rows" ]; then
  no "could not parse the Ports table in $CONTRACT — check 6 proves nothing"
else
  # Which compose services are actually up, across every profile. A row for a
  # service this profile does not run is skipped, not failed (same shape as
  # check 5), so `core` and `lite` runs stay meaningful.
  running=" $(docker compose -f compose/compose.yaml \
                --profile full --profile core --profile lite ps \
                --format '{{.Service}}' 2>/dev/null | tr '\n' ' ') "

  # Collect in-network targets first so they cost ONE container, not one each.
  targets=""
  while IFS="$TAB" read -r svc net host st; do
    [ -n "$svc" ] || continue
    case "$st" in live|"live (lite)"|contracted-not-yet-listening) ;; *) continue ;; esac
    case "$net" in *:*) ;; *) continue ;; esac
    case "$running" in *" ${net%%:*} "*) targets="$targets $net" ;; esac
  done <<EOF
$rows
EOF

  # bash 3.2 (macOS) has no associative arrays, so grep the batch probe output.
  # /dev/tcp rather than nc: nc is not on every host, and every target here is
  # loopback or a docker network, where a refusal comes back immediately.
  reach() { printf '%s\n' "$netout" | grep -qx "OK $1"; }
  hostup() {
    case "$1" in *:*) ;; *) return 1 ;; esac
    (exec 3<>"/dev/tcp/${1%:*}/${1##*:}") 2>/dev/null
  }

  # Probe, classify, and retry the whole pass if anything live looks down.
  # Check 4 restarts a broker immediately before this one, and a container that
  # is running is not yet a process that has bound its port — without the retry
  # this check would flake on exactly the failure it exists to rule out.
  broken=""; drift=""; table=""
  attempt=1
  while : ; do
    broken=""; drift=""; table=""

    netout=""
    if [ -n "$targets" ]; then
      netout=$(docker run --rm --network "$NET" --entrypoint bash "$KAFKA_IMAGE" -c '
        for t in "$@"; do
          h=${t%%:*}; p=${t##*:}
          if (exec 3<>"/dev/tcp/$h/$p") 2>/dev/null; then echo "OK $t"; else echo "NO $t"; fi
        done' bash $targets 2>/dev/null)
    fi

    while IFS="$TAB" read -r svc net host st; do
      [ -n "$svc" ] || continue
      cs=${net%%:*}
      detail=""
      case "$st" in
        live|"live (lite)")
          case "$running" in
            *" $cs "*)
              if reach "$net" && hostup "$host"; then
                detail="ok: $net, $host — both answer"
              elif reach "$net"; then
                detail="FAIL: $host UNREACHABLE ($net ok)"
                broken="$broken $svc"
              elif hostup "$host"; then
                detail="FAIL: $net UNREACHABLE ($host ok)"
                broken="$broken $svc"
              else
                detail="FAIL: neither $net nor $host answers"
                broken="$broken $svc"
              fi
              ;;
            *) detail="skip: $cs not running on this profile" ;;
          esac
          ;;
        internal)
          detail="skip: in-network only, no host port to probe"
          ;;
        contracted-not-yet-listening)
          if reach "$net" || hostup "$host"; then
            detail="DRIFT: $net answers — flip this row to live"
            drift="$drift $svc"
          else
            detail="skip: nothing answers on $net yet (expected)"
          fi
          ;;
        *)
          detail="skip: unrecognised status '$st'"
          ;;
      esac
      table="$table$(printf '  %-17s  %-29s  %s' "$svc" "$st" "$detail")
"
    done <<EOF
$rows
EOF

    if [ -z "$broken" ] || [ "$attempt" -ge 3 ]; then break; fi
    echo "  (retrying$broken — may still be starting)"
    attempt=$((attempt + 1))
    sleep 3
  done

  printf '  %-17s  %-29s  %s\n' SERVICE STATUS DETAIL
  printf '%s' "$table"

  if [ -n "$broken" ]; then
    no "live-marked contract ports unreachable:$broken"
  else
    ok "every live-marked contract port answers; non-live rows skipped with a reason"
  fi
  if [ -n "$drift" ]; then
    printf '  \033[33mNOTE\033[0m  contracted-not-yet-listening but answering:%s\n' "$drift"
    printf '        The listener landed. Update %s in the same change.\n' "$CONTRACT"
  fi
fi

# ─── 7. undeliverable payloads reach the DLQ ─────────────────────────────────
# The gateway's DLQ used to log an event id and discard the bytes, so nothing it
# gave up on was recoverable (its finding F-002). Since T-1.2 it retains the
# whole envelope on `ingestion-dlq`, and since P2-C C-1 a log rejected by the
# list's Lua cap is dead-lettered rather than dropped outright.
#
# Asserting the key merely exists would prove nothing — it is empty on a healthy
# stack. So this check INDUCES a real failure: fill `ingestion-logs` to its cap,
# post one log, and confirm the payload is retained with the right reason.
#
# This check mutates `ingestion-logs` and restores its depth afterwards. It is
# the only destructive check in the suite; it runs last for that reason.
head_ "7. Undeliverable payload is retained in the DLQ"

REDIS="docker exec pulse-redis redis-cli -a pulse-local-not-a-secret --no-auth-warning"

if [ -z "$(docker ps -q -f name=pulse-gateway)" ]; then
  echo "  gateway not running (profile core/lite) — skipping"
elif ! docker exec pulse-gateway env 2>/dev/null | grep -q '^REDIS_LIST_DLQ='; then
  # Fail rather than skip: a running gateway that does not know about the DLQ is
  # an image older than the config it was started with, which is exactly the
  # drift this suite exists to catch.
  no "gateway has no REDIS_LIST_DLQ in its environment — the image predates T-1.2; rebuild it"
  echo "        docker compose -f compose/compose.yaml --profile full up -d --build gateway"
else
  DLQ_KEY=$(docker exec pulse-gateway env | sed -n 's/^REDIS_LIST_DLQ=//p' | tr -d '\r')
  LOG_KEY=$(docker exec pulse-gateway env | sed -n 's/^REDIS_LIST_LOGS=//p' | tr -d '\r')
  LOG_CAP=$(docker exec pulse-gateway env | sed -n 's/^REDIS_LIST_LOGS_MAX_LEN=//p' | tr -d '\r')
  echo "  dlq=$DLQ_KEY  logs=$LOG_KEY  cap=$LOG_CAP"

  dlq_before=$($REDIS LLEN "$DLQ_KEY" 2>/dev/null | tr -d '\r'); dlq_before=${dlq_before:-0}
  log_before=$($REDIS LLEN "$LOG_KEY" 2>/dev/null | tr -d '\r'); log_before=${log_before:-0}

  # Fill to the cap in one server-side loop rather than thousands of round trips.
  need=$(( LOG_CAP - log_before ))
  if [ "$need" -gt 0 ]; then
    # docker exec needs -i here: without it stdin is not forwarded and
    # redis-cli --eval silently reads an empty script, filling nothing.
    docker exec -i pulse-redis redis-cli -a pulse-local-not-a-secret --no-auth-warning \
      --eval /dev/stdin "$LOG_KEY" , "$need" >/dev/null 2>&1 <<'LUA'
for i = 1, tonumber(ARGV[1]) do
  redis.call('RPUSH', KEYS[1], '{"_pulse_infra_verify_filler":true}')
end
return 1
LUA
  fi
  echo "  LLEN $LOG_KEY: $log_before -> $($REDIS LLEN "$LOG_KEY" 2>/dev/null | tr -d '\r') (cap $LOG_CAP)"

  LOGP='{"event_id":"01JVERIFYDLQ00000000001","timestamp":"2026-01-01T00:00:00.000Z","severity":"ERROR","message":"pulse-infra-verify dlq induction"}'
  lcode=$(curl -s -o /tmp/pulse-verify-dlq -w '%{http_code}' \
    -X POST http://localhost:8080/telemetry/logs/v1 \
    -H 'Content-Type: application/json' \
    -H 'x-client-id: pulse_infra_smoke' \
    -H 'x-api-key: local-not-a-secret-smoke-key' \
    -d "$LOGP" 2>/dev/null)
  echo "  POST /telemetry/logs/v1 (list at cap) -> HTTP $lcode"

  # 202 is correct here and is not a contradiction: the list is full, not the
  # in-memory buffer. Saturation of the BUFFER is what returns 503; a backend
  # that rejects the write afterwards is a delivery failure, and delivery
  # failures are what the DLQ is for.
  if [ "$lcode" = "202" ]; then
    ok "gateway accepted the log (202 — the buffer had room; the backend rejects it later)"
  else
    no "gateway returned $lcode (expected 202)"
  fi

  # Worker flush + retry budget. WORKER_FLUSH_MS is small, but a LOGS_LIST_FULL
  # is routed as an invalid payload, so it is dead-lettered without retrying.
  sleep 3
  dlq_after=$($REDIS LLEN "$DLQ_KEY" 2>/dev/null | tr -d '\r'); dlq_after=${dlq_after:-0}
  echo "  LLEN $DLQ_KEY: $dlq_before -> $dlq_after"

  if [ "$dlq_after" -gt "$dlq_before" ]; then
    ok "undeliverable payload was retained in $DLQ_KEY"
  else
    no "nothing reached $DLQ_KEY — the payload was lost, not dead-lettered"
  fi

  newest_dlq=$($REDIS LRANGE "$DLQ_KEY" -1 -1 2>/dev/null)
  echo "  --- newest DLQ entry ---"
  printf '%s\n' "$newest_dlq" | sed 's/^/  /' | head -6

  # The reason is what makes an entry triageable, and it is a metric label too.
  if printf '%s' "$newest_dlq" | grep -q 'logs_list_full'; then
    ok "DLQ entry carries error_reason=logs_list_full"
  else
    no "DLQ entry has no logs_list_full reason — cannot be triaged or alerted on"
  fi

  # The envelope must be whole, not a summary: replay depends on it.
  if printf '%s' "$newest_dlq" | grep -q '"payload"' && printf '%s' "$newest_dlq" | grep -q 'gateway_id'; then
    ok "DLQ entry retains the full envelope (payload + gateway_id) — replayable"
  else
    no "DLQ entry is missing payload or gateway_id — not replayable"
  fi

  # Restore the log list to the depth we found it at.
  #
  # Note LTRIM 0 -1 keeps EVERYTHING rather than nothing, so the log_before=0
  # case cannot be expressed as a trim and needs the DEL.
  if [ "$need" -gt 0 ]; then
    if [ "$log_before" -eq 0 ]; then
      $REDIS DEL "$LOG_KEY" >/dev/null 2>&1
    else
      $REDIS LTRIM "$LOG_KEY" 0 $(( log_before - 1 )) >/dev/null 2>&1
    fi
    echo "  restored LLEN $LOG_KEY -> $($REDIS LLEN "$LOG_KEY" 2>/dev/null | tr -d '\r') (was $log_before)"
  fi
fi

head_ "Summary"
printf '  %d passed, %d failed\n\n' "$pass" "$fail"
[ "$fail" -eq 0 ]
