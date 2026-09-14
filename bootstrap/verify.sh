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
#   5 Redis honours its contract: auth, both structures, noeviction
#   6 every port the stack contract marks `live` actually answers
#   7 everything in the dependency registry is actually provisioned
#
# NOTHING HERE DRIVES A CONSUMER. This stack provides backing services; proving
# that a gateway or an ingestor behaves correctly is that repo's suite, not this
# one. If a check needs another project running, it does not belong here.
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

# ─── 5. Redis works as promised ──────────────────────────────────────────────
# This stack PROVIDES Redis; it does not own what any project writes into it.
# So this check exercises Redis itself against the guarantees the contract makes
# — auth, both structures registered against it, and the noeviction policy that
# consumers' error models depend on. It never drives a consumer's API.
head_ "5. Redis honours the contract"
if [ -z "$(docker ps -q -f name=pulse-redis)" ]; then
  echo "  redis not running (profile core/lite) — skipping"
else
  R="docker exec pulse-redis redis-cli -a pulse-local-not-a-secret --no-auth-warning"

  # Auth is contract: an unauthenticated client must be refused, not served.
  if docker exec pulse-redis redis-cli PING 2>&1 | grep -qi 'NOAUTH\|auth'; then
    ok "unauthenticated clients are refused (requirepass in force)"
  else
    no "Redis answered an unauthenticated PING — requirepass is not in force"
  fi

  # A stream, the structure registered for events/signals.
  SK="__pulse_infra_verify_stream"
  $R DEL "$SK" >/dev/null 2>&1
  $R XADD "$SK" '*' data '{"probe":"pulse-infra-verify"}' >/dev/null 2>&1
  if [ "$($R XLEN "$SK" 2>/dev/null | tr -d '\r')" = "1" ] &&
     $R XRANGE "$SK" - + 2>/dev/null | grep -q 'pulse-infra-verify'; then
    ok "stream XADD/XRANGE round-trips a one-field \`data\` entry"
  else
    no "stream round-trip failed on $SK"
  fi
  $R DEL "$SK" >/dev/null 2>&1

  # A list, the structure registered for logs/dlq.
  LK="__pulse_infra_verify_list"
  $R DEL "$LK" >/dev/null 2>&1
  $R RPUSH "$LK" '{"probe":"pulse-infra-verify"}' >/dev/null 2>&1
  if $R LRANGE "$LK" 0 -1 2>/dev/null | grep -q 'pulse-infra-verify'; then
    ok "list RPUSH/LRANGE round-trips an entry"
  else
    no "list round-trip failed on $LK"
  fi
  $R DEL "$LK" >/dev/null 2>&1

  # noeviction is the load-bearing one: consumers are promised that a write
  # fails loudly rather than a key being evicted from under them.
  pol=$($R CONFIG GET maxmemory-policy 2>/dev/null | tr -d '\r' | tail -1)
  mem=$($R CONFIG GET maxmemory 2>/dev/null | tr -d '\r' | tail -1)
  echo "  maxmemory=$mem maxmemory-policy=$pol"
  if [ "$pol" = "noeviction" ]; then
    ok "maxmemory-policy is noeviction — writes fail loudly, keys are not evicted"
  else
    no "maxmemory-policy is '$pol', not noeviction — consumers' error model is broken"
  fi
fi

# ─── 6. contract ports match reality ─────────────────────────────────────────
# The Ports table in docs/stack-contract.md is what four other repos resolve
# addresses from, and until now nothing stopped it drifting from the stack it
# describes. This check reads the table and probes it, in both directions:
#
#   live                          must answer, or the suite FAILS
#   live (lite) / internal        skipped, with the reason printed
#   external                      another project's listener. This stack only
#                                 reserves the number; probing it would be
#                                 testing someone else's service, so we don't
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
    # `external` is deliberately absent here: not ours to probe.
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
        external)
          detail="skip: $net is its owner's listener, not this stack's"
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

# ─── 7. everything registered is actually provisioned ────────────────────────
# registry/dependencies.tsv is where consumer projects declare what they need
# from this stack. A row that was added but never provisioned is the same class
# of defect as a port row that drifted from reality — an interface that exists
# on paper and not in the stack. So read the registry and check it, rather than
# trusting that bootstrap ran.
#
# Only `topic` and `bucket` rows are provisioned here. `redis-key` rows are
# created by their owner on first write and `port` rows are reserved for a
# listener this stack does not run, so both are reported, not asserted.
head_ "7. Registered dependencies are provisioned"

REGISTRY=registry/dependencies.tsv

if [ ! -r "$REGISTRY" ]; then
  no "registry $REGISTRY is missing or unreadable — nothing can be provisioned from it"
else
  live_topics=$(kexec /opt/kafka/bin/kafka-topics.sh --bootstrap-server "$BOOTSTRAP" --list 2>/dev/null | tr -d '\r')
  live_buckets=$(wget_ -q -O- "${GCS}/storage/v1/b?project=pulse-local" 2>/dev/null)

  missing=""
  printf '  %-11s %-21s %-30s %s\n' KIND NAME OWNER RESULT
  while read -r kind name owner spec; do
    case "$kind" in ''|\#*) continue ;; esac
    [ -n "$name" ] || continue
    case "$kind" in
      topic)
        if printf '%s\n' "$live_topics" | grep -qx "$name"; then
          detail="provisioned"
        else
          detail="MISSING from Kafka"
          missing="$missing $name"
        fi
        ;;
      bucket)
        if printf '%s' "$live_buckets" | grep -q "\"$name\""; then
          detail="provisioned"
        else
          detail="MISSING from object storage"
          missing="$missing $name"
        fi
        ;;
      redis-key)
        detail="declared ($spec) — created by its owner on first write"
        ;;
      port)
        detail="reserved ($spec) — listener is the owner's, not this stack's"
        ;;
      *)
        detail="unknown kind — not provisioned"
        missing="$missing $name"
        ;;
    esac
    printf '  %-11s %-21s %-30s %s\n' "$kind" "$name" "$owner" "$detail"
  done < "$REGISTRY"

  if [ -n "$missing" ]; then
    no "registered but not provisioned:$missing — re-run \`make up\`, or bootstrap failed"
  else
    ok "every provisioned-kind registry row exists in the stack"
  fi
fi

head_ "Summary"
printf '  %d passed, %d failed\n\n' "$pass" "$fail"
[ "$fail" -eq 0 ]
