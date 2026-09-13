#!/usr/bin/env bash
# pulse-infra bootstrap — cluster readiness gate, topic creation, bucket creation.
#
# Idempotent by design: re-running against a live stack is a no-op. `make reset`
# wipes volumes and re-runs this, so "known-clean" means topics and bucket present,
# data absent.
#
# Runs inside the apache/kafka image (Alpine: bash + BusyBox wget, no curl).
# Every value comes from the environment — see compose/compose.yaml.
set -euo pipefail

KAFKA_BOOTSTRAP="${KAFKA_BOOTSTRAP:?}"
EXPECTED_BROKERS="${EXPECTED_BROKERS:?}"
TOPIC_PARTITIONS="${TOPIC_PARTITIONS:?}"
TOPIC_REPLICATION_FACTOR="${TOPIC_REPLICATION_FACTOR:?}"
TOPIC_MIN_INSYNC_REPLICAS="${TOPIC_MIN_INSYNC_REPLICAS:?}"
GCS_ENDPOINT="${GCS_ENDPOINT:?}"
GCS_BUCKET="${GCS_BUCKET:?}"

# Topic names deliberately mirror the gateway's Redis key names, so one
# vocabulary covers both transports. Changing these breaks four-plus repos —
# see docs/stack-contract.md.
TOPICS="ingestion-events ingestion-signals ingestion-logs"

KT=/opt/kafka/bin/kafka-topics.sh
KB=/opt/kafka/bin/kafka-broker-api-versions.sh

log() { printf '[bootstrap] %s\n' "$*"; }
fail() { printf '[bootstrap] FAILED: %s\n' "$*" >&2; exit 1; }

# ─── Gate 1: every expected broker answers metadata ──────────────────────────
# A container that has started is not a ready broker. The brief's bar is
# "metadata returns all expected brokers", which is stricter than a TCP check
# and is what catches a broker that booted but never joined the quorum.
log "waiting for ${EXPECTED_BROKERS} broker(s) via ${KAFKA_BOOTSTRAP}"
deadline=$(( $(date +%s) + 180 ))
while :; do
  count=$("$KB" --bootstrap-server "$KAFKA_BOOTSTRAP" 2>/dev/null | grep -c '(id: ' || true)
  if [ "${count:-0}" -ge "$EXPECTED_BROKERS" ]; then
    log "cluster reports ${count} broker(s) — ready"
    break
  fi
  [ "$(date +%s)" -lt "$deadline" ] || fail "only ${count:-0}/${EXPECTED_BROKERS} brokers after 180s"
  sleep 3
done

# ─── Gate 2: a topic can actually be created ─────────────────────────────────
# Metadata answering is necessary but not sufficient — a cluster with too few
# in-sync replicas answers metadata and still refuses topic creation.
probe="__pulse_infra_readiness_probe"
"$KT" --bootstrap-server "$KAFKA_BOOTSTRAP" --create --if-not-exists \
      --topic "$probe" --partitions 1 \
      --replication-factor "$TOPIC_REPLICATION_FACTOR" >/dev/null \
  || fail "cluster answers metadata but refuses topic creation"
"$KT" --bootstrap-server "$KAFKA_BOOTSTRAP" --delete --topic "$probe" >/dev/null 2>&1 || true
log "topic creation verified"

# ─── Topics ──────────────────────────────────────────────────────────────────
for t in $TOPICS; do
  if "$KT" --bootstrap-server "$KAFKA_BOOTSTRAP" --list | grep -qx "$t"; then
    log "topic ${t} already exists — leaving as is"
  else
    "$KT" --bootstrap-server "$KAFKA_BOOTSTRAP" --create \
          --topic "$t" \
          --partitions "$TOPIC_PARTITIONS" \
          --replication-factor "$TOPIC_REPLICATION_FACTOR" \
          --config "min.insync.replicas=${TOPIC_MIN_INSYNC_REPLICAS}" >/dev/null
    log "created topic ${t} (partitions=${TOPIC_PARTITIONS} rf=${TOPIC_REPLICATION_FACTOR} min.isr=${TOPIC_MIN_INSYNC_REPLICAS})"
  fi
done

# ─── Staging bucket ──────────────────────────────────────────────────────────
# "staging", not "bronze": a transient landing pad with no retention promise.
if wget -q -O- "${GCS_ENDPOINT}/storage/v1/b/${GCS_BUCKET}" >/dev/null 2>&1; then
  log "bucket ${GCS_BUCKET} already exists — leaving as is"
else
  wget -q -O- \
    --header 'Content-Type: application/json' \
    --post-data "{\"name\":\"${GCS_BUCKET}\"}" \
    "${GCS_ENDPOINT}/storage/v1/b?project=pulse-local" >/dev/null \
    || fail "could not create bucket ${GCS_BUCKET} at ${GCS_ENDPOINT}"
  log "created bucket ${GCS_BUCKET}"
fi

# Object paths are a convention, not pre-created directories. GCS has no real
# directories; the ingestor writes the full object name:
#   {topic}/dt=YYYY-MM-DD/{partition}-{startoffset}-{endoffset}.parquet
log "object path convention: {topic}/dt=YYYY-MM-DD/{partition}-{startoffset}-{endoffset}.parquet"

# ─── Report final state ──────────────────────────────────────────────────────
log "topics now present:"
"$KT" --bootstrap-server "$KAFKA_BOOTSTRAP" --list | sed 's/^/[bootstrap]   /'
log "buckets now present:"
wget -q -O- "${GCS_ENDPOINT}/storage/v1/b?project=pulse-local" | sed 's/^/[bootstrap]   /'
echo
log "bootstrap complete"
