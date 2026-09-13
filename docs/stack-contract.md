# Stack contract

The stable interface the consumer repos code against. **Anything on this page is
a breaking change for four-plus repos** (`pulse-gateway`, `pulse-ingestor`,
`pulse-conflux`, `pulse-client`, and the out-of-scope silver worker). Change a
topic name, a port, a bucket path or a Redis key here and something else stops
working — coordinate the change across those repos in the same breath.

Everything below was verified against a running stack, not copied from intent.

## Service inventory

| Service | Image | Profiles | Container name | Purpose |
|---|---|---|---|---|
| `kafka-1` | `apache/kafka:4.3.1` | core, full | `pulse-kafka-1` | Broker + KRaft controller, node 1 |
| `kafka-2` | `apache/kafka:4.3.1` | core, full | `pulse-kafka-2` | Broker + KRaft controller, node 2 |
| `kafka-3` | `apache/kafka:4.3.1` | core, full | `pulse-kafka-3` | Broker + KRaft controller, node 3 |
| `kafka-lite` | `apache/kafka:4.3.1` | lite | `pulse-kafka-lite` | Single broker, RF=1 |
| `redis` | `redis:7.4.11-alpine` | full | `pulse-redis` | Gateway queue backend |
| `fake-gcs` | `fsouza/fake-gcs-server:1.56.1` | core, full, lite | `pulse-fake-gcs` | GCS staging stand-in |
| `gateway` | built from `../pulse-gateway` | full | `pulse-gateway` | HTTP ingestion gateway |
| `bootstrap` | `apache/kafka:4.3.1` | core, full | `pulse-bootstrap` | One-shot: topics + bucket, then exits 0 |
| `bootstrap-lite` | `apache/kafka:4.3.1` | lite | `pulse-bootstrap-lite` | Same, RF=1 |

Digests are in [`../images.lock`](../images.lock). Docker network: `pulse-infra`.

## Ports

Container-internal names are what in-network consumers use; host ports are for
processes running on the laptop outside Docker.

| Service | In-network address | Host address | Notes |
|---|---|---|---|
| Kafka broker 1 | `kafka-1:9092` | `localhost:19092` | `INTERNAL` / `EXTERNAL` listeners |
| Kafka broker 2 | `kafka-2:9092` | `localhost:19093` | |
| Kafka broker 3 | `kafka-3:9092` | `localhost:19094` | |
| Kafka (lite) | `kafka-lite:9092` | `localhost:19092` | Reuses broker 1's host port |
| Kafka controllers | `kafka-N:9093` | not published | KRaft quorum, internal only |
| Redis | `redis:6379` | `localhost:6379` | |
| fake-gcs-server | `fake-gcs:4443` | `localhost:4443` | **HTTP**, not HTTPS |
| Gateway | `gateway:8080` | `localhost:8080` | |

**Bootstrap servers**, in-network: `kafka-1:9092,kafka-2:9092,kafka-3:9092`.
From the host: `localhost:19092,localhost:19093,localhost:19094`.

`lite` and `core`/`full` both bind host port 19092 and **must not run at the
same time**. `make reset` between profile switches.

## Kafka topics

| Topic | Partitions | RF (core/full) | RF (lite) | `min.insync.replicas` | Written by | Read by |
|---|---|---|---|---|---|---|
| `ingestion-events` | 6 | 3 | 1 | 2 (core/full), 1 (lite) | gateway (future) | `pulse-ingestor` |
| `ingestion-signals` | 6 | 3 | 1 | 2 (core/full), 1 (lite) | gateway (future) | `pulse-ingestor` |
| `ingestion-logs` | 6 | 3 | 1 | 2 (core/full), 1 (lite) | gateway (future) | `pulse-ingestor` |

- Names deliberately **mirror the Redis key names** — one vocabulary across both
  transports.
- **6 partitions** divides evenly by 1, 2, 3 and 6 consumers, so consumer-group
  rebalancing is observable at every group size worth trying on a laptop.
- **RF=3 with `min.insync.replicas=2`** is what makes a single broker loss
  survivable *and* the survival meaningful. RF=3 with min ISR=3 would make the
  cluster unwritable on one loss; RF=1 makes the test vacuous.
- **Auto-topic-creation is OFF** (`KAFKA_AUTO_CREATE_TOPICS_ENABLE=false`).
  Topics are contract, not a side effect of a typo'd producer. A consumer
  subscribing to a misspelled topic gets an error, not a silent empty topic.
- **Consumer groups are the consumer's business.** Nothing here creates one.
- "Written by gateway (future)" is literal: the gateway has **no Kafka producer
  at this commit**. See [`divergences.md`](divergences.md).

## GCS staging

| Property | Value |
|---|---|
| Bucket | `pulse-staging-local` |
| Endpoint (in-network) | `http://fake-gcs:4443` |
| Endpoint (host) | `http://localhost:4443` |
| Client env var | `STORAGE_EMULATOR_HOST=fake-gcs:4443` (or `localhost:4443`) |
| Credentials | **None.** Unauthenticated. |
| Object path | `{topic}/dt=YYYY-MM-DD/{partition}-{startoffset}-{endoffset}.parquet` |
| Example | `ingestion-events/dt=2026-09-12/0-0-99.parquet` |

The word is **staging**, not "bronze", everywhere — a transient landing pad with
no retention promise. If it ever becomes a replay source you intend to keep, that
is a different word with a different cost, and the rename touches every repo here.

**On idempotency.** Deterministic naming means a retried batch overwrites the
same object instead of creating a duplicate — that is the point of putting the
offsets in the name, and it gives `pulse-ingestor` idempotency without a
transaction. But the guarantee holds **only when a retry reproduces the same
batch boundaries**. After a consumer-group rebalance, the retrying consumer may
assemble a batch with different start/end offsets, writing a differently-named
object that overlaps the same events. That is duplication, not an overwrite.
Do not treat the path convention as a stronger guarantee than it is.

Buckets are created by bootstrap; object "directories" are not, because GCS has
no directories. The consumer writes the full object name.

## Redis

The gateway's contract, as implemented in its `internal/queue/redis/producer.go`.

| Key | Structure | Written by | Read by | Cap (local) |
|---|---|---|---|---|
| `ingestion-events` | **Stream** (`XADD`) | gateway | `pulse-conflux` | `MAXLEN ~10000` |
| `ingestion-signals` | **Stream** (`XADD`) | gateway | `pulse-conflux` | `MAXLEN ~10000` |
| `ingestion-logs` | **List** (`RPUSH` via Lua + `EXPIRE`) | gateway | `pulse-conflux` | 10000 entries, TTL 120s |

- Auth: `requirepass pulse-local-not-a-secret`, database `0`.
- `maxmemory 512mb`, **`maxmemory-policy noeviction`**. The policy is
  load-bearing: the gateway's error model depends on writes failing loudly
  rather than keys being evicted from under a consumer. Do not change it.
- **Stream entries have exactly one field, `data`,** whose value is the JSON
  envelope. Not one Redis field per envelope key.
- **Both destinations are lossy under lag, in different ways.** Streams trim
  (approximate trimming, `REDIS_STREAM_APPROX_MAX_LEN=true`); the log list's Lua
  script **drops the write outright** once the cap is hit and returns
  `LOGS_LIST_FULL`. Neither applies backpressure to the gateway, so **consumer
  lag is the metric that matters** — falling behind means data loss upstream.
- Caps are **10k locally, vs 100k in the gateway's own example config**. That is
  deliberate: low caps make the lossy path visible in development rather than in
  the dev environment.
- Envelope (`model.EnrichedPayload`): `event_id`, `gateway_id`, `received_at`
  (UTC RFC3339), `retry_count`, `stream_name`, `event_header` (**JWT requests
  only**), `payload`. `destination_type`, `ttl` and error fields are `json:"-"`
  and never appear on the wire.

## Gateway HTTP API

| Route | Method | Auth | Success |
|---|---|---|---|
| `/telemetry/events/v1` | POST | API key or JWT | `202 {"status":"accepted"}` |
| `/telemetry/logs/v1` | POST | API key or JWT | `202` |
| `/telemetry/signals/v1` | POST | API key or JWT | `202` |
| `/livez` | GET | none | `200` |
| `/readyz` | GET | none | `200` + buffer/Redis detail |
| `/metrics` | GET | API key or JWT | Prometheus text |

**HTTP only — there is no gRPC endpoint.** `pulse-client` is specified to drive
both; only HTTP exists to drive.

### Auth, exactly as implemented

Verified by observation, because the details are easy to get wrong:

- **API-key requests need TWO headers**: `x-api-key` *and* `x-client-id`. The
  client id keys the store; sending only the key is a flat `401` with an empty
  `client_id` in the gateway log. (This is not in the gateway's own docs.)
- **JWT requests** send `Authorization: Bearer <token>`, HMAC `HS256`/`HS384`/
  `HS512`, validated against the `jwtSecrets` list in order.
- **Sending both** an API key and an `Authorization` header is a `401` —
  ambiguous identity.
- **`allowed_routes` is an exact string match** on the full path, `/telemetry`
  prefix included, no wildcards, no trailing slash. It is enforced for **API
  keys only** — a valid JWT reaches every authenticated route regardless. A key
  that scrapes `/metrics` must list `/metrics` explicitly.
- **`event_header` appears on the Redis envelope for JWT requests and is absent
  for API-key requests.** Verified both ways in `bootstrap/verify.sh` check 5.
  Consumers must treat it as optional.
- Validation failures are `400` (not `422`), with all field errors joined in one
  message.

### Credential fixtures

| Fixture | Location | Contents |
|---|---|---|
| Gateway auth keys | `bootstrap/fixtures/api_keys.local.json` | 2 fake clients, 2 fake JWT secrets |
| Gateway JWT | generated by `bootstrap/mint-dev-jwt.sh` | HS256, 1h default |
| GCS credentials | **none needed** | fake-gcs is unauthenticated |

Client ids and keys: `pulse_client_local` / `local-not-a-secret-client-key`
(three telemetry routes), and `pulse_infra_smoke` /
`local-not-a-secret-smoke-key` (telemetry routes plus `/metrics`).

Every value is obviously fake and named to stay that way. JWTs are **generated,
never checked in**. The gateway will not start at all without this file.

## Health endpoints and readiness

| Service | Check | What "ready" means |
|---|---|---|
| Kafka | `kafka-topics.sh --list` (container healthcheck) | Broker answers metadata |
| Kafka cluster | `bootstrap/bootstrap.sh` gates 1 and 2 | All expected brokers in metadata **and** a topic can actually be created |
| Redis | `redis-cli ping` | `PONG` |
| fake-gcs | `GET /storage/v1/b?project=pulse-local` | HTTP 200 |
| Gateway | `GET /livez` (container), `GET /readyz` (real) | `/readyz` reports Redis latency + per-buffer utilization |

A started container is not a ready service. The cluster-level gate is stricter
than the per-container healthcheck on purpose: a cluster can answer metadata and
still refuse topic creation when it has too few in-sync replicas.

`make up` blocks until the bootstrap container **exits 0**, so when it returns,
topics and the bucket exist. (`docker compose up --wait` alone does not
guarantee this — it treats a one-shot container as satisfied once it is merely
running.)
