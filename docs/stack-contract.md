# Stack contract

The stable interface the consumer repos code against. **Anything on this page is
a breaking change for four-plus repos** (`pulse-gateway`, `pulse-ingestor`,
`pulse-conflux`, `pulse-client`, and the out-of-scope silver worker). Change a
topic name, a port, a bucket path or a Redis key here and something else stops
working — coordinate the change across those repos in the same breath.

Everything marked `live` below was verified against a running stack, not copied
from intent. Rows marked `contracted-not-yet-listening` are agreed interfaces
that nothing implements yet, and say so — see "Changing this contract".

## Changing this contract

**Every new port, topic, Redis key or bucket path is a change to this page, and
the entry lands in the same change as the code that creates it** — not in a
follow-up, not "once it works". A listener that exists without a row here is
invisible to the repos that have to reach it, and the reverse costs more than it
looks: `pulse-client` is specified not to restate an address this repo owns, so a
missing row does not degrade it, it blocks it.

That rule needs somewhere to put an interface that has been agreed but not yet
built, or it quietly encourages the opposite mistake — leaving a ratified address
off the page because nothing answers on it yet. Hence a **`Status` column on the
Ports table**:

| Status | Meaning | In `make verify` |
|---|---|---|
| `live` | Published and answering on the `full` profile | Probed. An unreachable `live` entry **fails** the suite |
| `live (lite)` | Live only on the `lite` profile | Skipped — the suite runs `full` |
| `internal` | In-network only, no host port | Skipped, with the reason printed |
| `contracted-not-yet-listening` | Ratified by the owning repo; nothing listening yet | Skipped, with the reason printed |

`contracted-not-yet-listening` is a promise about the *address*, not about
reachability: code against it and it will not move under you, but do not expect a
connection. When the listener lands, publishing the port and flipping the row to
`live` are the same change.

Check 6 of `make verify` reads this table and probes it, so the page cannot drift
from the running stack in either direction: a `live` row that stops answering
fails the suite, and a `contracted-not-yet-listening` row that *starts* answering
is reported as drift to be resolved by flipping it to `live`.

The other tables carry the same idea in prose rather than a column — the Kafka
topics table says "written by gateway (future)" for exactly this reason. Only the
Ports table is machine-checked, because only it is probeable.

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

| Service | In-network address | Host address | Status | Notes |
|---|---|---|---|---|
| Kafka broker 1 | `kafka-1:9092` | `localhost:19092` | `live` | `INTERNAL` / `EXTERNAL` listeners |
| Kafka broker 2 | `kafka-2:9092` | `localhost:19093` | `live` | |
| Kafka broker 3 | `kafka-3:9092` | `localhost:19094` | `live` | |
| Kafka (lite) | `kafka-lite:9092` | `localhost:19092` | `live (lite)` | Reuses broker 1's host port |
| Kafka controllers | `kafka-N:9093` | not published | `internal` | KRaft quorum, internal only |
| Redis | `redis:6379` | `localhost:6379` | `live` | |
| fake-gcs-server | `fake-gcs:4443` | `localhost:4443` | `live` | **HTTP**, not HTTPS |
| Gateway HTTP | `gateway:8080` | `localhost:8080` | `live` | Fiber |
| Gateway gRPC | `gateway:9090` | `localhost:9090` | `contracted-not-yet-listening` | Owned by `pulse-gateway` `ADR-009`. **Nothing listens yet**, and the host port is deliberately not published — see below |

**Bootstrap servers**, in-network: `kafka-1:9092,kafka-2:9092,kafka-3:9092`.
From the host: `localhost:19092,localhost:19093,localhost:19094`.

`lite` and `core`/`full` both bind host port 19092 and **must not run at the
same time**. `make reset` between profile switches.

### Gateway gRPC, 9090 — contracted, not published

`pulse-gateway` has ratified a gRPC transport on its own port: `ADR-009` in its
`specs/001-telemetry-ingestion/plan.md` §5, with `GRPC-001`…`GRPC-011` in
`spec.md` pinning the semantics. Fiber is `fasthttp` and cannot serve `grpc-go`
on one listener; `cmux` and Connect were both considered and rejected. 9090 was
picked because it is free against everything else on this page. That ADR records
this row as the cross-repo dependency it creates.

**The row exists so the address is resolvable. Nothing listens on it.** Every
`GRPC-*` requirement is `MISS` at the gateway's current commit — the contract is
agreed, the server is unwritten.

The host port is **deliberately not published in `compose/compose.yaml`**, and
the trade is worth stating because it is close. Publishing `9090:9090` now would
cost nothing to start and save one line of diff later, but `make ps` and `make
health` would then advertise `0.0.0.0:9090->9090/tcp` for an endpoint that
refuses every connection — health output that lies, which is the one thing this
stack is for. Not publishing costs a second edit when the listener lands, in a
change that has to touch this file anyway to flip the row to `live`. A missing
answer is cheaper to live with than a wrong one.

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
| `ingestion-events` | **Stream** (`XADD`) | gateway | `pulse-conflux` | `MAXLEN ~10000` **and** 30-min age trim |
| `ingestion-signals` | **Stream** (`XADD`) | gateway | `pulse-conflux` | `MAXLEN ~10000` **and** 30-min age trim |
| `ingestion-logs` | **List** (`RPUSH` via Lua + `EXPIRE`) | gateway | `pulse-conflux` | 10000 entries; key TTL 120s — *idle purge only* |
| `ingestion-dlq` | **List** (`RPUSH` + `LTRIM` + `EXPIRE`) | gateway | **nobody — operators, for replay** | 10000 entries, TTL 7d |

- Auth: `requirepass pulse-local-not-a-secret`, database `0`.
- `maxmemory 512mb`, **`maxmemory-policy noeviction`**. The policy is
  load-bearing: the gateway's error model depends on writes failing loudly
  rather than keys being evicted from under a consumer. Do not change it.
- **Stream entries have exactly one field, `data`,** whose value is the JSON
  envelope. Not one Redis field per envelope key.
- **Both destinations are lossy under lag, in different ways.** Streams trim
  (approximate trimming, `REDIS_STREAM_APPROX_MAX_LEN=true`); the log list's Lua
  script refuses the write once the cap is hit and returns `LOGS_LIST_FULL`.
  Neither applies backpressure to the gateway, so **consumer lag is the metric
  that matters** — falling behind means data loss upstream.
- **Changed 2026-09-14 (gateway `P2-C` C-1):** a `LOGS_LIST_FULL` payload is no
  longer dropped outright. It is dead-lettered to `ingestion-dlq`, so it is
  recoverable. The stream trim path is still a silent loss.
- **`ingestion-dlq` is not telemetry.** It holds `EnrichedPayload` envelopes the
  gateway could not deliver, each stamped with an `error_reason`. `pulse-conflux`
  must **not** consume it as a normal source: replaying it is a deliberate
  operator action, not part of the pipeline. Reasons are `buffer_overflow`,
  `max_retries_exceeded`, `shutdown_during_retry`, `logs_list_full`,
  `dlq_sink_failed`.
- **Stream age trim vs the log list TTL are not the same mechanism.**
  `REDIS_STREAM_RETENTION_SECONDS` age-trims stream *entries* (`XTRIM MINID ~`),
  so a consumer sees a moving 30-minute window regardless of volume.
  `REDIS_LIST_LOGS_TTL_SECONDS` expires the log list *key* and is refreshed on
  every push, so it only purges an **idle** list — under continuous writes it
  never fires. Redis lists have no age-based trim primitive (gateway `DEL-012`).
- **The gateway now answers `503` when buffers saturate** rather than `202`
  (gateway `BUF-005`), with `Retry-After: 1`. A `202` therefore now means
  buffered *and* recoverable. Clients that treated `202` as "definitely stored"
  were previously wrong; they are now right.
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

**HTTP is the only transport listening — but the gRPC surface is specified, not
undefined.** `pulse-gateway` owns and has ratified it: three unary RPCs
(`PublishEvent`, `PublishLog`, `PublishSignal`) mapping 1:1 onto the three
telemetry routes, on `gateway:9090`, with envelope parity, status-code mapping
and a shared route allowlist all pinned by `GRPC-001`…`GRPC-011`. The address is
in the Ports table above and will not move. What does not exist is the server:
every one of those requirements is `MISS` at the gateway's current commit.

So `pulse-client` — specified to drive both transports — can resolve the gRPC
endpoint from this page today, and can drive only HTTP until the listener lands.
See [`divergences.md`](divergences.md).

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
