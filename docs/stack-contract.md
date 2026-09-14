# Stack contract

The stable interface the consumer repos code against. **Anything on this page is
a breaking change for four-plus repos** (`pulse-gateway`, `pulse-ingestor`,
`pulse-conflux`, `pulse-client`, and the out-of-scope silver worker). Change a
topic name, a port, a bucket path or a Redis key here and something else stops
working — coordinate the change across those repos in the same breath.

**This stack provides backing services and nothing else.** It does not build,
start, configure or test any consumer project. Projects declare what they need
in [`../registry/dependencies.tsv`](../registry/dependencies.tsv) and run their
own containers against the addresses below. If you are looking for how to run
the gateway or the ingestor, it is in their repo, not this one.

Everything marked `live` below was verified against a running stack, not copied
from intent. Rows marked `contracted-not-yet-listening` are agreed interfaces
that nothing implements yet, and say so — see "Changing this contract".

## Registering a dependency

**The direction is one-way.** This stack does not know what any consumer does,
and must never need to: it provides Kafka, Redis and object storage, and stops
there. A project that needs something from it **registers** that need in
[`../registry/dependencies.tsv`](../registry/dependencies.tsv) and connects from
its own container.

| You need | Register kind | What happens |
|---|---|---|
| A Kafka topic | `topic` | Created by bootstrap at `make up`, idempotently |
| A bucket | `bucket` | Same |
| A Redis key | `redis-key` | Nothing is created — Redis keys appear on first write. The row reserves the name and records the structure |
| A host or in-network port | `port` | Reserved so the number cannot be handed out twice. **This stack does not run your listener** |

How to add one: open a PR against this repo adding the row, in the same change
as the code that uses it (see "Changing this contract" below). Adding a row is
the *only* way to get something provisioned — nothing in this stack is
configured per-consumer, and no consumer's config, credentials or build lives
here.

What registration does **not** buy you: a container. Your service runs in your
repo's compose file, joined to the `pulse-infra` network, using the in-network
addresses in the Ports table. See "Containerizing a consumer".

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
| `live` | Provided by this stack and answering on the `full` profile | Probed. An unreachable `live` entry **fails** the suite |
| `live (lite)` | Live only on the `lite` profile | Skipped — the suite runs `full` |
| `internal` | In-network only, no host port | Skipped, with the reason printed |
| `external` | **Reserved for another project's listener.** This stack allocates the number so it cannot be handed out twice; it does not run the process | Never probed — it is not ours to serve |
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
| Gateway HTTP | `gateway:8080` | `localhost:8080` | `external` | Run by `pulse-gateway` from its own compose file, not by this stack |
| Gateway gRPC | `gateway:9090` | `localhost:9090` | `external` | Reserved for `pulse-gateway` `ADR-009`. **Nothing listens yet** — see below |

**Bootstrap servers**, in-network: `kafka-1:9092,kafka-2:9092,kafka-3:9092`.
From the host: `localhost:19092,localhost:19093,localhost:19094`.

`lite` and `core`/`full` both bind host port 19092 and **must not run at the
same time**. `make reset` between profile switches.

### Containerizing a consumer

Every consumer repo's `.env.example` currently ships **host** addresses
(`localhost:19092`, `localhost:6379`, `localhost:4443`) because they are written
for a process running on the laptop. The moment that process moves into a
container on the `pulse-infra` network, every one of those is wrong — and wrong
in the worst way, because `localhost` inside a container resolves to the
container itself, so the failure is a connection refused, not a name-resolution
error that would point at the cause.

Swap to the in-network column, whole:

| Consumer | Host form (`.env.example` today) | In-network form (containerized) |
|---|---|---|
| `pulse-ingestor` | `KAFKA_BOOTSTRAP_SERVERS=localhost:19092,…3,…4` | `kafka-1:9092,kafka-2:9092,kafka-3:9092` |
| `pulse-ingestor` | `STORAGE_EMULATOR_HOST=localhost:4443` | `fake-gcs:4443` |
| `pulse-conflux` | `REDIS_ADDR=localhost:6379` | `redis:6379` |

Topic names, bucket paths, Redis keys and credentials do **not** change — only
the addresses. That is the whole point of the two columns.

### Where the service itself goes

**In your repo, not this one.** Define it in your own compose file and join it
to this stack's network, which is external to you and already running:

```yaml
# in pulse-<yours>/compose.yaml
services:
  my-service:
    build: .
    env_file: [./local.env]      # in-network addresses, per the table above
    networks: [pulse-infra]
networks:
  pulse-infra:
    external: true               # created by `make up` in pulse-infra
```

Bring this stack up first (`make up` here), then your service. `make up` does
not return until every registered topic and bucket exists, so there is no race
to guard against — and nothing here waits on you, which is what lets the two
lifecycles stay independent.

Two rules:

- **Publish a host port only if something outside Docker needs to reach it.** A
  consumer that only talks to Kafka and GCS needs none. If it does take one,
  register it (`kind: port`) so the number cannot be handed out twice — that
  reservation is how 9090 was known to be free.
- **Never add your service to this repo's `compose.yaml`.** That is the coupling
  this layout exists to prevent: it would make this stack build your code, hold
  your config, and fail when your build breaks.

### Gateway 8080 and 9090 — reserved, not served

Both gateway rows are `external`: the numbers are allocated here so nothing else
claims them, and `pulse-gateway` runs the listeners in its own repo. This stack
does not start, build or probe either one.

9090 is the reservation that earned the mechanism. `pulse-gateway`'s `ADR-009`
ratified a gRPC transport on its own port — Fiber is `fasthttp` and cannot serve
`grpc-go` on one listener, and `cmux` and Connect were both considered and
rejected — and 9090 was chosen precisely because it was free against this page.
That is what a `port` registration is for: the check happened before the number
was committed to, not after two projects collided.

**Nothing listens on 9090 yet.** Every `GRPC-*` requirement is `MISS` at the
gateway's current commit: the transport is agreed, the server is unwritten. The
address is safe to resolve and will not move; a connection to it will fail.
`pulse-client` can write its endpoint resolution against this row today and
cannot exercise it until the gateway ships the listener.

## Kafka topics

Provisioned from [`../registry/dependencies.tsv`](../registry/dependencies.tsv)
at `make up`. That file is the source; this table is its documented form, and
`make verify` check 7 asserts the two agree with the running cluster.

| Topic | Partitions | RF (core/full) | RF (lite) | `min.insync.replicas` | Registered by | Read by |
|---|---|---|---|---|---|---|
| `ingestion-events` | 6 | 3 | 1 | 2 (core/full), 1 (lite) | `pulse-ingestor`, `pulse-gateway` | `pulse-ingestor` |
| `ingestion-signals` | 6 | 3 | 1 | 2 (core/full), 1 (lite) | `pulse-ingestor`, `pulse-gateway` | `pulse-ingestor` |
| `ingestion-logs` | 6 | 3 | 1 | 2 (core/full), 1 (lite) | `pulse-ingestor`, `pulse-gateway` | `pulse-ingestor` |

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
- **Nothing in this stack produces to these topics.** They are provisioned and
  left empty; a registered writer fills them from its own repo. See
  [`divergences.md`](divergences.md).

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

This stack provides Redis. It does not own what anyone writes into it — the key
names, structures and owners below are **registered** in
[`../registry/dependencies.tsv`](../registry/dependencies.tsv), and the meaning
of the bytes belongs to the writer.

| Key | Structure | Registered by | Read by |
|---|---|---|---|
| `ingestion-events` | **Stream** (`XADD`) | `pulse-gateway` | `pulse-conflux` |
| `ingestion-signals` | **Stream** (`XADD`) | `pulse-gateway` | `pulse-conflux` |
| `ingestion-logs` | **List** (`RPUSH`) | `pulse-gateway` | `pulse-conflux` |
| `ingestion-dlq` | **List** (`RPUSH`) | `pulse-gateway` | **nobody — operators, for replay** |

Redis keys are not provisioned: they spring into being on first write. The rows
exist so two projects cannot pick the same name, and so a consumer knows which
structure to expect before it connects.

### What this stack guarantees

- **Auth**: `requirepass pulse-local-not-a-secret`, database `0`. An
  unauthenticated client is refused — `make verify` check 5 asserts it.
- **`maxmemory 512mb`, `maxmemory-policy noeviction`.** The policy is
  load-bearing and it is a promise to the owners of those keys: a write fails
  **loudly** rather than a key being evicted from under a consumer. Every
  registered writer's error model is built on that. Do not change it.
- Both structures round-trip as registered — check 5 exercises `XADD`/`XRANGE`
  and `RPUSH`/`LRANGE` directly, against Redis, driving no consumer.

### What this stack does not define

Caps, trimming, retention, envelope shape, `error_reason` values and what any
status code means are **the writer's contract with its readers**, not ours. They
are configured in the owning repo and documented there.

For the `ingestion-*` keys that is `pulse-gateway`: see its
`internal/queue/redis/` and its own docs for the `EnrichedPayload` envelope,
the stream/list cap and retention behaviour, and the dead-letter reasons.
`pulse-conflux` should read that, not this page, before depending on a field.

Two properties are worth knowing anyway, because they shape every consumer here:
**neither destination applies backpressure**, so consumer lag is the metric that
matters; and **`ingestion-dlq` is not telemetry** — replaying it is a deliberate
operator action, never a pipeline source.

## Health endpoints and readiness

| Service | Check | What "ready" means |
|---|---|---|
| Kafka | `kafka-topics.sh --list` (container healthcheck) | Broker answers metadata |
| Kafka cluster | `bootstrap/bootstrap.sh` gates 1 and 2 | All expected brokers in metadata **and** a topic can actually be created |
| Redis | `redis-cli ping` | `PONG` |
| fake-gcs | `GET /storage/v1/b?project=pulse-local` | HTTP 200 |

A started container is not a ready service. The cluster-level gate is stricter
than the per-container healthcheck on purpose: a cluster can answer metadata and
still refuse topic creation when it has too few in-sync replicas.

`make up` blocks until the bootstrap container **exits 0**, so when it returns,
every registered topic and bucket exists. (`docker compose up --wait` alone does
not guarantee this — it treats a one-shot container as satisfied once it is
merely running.) Nothing in this stack waits on a consumer, and no consumer
should have to wait on anything here beyond that exit.

Consumer readiness is the consumer's own to report. This table covers the
backing services only.
