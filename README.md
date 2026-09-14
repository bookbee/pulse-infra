# pulse-infra

Local development infrastructure for the Pulse platform: Kafka, Redis, a GCS
stand-in, and `pulse-gateway`, brought up with one command so every other Pulse
repo can be developed, run and integrated **entirely on a laptop before anything
is committed to Git**.

> ## Scope: local development only
>
> **Not staging. Not production.** No IAM, no TLS, no auth on Kafka or GCS, fake
> credentials checked in on purpose, and data that a single `make reset` throws
> away. Nothing here is a deployment artifact and nothing here should grow into
> one — infra repos drift toward production scope by default.
>
> A green run here is **not** evidence that anything works against GCP. Read
> [`docs/divergences.md`](docs/divergences.md) before you trust a local pass.

## Quickstart

Requires Docker (Compose v2), `make`, and — for the `full` profile only — the
sibling repo `../pulse-gateway` checked out.

```bash
make up          # cold start from nothing, full profile
make verify      # prove the contract: 17 checks, real output
make down        # stop, keep data
make reset       # wipe this stack's volumes, back to known-clean
```

`make up` returns when the stack is healthy **and** topics and the bucket exist.

What you get on `full`:

| | Address |
|---|---|
| Kafka (from host) | `localhost:19092,localhost:19093,localhost:19094` |
| Kafka (in-network) | `kafka-1:9092,kafka-2:9092,kafka-3:9092` |
| Redis | `localhost:6379`, password `pulse-local-not-a-secret` |
| GCS staging | `http://localhost:4443`, bucket `pulse-staging-local` |
| Gateway | `http://localhost:8080` |

Topics `ingestion-events`, `ingestion-signals`, `ingestion-logs` — 6 partitions,
RF=3. Full interface details, including the auth headers that are easy to get
wrong, are in [`docs/stack-contract.md`](docs/stack-contract.md).

Send an event end to end:

```bash
curl -X POST http://localhost:8080/telemetry/events/v1 \
  -H 'Content-Type: application/json' \
  -H 'x-client-id: pulse_infra_smoke' \
  -H 'x-api-key: local-not-a-secret-smoke-key' \
  -d '{"event_id":"01JEXAMPLE","timestamp":"2026-01-01T00:00:00.000Z",
       "type":"track","event":"demo","user":{"user_id":"u1"},
       "context":{"app":{"name":"demo","version":"1.0.0"}}}'
# -> 202 {"status":"accepted"}

docker exec pulse-redis redis-cli -a pulse-local-not-a-secret --no-auth-warning \
  XLEN ingestion-events
```

## Profiles

| Profile | Contents | Purpose | Measured idle footprint |
|---|---|---|---|
| `core` | 3-broker Kafka + fake-gcs | the ingestor's world | **~1.01 GB**, ~0.2 CPU |
| `full` | everything: Kafka + Redis + fake-gcs + gateway | end-to-end local runs | **~1.03 GB**, ~0.3 CPU |
| `lite` | single-broker Kafka + fake-gcs | low-resource fallback | **~0.33 GB**, ~0.1 CPU |

```bash
make up PROFILE=core
make up PROFILE=lite
```

Measured on Apple Silicon (arm64), Docker 29.7.2, 8 CPUs / 8.2 GB available to
Docker, idle after a ~40s settle. Per-broker steady state is ~330 MB with a
512 MB JVM heap cap, so **the three brokers are essentially the entire
footprint** — Redis, fake-gcs and the gateway are ~10–16 MB each. `full` needs
about 1 GB, not 8; the ceiling you will actually hit is CPU during a heavy
produce, not memory.

**Kafka runs as a multi-broker cluster by default**, because a single broker
cannot exercise consumer-group rebalancing or ISR behaviour — exactly the
failure modes `pulse-ingestor`'s commit-after-upload correctness depends on.
`lite` exists for laptops under memory pressure and buys ~700 MB by giving up
those behaviours entirely; see [`docs/divergences.md`](docs/divergences.md).

`lite` and `core`/`full` both bind host port 19092 and **must not run
simultaneously**. `make reset` between profile switches.

## Lifecycle

Three operations, and what each does to your data:

| Command | Containers | Volumes | Topics / bucket | Events in Redis, objects in GCS |
|---|---|---|---|---|
| `make up` | created, waited healthy | created if absent | created if absent (idempotent) | preserved |
| `make down` | removed | **kept** | kept | **kept** |
| `make reset` | removed | **deleted** | gone, recreated on next `up` | **gone** |

**What survives a restart** (`make down && make up`): Kafka log directories,
Redis AOF, and the fake-GCS filesystem backend, all in named volumes. Verified:
an event and an object written before `down` were both still there after `up`,
and bootstrap reported every topic and the bucket as already existing.

**What a reset destroys**: only this stack's own six named volumes, by name,
one at a time. It never runs `docker system prune` or `docker volume prune`, and
it never touches a volume that isn't ours. After a reset, the next `make up`
recreates the identical contract from nothing — verified twice, with matching
topic/partition/RF output both times.

Other targets: `make ps`, `make logs`, `make health`, `make topics`,
`make buckets`, `make footprint`, `make digests`, `make kafka-shell`, `make help`.

## Reproducibility

Every pulled image is pinned by **manifest-list digest** in
[`images.lock`](images.lock), with the resolution date and the architectures
verified. Manifest-list rather than per-platform digests on purpose: a digest
resolved on an arm64 laptop cannot be pulled on amd64, and this stack has to
come up on both.

> **amd64 is not yet verified on a native host.** Everything above was built and
> verified on arm64. All three images carry `linux/amd64` in their index, and an
> amd64 image pulls and executes here under emulation — but no one has run
> `make up && make verify` on a real amd64 machine. If you are on Intel or
> Linux, doing that once closes the last open question in this repo.

```bash
make digests    # re-resolve from the registry, report drift, write nothing
```

The bump procedure is deliberate and documented at the top of `images.lock`:
resolve, edit `images.lock` **and** `compose/compose.yaml` together, then
`make reset && make up && make verify`.

The one exception is the gateway, which is built from source at
`../pulse-gateway` using that repo's own `Dockerfile` — so its bytes follow
whatever commit is checked out there. Same recipe as CI or any other consumer of
that image, which is the point of it living in the gateway repo rather than
here.

## Verification

`make verify` runs against a live `full` stack and prints what it actually
observed, not just pass/fail:

1. Stack health — no unhealthy containers
2. Create a topic; produce a message; consume it back; describe the topic
3. Write an object at the agreed path; read the same bytes back; list the bucket
4. Stop a broker; produce with `acks=all` and read back from the degraded
   cluster; show the under-replicated partitions; restart the broker
5. Gateway ingest via API key **and** via a minted JWT; confirm both land in the
   Redis stream and that only the JWT envelope carries `event_header`
6. Read the Ports table in `docs/stack-contract.md` and probe every row: an
   address marked `live` that does not answer **fails the suite**, and a row
   marked `contracted-not-yet-listening` that *does* answer is reported as drift
7. Fill `ingestion-logs` to its cap to induce a real delivery failure, then
   confirm the refused payload is retained whole in `ingestion-dlq` with
   `error_reason=logs_list_full` — not dropped. Restores the list depth after

Check 7 is the only check that mutates state, which is why it runs last. It
fails rather than skips if a running gateway has no `REDIS_LIST_DLQ`: that means
the image is older than the config it was started with, which is drift this
suite exists to catch.

Cold start, reset and reproducibility are lifecycle operations rather than
suite checks — `make reset && make up` twice, which the table above documents.

Check 6 is what keeps the contract page honest: the table four other repos
resolve addresses from is now machine-checked against the stack it claims to
describe, in both directions. Rows that are deliberately not live — `internal`,
`live (lite)`, `contracted-not-yet-listening` — are skipped **with their reason
printed**, never silently.

## Troubleshooting

Failure modes actually hit while building this, in rough order of likelihood:

**`Cannot connect to the Docker daemon`** — Docker Desktop isn't running.
`open -a Docker`, wait for it, retry.

**Gateway returns `401 {"error":"unauthorized"}`** — API-key auth needs **two**
headers: `x-api-key` *and* `x-client-id`. The client id keys the store, so a
request with only the key is a flat 401 with an empty `client_id` in the gateway
log. This is not documented in the gateway repo. If you sent both an API key and
an `Authorization` header, that is also a 401 — ambiguous identity, by design.

**Gateway returns `400` listing required fields** — the event schema is stricter
than it looks: `event_id`, `timestamp`, `type` and `event` are all required,
`user` needs at least one of `user_id`/`anonymous_id`/`device_id`, and `context`
needs at least one populated field anywhere inside it. Validation failures are
`400`, not `422`. The quickstart payload above is a valid minimum.

**Gateway logs `no .env file found, reading from environment`** — expected and
harmless in a container. Config comes from `compose/gateway.env`; the gateway
looks for a `.env` in its working directory first and warns when there isn't one.

**Gateway build fails** — `../pulse-gateway` must be checked out next to this
repo. Only the `full` profile needs it; `core` and `lite` don't build it at all.

**`Bind for 0.0.0.0:19092 failed: port is already allocated`** — either `lite`
and `core`/`full` are both up (they share 19092), or something else on the
laptop holds the port. `make reset`, then check with
`lsof -nP -iTCP:19092 -sTCP:LISTEN`.

**A consumer sees no topics right after startup** — if you are starting
consumers from your own scripts rather than `make up`, note that
`docker compose up --wait` returns before a one-shot bootstrap container has
finished: it treats "running" as satisfied. `make up` blocks on the bootstrap
container exiting 0, which is why it is the supported entry point.

**Under-replicated partitions after a broker restart** — normal for a few
seconds while the returning broker catches up. `make topics` shows ISR
recovering. If it persists, check that broker's logs with `make logs`.

## Layout

```
pulse-infra/
├── README.md                          this file
├── Makefile                           lifecycle: up / down / reset, inspection, verify
├── images.lock                        pinned digests + resolution date + bump procedure
├── compose/
│   ├── compose.yaml                   all services, three profiles, health gates
│   └── gateway.env                    all 47 gateway config vars (local fixtures)
├── bootstrap/
│   ├── bootstrap.sh                   readiness gates, topics, bucket (idempotent)
│   ├── verify.sh                      the 17-check verification suite
│   ├── resolve-digests.sh             digest drift report
│   ├── mint-dev-jwt.sh                generate a local HS256 token
│   └── fixtures/
│       └── api_keys.local.json        obviously-fake gateway auth fixtures
└── docs/
    ├── stack-contract.md              the interface four-plus repos code against
    └── divergences.md                 where local lies to you
```

## Extension point: Iceberg silver

Deliberately not built. The silver worker reads GCS staging and writes Iceberg,
and neither side of that is exercised locally, so there is no Iceberg REST
catalog here. When it comes into scope it belongs in `compose.yaml` as its own
profile — not added to `full`, which would tax every end-to-end run with a
component nothing in the local loop reads.
