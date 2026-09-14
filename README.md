# pulse-infra

Local development infrastructure for the Pulse platform: Kafka, Redis and a GCS
stand-in, brought up with one command so every other Pulse repo can be
developed, run and integrated **entirely on a laptop before anything is
committed to Git**.

**This repo provides backing services and nothing else.** It does not build,
start, configure or test any other Pulse project — they run their own containers
and connect in. What gets provisioned comes from
[`registry/dependencies.tsv`](registry/dependencies.tsv), where each project
registers the topics, buckets, Redis keys and ports it needs.

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

Requires Docker (Compose v2) and `make`. No sibling repo needs to be checked
out: nothing here builds another project's code.

```bash
make up          # cold start from nothing, full profile
make verify      # prove the contract: 14 checks, real output
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

Use the host addresses for a process on your laptop and the in-network ones from
a container. Topics `ingestion-events`, `ingestion-signals`, `ingestion-logs` —
6 partitions, RF=3, provisioned from
[`registry/dependencies.tsv`](registry/dependencies.tsv). Full interface details
are in [`docs/stack-contract.md`](docs/stack-contract.md).

Check it is really up:

```bash
make verify      # 14 checks, real output

# or by hand:
docker exec pulse-redis redis-cli -a pulse-local-not-a-secret --no-auth-warning PING
make topics      # every registered topic, with partitions and ISR
make buckets
```

To run a service against it, join this stack's network from your own compose
file — see **Containerizing a consumer** in
[`docs/stack-contract.md`](docs/stack-contract.md). Nothing needs to be added
here for that; if your service needs a topic, key or port, register it in
`registry/dependencies.tsv`.

## Profiles

| Profile | Contents | Purpose | Measured idle footprint |
|---|---|---|---|
| `core` | 3-broker Kafka + fake-gcs | the ingestor's world | **~1.01 GB**, ~0.2 CPU |
| `full` | every backing service: Kafka + Redis + fake-gcs | what consumers connect to | **~1.03 GB**, ~0.3 CPU |
| `lite` | single-broker Kafka + fake-gcs | low-resource fallback | **~0.33 GB**, ~0.1 CPU |

```bash
make up PROFILE=core
make up PROFILE=lite
```

Measured on Apple Silicon (arm64), Docker 29.7.2, 8 CPUs / 8.2 GB available to
Docker, idle after a ~40s settle. Per-broker steady state is ~330 MB with a
512 MB JVM heap cap, so **the three brokers are essentially the entire
footprint** — Redis and fake-gcs are ~10–16 MB each. `full` needs about 1 GB,
not 8; the ceiling you will actually hit is CPU during a heavy produce, not
memory. Consumers you run alongside cost whatever they cost, on top of this.

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

There is no exception any more: this stack builds nothing, so every byte it runs
is digest-pinned. A consumer you run alongside is reproducible to the extent its
own repo makes it so — which is that repo's problem to state, not this one's.

## Verification

`make verify` runs against a live `full` stack and prints what it actually
observed, not just pass/fail:

1. Stack health — no unhealthy containers
2. Create a topic; produce a message; consume it back; describe the topic
3. Write an object at the agreed path; read the same bytes back; list the bucket
4. Stop a broker; produce with `acks=all` and read back from the degraded
   cluster; show the under-replicated partitions; restart the broker
5. Redis itself: refuse an unauthenticated client, round-trip a stream
   (`XADD`/`XRANGE`) and a list (`RPUSH`/`LRANGE`), and confirm
   `maxmemory-policy` is still `noeviction`
6. Read the Ports table in `docs/stack-contract.md` and probe every row: an
   address marked `live` that does not answer **fails the suite**, and a row
   marked `contracted-not-yet-listening` that *does* answer is reported as drift
7. Read `registry/dependencies.tsv` and confirm every registered topic and
   bucket actually exists in the running stack

**No check drives another project.** Proving that a gateway or an ingestor
behaves correctly is that repo's suite, not this one — a check here that needed
a consumer running would be the coupling this layout exists to prevent. What
this suite proves is that the backing services work and that what was registered
was provisioned.

Cold start, reset and reproducibility are lifecycle operations rather than
suite checks — `make reset && make up` twice, which the table above documents.

Checks 6 and 7 are what keep the paperwork honest, from both ends. Check 6
machine-checks the Ports table four other repos resolve addresses from, in both
directions — rows deliberately not live (`internal`, `live (lite)`, `external`,
`contracted-not-yet-listening`) are skipped **with their reason printed**, never
silently. Check 7 does the same for the registry: a row added but never
provisioned is an interface that exists on paper and not in the stack.

## Troubleshooting

Failure modes actually hit while building this, in rough order of likelihood:

**`Cannot connect to the Docker daemon`** — Docker Desktop isn't running.
`open -a Docker`, wait for it, retry.

**Your containerized service cannot reach `kafka-1` / `redis` / `fake-gcs`** —
almost always one of two things. Either it is not on this stack's network (join
`pulse-infra` as an `external` network — see "Containerizing a consumer" in the
stack contract), or it is still using **host** addresses. Every consumer repo's
`.env.example` ships `localhost:…` because it is written for a laptop process;
inside a container `localhost` is the container itself, so you get connection
refused rather than an error that points at the cause.

**Your service starts before topics exist** — bring this stack up first. `make
up` does not return until bootstrap has exited 0, so every registered topic and
bucket is present when it does. Nothing here waits on you.

**Something you registered was never created** — `make verify` check 7 names it.
Re-run `make up`; if it is still missing, `docker logs pulse-bootstrap` will say
why. A row added to `registry/dependencies.tsv` after the stack was already up
is not provisioned until the next `make up`.

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
│   └── compose.yaml                   backing services only, three profiles, health gates
├── registry/
│   └── dependencies.tsv               what each project registered: topics, buckets, keys, ports
├── bootstrap/
│   ├── bootstrap.sh                   readiness gates, then provisions what is registered
│   ├── verify.sh                      the 14-check verification suite
│   └── resolve-digests.sh             digest drift report
└── docs/
    ├── stack-contract.md              the interface four-plus repos code against
    └── divergences.md                 where local lies to you
```

No consumer's config, credentials or build lives here. If you are looking for
the gateway's env file or auth fixtures, they moved to `pulse-gateway` — this
stack no longer runs it.

## Extension point: Iceberg silver

Deliberately not built. The silver worker reads GCS staging and writes Iceberg,
and neither side of that is exercised locally, so there is no Iceberg REST
catalog here. When it comes into scope it belongs in `compose.yaml` as its own
profile — not added to `full`, which would tax every end-to-end run with a
component nothing in the local loop reads.
