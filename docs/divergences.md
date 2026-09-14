# Divergences: local vs GCP production

Short and blunt, because the point of this page is to stop a green local run
being mistaken for evidence.

**A green local suite is not a deployable artifact.**

## The big one: GCS auth is never exercised

`fake-gcs-server` has **no IAM**, accepts **unauthenticated** requests, serves
**HTTP** instead of HTTPS, and returns **different error bodies** from real GCS.

Consequence: **`pulse-ingestor`'s GCS write path is unverified for auth until the
dev environment.** Everything about credentials, scopes, service accounts, token
refresh, bucket-level permissions and the retry behaviour that depends on
distinguishing a 401 from a 403 from a 429 is untested locally. Code that writes
objects perfectly here can fail on its first real write.

Classes of bug that pass locally and fail later:

- Missing or wrong service-account scope; ADC not wired up at all.
- Error handling keyed on GCS error *bodies* or codes this emulator doesn't emit.
- Retry logic that treats an auth failure as retryable and hammers the API.
- Anything depending on TLS: cert pinning, proxy behaviour, handshake timeouts.

## Nothing in this stack produces data

Topics and buckets are provisioned and then left **empty**. Redis keys are not
even created — they appear on first write, and this stack never writes. Every
byte that flows locally is put there by a consumer you started yourself, or by a
verification check that cleans up after itself.

Consequence: **a green `make up` proves the plumbing exists, not that anything
flows through it.** The producers are elsewhere and, at their current commits,
partly unwritten — `pulse-gateway` has no Kafka producer at all, so the
`ingestion-*` topics have never been fed by the thing the platform diagram says
feeds them. The first real dual-write divergence (Redis accepted, Kafka didn't,
or the reverse) becomes possible only when that lands, and nothing in this
stack's history will have tested it.

Track producer status in the producing repo. This page only promises that the
destination exists and behaves.

## Reserved ports are not served ports

`gateway:8080` and `gateway:9090` appear in the Ports table with status
`external`. That means the numbers are **allocated so nothing else takes them**,
not that anything answers. This stack does not run those listeners.

9090 is the sharper case: `pulse-gateway`'s `ADR-009` ratified a gRPC transport
there, and every `GRPC-*` requirement is `MISS` at its current commit. So the
address is stable and safe to code against, and a connection to it fails.
`pulse-client` is specified to drive HTTP **and** gRPC; locally it can drive
neither unless you start the gateway yourself from its repo, and the gRPC leg
not even then.

## Redis: no backpressure, and local caps are tighter

Redis here is a real Redis with `maxmemory 512mb` and `maxmemory-policy
noeviction`, so a write that cannot be served **fails loudly** rather than
silently evicting someone's key. That much is this stack's promise and `make
verify` check 5 asserts it.

Everything past that is the writer's design, not ours: trimming, retention,
caps, what gets dead-lettered and what is dropped. Two things are worth carrying
anyway, because they shape any consumer built here:

- **Nothing applies backpressure.** A slow consumer does not slow a producer
  down; it just falls behind. **Consumer lag is the metric that matters**, in
  both local and production.
- **Local caps are deliberately small** — the registered writers use ~10k here
  against ~100k in their own example configs, so the lossy path shows up on a
  laptop instead of in the dev environment. The *absolute* numbers therefore
  mean nothing for capacity planning.

Whether a given overflow is recoverable is the writer's contract with its
readers — for the `ingestion-*` keys, see `pulse-gateway`. Do not assume from
this page that a dropped record is gone, or that it is kept.

## No failure injection of any kind

Absent locally, present in production:

- **No network partitions, no injected latency.** Every hop is loopback on one
  host. Timeout tuning, retry budgets, and partition-tolerance assumptions are
  untested; a consumer that would thrash under 200ms of cross-zone latency looks
  perfect here.
- **No quotas or throttling.** No Kafka client quotas, no GCS rate limits, no
  429s. Code that ignores throttling responses passes.
- **No object lifecycle rules.** Nothing expires the staging bucket. In
  production "staging" implies an expiry policy; locally objects live until
  `make reset`. A consumer that silently depends on old objects still being
  there will pass locally.
- **No disk-pressure or memory-pressure behaviour.** Redis has a 512MB
  `maxmemory` with `noeviction`, so it will refuse writes rather than evict —
  matching production *policy*, but at a size that will not be hit.

## `lite` profile: no rebalancing, no ISR

On `lite` there is **one broker and RF=1**. That means **no consumer-group
rebalancing and no ISR behaviour** — precisely the failure modes
`pulse-ingestor`'s commit-after-upload correctness depends on. A single broker
also cannot lose a broker, so the survivability test is meaningless.

Use `lite` when a laptop is under memory pressure and you need the stack at all.
Do not use it to validate consumer correctness, and do not report a `lite` run as
a passing integration run. `core`/`full` are the default for a reason.

## Kafka config differs in ways that matter

| Setting | Local | Production expectation |
|---|---|---|
| Brokers | 3 (1 on `lite`) | more, across zones |
| Replication factor | 3 (1 on `lite`) | 3+, rack-aware |
| `min.insync.replicas` | 2 (1 on `lite`) | 2+ |
| `group.initial.rebalance.delay.ms` | `0` | default (3000) — batched joins |
| `log.retention.hours` | 24 | policy-driven, longer |
| Auth | **PLAINTEXT, no auth, no TLS** | SASL/TLS |
| Heap | 512MB per broker | sized to the host |

`group.initial.rebalance.delay.ms=0` makes rebalances immediate so they are
observable in development. It also means local rebalance timing looks nothing
like production, where the delay batches joining members.

**No broker authentication or encryption locally.** Every listener is
PLAINTEXT. Nothing about the SASL/TLS path is exercised.

## Consumer images are not this stack's problem — or its guarantee

Every image **this stack runs** is digest-pinned in `images.lock`, multi-arch,
and bumped only deliberately. Since 2026-09-14 nothing here is built from
source, so there is no "except the gateway" caveat any more: what you run is
what the lockfile says.

That guarantee stops at the network boundary. A consumer you start alongside is
reproducible only to the extent its own repo makes it so, and two laptops
running different consumer commits will both report a perfectly clean stack
here — because from this side they are indistinguishable. **A green run here
says nothing about which version of anything else you are running.**
