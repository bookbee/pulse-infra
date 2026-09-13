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

## The gateway does not write Kafka

The platform diagram shows `pulse-gateway → Kafka`. That edge **does not exist in
the gateway at this commit** — `internal/queue/` contains only a Redis producer,
and `QUEUE_BACKEND=redis` is the only backend that resolves. Kafka appears in
one doc comment as a future implementation.

Consequence: **no local run exercises dual-write divergence**, because there is
only one write path. When the gateway gains a Kafka producer, the interesting
failure — Redis accepted and Kafka didn't, or vice versa, leaving the two paths
disagreeing — becomes possible for the first time, and nothing in this stack's
history will have tested it. Kafka is provisioned here and fed by
`pulse-ingestor`'s own tests and `pulse-client`, not by the gateway.

## The gateway has no gRPC endpoint

`pulse-client` is specified to drive "HTTP + gRPC". The gateway serves **HTTP
only** — no gRPC dependency, no gRPC server. Any client work against a gRPC
surface is untestable locally because the surface does not exist.

## Redis losses are silent and local caps are tighter

Both Redis destinations shed data under lag rather than applying backpressure:
streams trim to `MAXLEN` (approximate), and the log list's Lua script drops the
write outright once the cap is hit.

Local caps are **10k**; the gateway's own example config uses **100k**, and a
real deployment would be larger still. So local runs hit the lossy path **sooner
than production would** — deliberately, so you see it here. The flip side: the
*absolute* numbers mean nothing for capacity planning. Consumer lag is the metric
that matters in both places.

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

## The gateway image is not reproducible

Every other image is digest-pinned. The gateway is **built from whatever commit
is checked out in `../pulse-gateway`**. It now builds from that repo's own
`Dockerfile`, so the *recipe* is shared with CI and any other consumer — but the
*source* still follows the sibling checkout. Two laptops on different gateway
commits run different gateways while both report a clean stack. Closing that
means publishing tagged gateway images and pinning one here, which is a decision
for when the gateway has a release process.
