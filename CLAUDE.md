# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

`README.md` covers quickstart, profiles, lifecycle and troubleshooting;
`docs/stack-contract.md` is the interface the other repos code against;
`docs/divergences.md` says where local lies. Read those for detail — this file
covers only what they don't.

## Commands

```bash
make up                  # cold start, full profile (~25s warm, ~80s first build)
make up PROFILE=core     # or lite
make verify              # 14 checks against a live full stack
make down                # stop, keep data
make reset               # delete this stack's volumes only
make digests             # digest drift report; writes nothing
```

There are no unit tests and no linter here — the artifact is a running stack, so
`make verify` is the test suite. It needs the `full` profile up; checks 2–4 work
on `core`, and check 5 self-skips without Redis. **No check drives another
project** — if a check would need a consumer running, it belongs in that repo.
Checks 6 and 7 make the paperwork executable: 6 parses the Ports table out of
`docs/stack-contract.md` and probes it (mark a row `live` and it must answer),
7 reads `registry/dependencies.tsv` and asserts every registered topic and
bucket exists.

## Scope boundary — hold it

**Local development only.** Not staging, not production. Infra repos drift
toward production scope by default, and the README's scope statement is load
bearing: no IAM, no TLS, fake credentials on purpose, disposable data. Silver /
Iceberg is deliberately out of the local loop — there is a marked extension
point in `compose/compose.yaml` and nothing built behind it.

**Backing services only — this is the boundary that matters most.** This repo
provides Kafka, Redis and object storage. It does **not** build, start,
configure, health-check or test any other Pulse project. The dependency arrow
points one way: consumers register what they need in
`registry/dependencies.tsv` and run their own containers against the addresses
in the contract.

The pressure is always toward the opposite. Adding a consumer to `compose.yaml`
is a two-line change that feels helpful and costs: this stack then builds their
code, holds their config, breaks when their build breaks, and its test suite
starts asserting their behaviour. That is exactly what was unwound on
2026-09-14, when the gateway service, its 57-var env file and its auth fixtures
were removed from here. **Do not add a consumer service back.** If someone needs
one running, they run it from their repo on the `pulse-infra` network.

## Where the real complexity is

- **`compose/compose.yaml` is the whole stack.** Three profiles in one file;
  `core`/`full` run three brokers, `lite` runs a separate single-broker service.
  YAML anchors (`x-kafka-*`) hold the settings shared by all three brokers, so a
  cluster-wide change is one edit, not three.
- **`bootstrap/bootstrap.sh` owns readiness**, not `depends_on`. It gates on two
  things a container healthcheck can't see: all expected brokers present in
  metadata, *and* a topic actually being creatable (a cluster with too few
  in-sync replicas answers metadata and still refuses creation). It is
  idempotent — re-running against a live stack is a no-op.
- **`make up` blocks on the bootstrap container exiting 0.** `docker compose up
  --wait` does NOT: it treats a one-shot container as satisfied once it is
  merely running, so on `core`/`lite` it would return before topics exist and a
  consumer would race the bootstrap. Don't "simplify" that wait away.
- **RF=3 with `min.insync.replicas=2`** is what makes single-broker loss both
  survivable and a meaningful test. min ISR=3 makes the cluster unwritable on
  one loss; RF=1 makes the test vacuous.

## Gotchas verified against the running stack

- **Consumers' in-network vs host addresses are the first thing that breaks.**
  Every consumer `.env.example` ships `localhost:…`; inside a container that is
  the container itself, so the symptom is connection-refused, not a name error.
  The Ports table's two columns exist for exactly this.
- **No consumer's config lives here any more.** `compose/gateway.env`, the API
  key fixtures and `mint-dev-jwt.sh` were removed on 2026-09-14 and belong to
  `pulse-gateway`. Do not accept them back: the moment this repo holds another
  project's tuning, it starts failing when their build breaks.
- **Nothing in this stack produces to Kafka or Redis.** Topics and keys are
  provisioned/reserved and left empty; their registered writers fill them from
  their own repos. `pulse-gateway` has no Kafka producer and no gRPC listener at
  its current commit (gRPC is specified in its `ADR-009` / `GRPC-001`…`011`, all
  `MISS`), but that is now *their* status to track, not ours — we only hold the
  port reservations.
- **The Kafka image has `bash` and BusyBox `wget`, but no `curl`.** Bucket
  creation uses `wget --post-data`.
- **`lite` and `core`/`full` share host port 19092** and cannot run together.

## Conventions

- **Everything is digest-pinned** via `images.lock` (manifest-list digests, so
  both arm64 and amd64 work). Bumping is deliberate: `make digests`, then edit
  `images.lock` and `compose/compose.yaml` together, then reset/up/verify. Never
  bump an image as a side effect of another change, and never fall back to a tag.
- **Never a destructive Docker command.** `make reset` removes this stack's six
  named volumes by name, one at a time, only if present. No `system prune`, no
  `volume prune`, nothing outside this stack.
- **This repo does no git operations.** That is the user's job.
- **Anything in `docs/stack-contract.md` is a breaking change for four-plus
  repos.** Topic names, ports, bucket paths, Redis keys and auth header shapes
  are cross-repo; change them in `pulse-gateway`'s config and the consumers in
  the same breath, and update the contract table.
- **A new listener means a contract entry, in the same change as the code** —
  never a follow-up. Consumers are specified not to restate an address this repo
  owns, so a missing row blocks them rather than degrading them. An interface
  that is agreed but unbuilt gets a row with status
  `contracted-not-yet-listening`; it is not left off the page. Publishing the
  host port and flipping that row to `live` are one change, and `make verify`
  check 6 enforces both directions. See "Changing this contract".
- Credential fixtures that remain (the Redis password) stay obviously fake and
  are named to prove it (`local-not-a-secret-*`). No consumer's credentials
  live here.
- New troubleshooting entries in the README come from failures actually hit, not
  anticipated ones.
