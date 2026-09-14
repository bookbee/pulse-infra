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
make verify              # 17 checks against a live full stack
make down                # stop, keep data
make reset               # delete this stack's volumes only
make digests             # digest drift report; writes nothing
```

There are no unit tests and no linter here — the artifact is a running stack, so
`make verify` is the test suite. It needs the `full` profile up; checks 2–4 work
on `core`, and checks 5–7 self-skip what the running profile doesn't have.
Check 7 is the only destructive one — it fills `ingestion-logs` to its cap to
induce a real delivery failure, then restores the depth it found.
Check 6 parses the Ports table out of `docs/stack-contract.md` and probes it, so
**that table is executable**: mark a row `live` and it must answer.

## Scope boundary — hold it

**Local development only.** Not staging, not production. Infra repos drift
toward production scope by default, and the README's scope statement is load
bearing: no IAM, no TLS, fake credentials on purpose, disposable data. Silver /
Iceberg is deliberately out of the local loop — there is a marked extension
point in `compose/compose.yaml` and nothing built behind it. Adding a component
to `full` taxes every end-to-end run, so new services get their own profile.

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

- **API-key auth on the gateway needs TWO headers**: `x-api-key` *and*
  `x-client-id`. The client id keys the store; key-only is a flat 401. Not
  documented in the gateway repo — found by hitting it.
- **`event_header` is on the Redis envelope for JWT requests and absent for
  API-key ones.** Contract, not a bug. Consumers treat it as optional.
- **The event schema is stricter than it looks**: `event_id`, `timestamp`,
  `type`, `event` all required, `user` needs one of three id fields, `context`
  needs at least one populated field anywhere inside it. Failures are `400`.
- **The gateway needs all 57 env vars** (`compose/gateway.env`) — it has no
  defaults and aborts listing every missing one. The count grows with the
  gateway: `SAFE_BUFFER_THRESHOLD` was inert until its `T-1.3` and is now
  **required and validated** in `(0,1]`, and `T-1.2`/`DEL-012` added the DLQ and
  stream-retention keys. Re-check the count against `internal/config` rather
  than trusting this number after a gateway bump.
- **The gateway has no Kafka producer and no gRPC listener** at its current
  commit, despite the platform diagram and `pulse-client`'s README. Both are
  *specified* upstream and neither is built — gRPC in `ADR-009` and
  `GRPC-001`…`GRPC-011`, all `MISS`. Say "no listener", not "no endpoint": the
  gRPC address is real contract (`gateway:9090`, status
  `contracted-not-yet-listening`) and `pulse-client` resolves it from our Ports
  table. Kafka here is provisioned for `pulse-ingestor`, not fed by the gateway.
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
- Credential fixtures stay obviously fake and are named to prove it
  (`local-not-a-secret-*`). JWTs are generated by `bootstrap/mint-dev-jwt.sh`,
  never checked in.
- New troubleshooting entries in the README come from failures actually hit, not
  anticipated ones.
