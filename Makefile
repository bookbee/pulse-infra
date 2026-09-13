# pulse-infra — local development stack lifecycle.
#
# Three operations, as documented in README.md:
#   make up      start (cold start from nothing works)
#   make down    stop, PRESERVING data
#   make reset   destroy this stack's volumes and re-bootstrap to known-clean
#
# PROFILE selects the service set: full (default) | core | lite

COMPOSE_FILE := compose/compose.yaml
PROFILE      ?= full
DC           := docker compose -f $(COMPOSE_FILE) --profile $(PROFILE)

# Only this stack's own volumes. Never `docker volume prune`.
VOLUMES := pulse-infra-kafka-1-data pulse-infra-kafka-2-data pulse-infra-kafka-3-data \
           pulse-infra-kafka-lite-data pulse-infra-fake-gcs-data pulse-infra-redis-data

# lite runs a single broker and its own bootstrap service; core/full share theirs.
BOOTSTRAP_SVC := $(if $(filter lite,$(PROFILE)),pulse-bootstrap-lite,pulse-bootstrap)

KAFKA_IMAGE := apache/kafka@sha256:77e3df9054047a88b520d0cc46e16696d3b22022e1d580aeccd2632df6532837
BOOTSTRAP   := kafka-1:9092,kafka-2:9092,kafka-3:9092
NET         := pulse-infra
BUCKET      := pulse-staging-local

.PHONY: help up down reset ps logs health verify footprint digests topics buckets kafka-shell

help:
	@echo "pulse-infra — local development stack (LOCAL DEV ONLY)"
	@echo
	@echo "  make up [PROFILE=full|core|lite]   start the stack (cold start safe)"
	@echo "  make down                          stop, preserving data"
	@echo "  make reset                         wipe this stack's volumes, re-bootstrap"
	@echo "  make ps / logs / health            inspect"
	@echo "  make verify                        run the full verification suite"
	@echo "  make footprint                     measure memory and CPU per service"
	@echo "  make topics / buckets              show the provisioned contract"
	@echo "  make digests                       re-resolve image digests (does NOT write)"
	@echo
	@echo "  PROFILE=$(PROFILE)"

# ─── Lifecycle ───────────────────────────────────────────────────────────────

# `docker compose up --wait` treats a one-shot container as satisfied the moment
# it is running, NOT when it exits 0 — so on the core and lite profiles it would
# return while topics and the bucket are still being created, and a consumer
# starting immediately would race the bootstrap. Only `full` is implicitly safe
# (the gateway depends on bootstrap completing). So block on it explicitly.
# --build is deliberate: compose only builds when the image tag is ABSENT, so
# without it an edit to ../pulse-gateway is silently ignored and you debug a
# stale binary. Layer caching makes the no-change case near-instant.
up:
	$(DC) up -d --wait --build
	@code=$$(docker wait $(BOOTSTRAP_SVC) 2>/dev/null | tail -1); \
	 if [ -z "$$code" ]; then \
	   echo "WARNING: bootstrap container $(BOOTSTRAP_SVC) not found — contract unverified"; \
	 elif [ "$$code" != "0" ]; then \
	   echo "bootstrap FAILED (exit $$code) — topics/bucket are NOT provisioned:"; \
	   docker logs $(BOOTSTRAP_SVC) 2>&1 | tail -20; \
	   exit 1; \
	 else \
	   echo "bootstrap complete — topics and bucket provisioned"; \
	 fi
	@echo
	@$(MAKE) --no-print-directory health

down:
	$(DC) down
	@echo "Stopped. Volumes preserved — Kafka log dirs, Redis AOF and the bucket survive."

# Reset must be reliable: reproducible test runs depend on it. Containers first,
# then only this stack's named volumes, then bootstrap runs again on next `up`.
reset:
	docker compose -f $(COMPOSE_FILE) --profile full --profile core --profile lite down --remove-orphans
	@# Remove only the volumes that actually exist, so a missing one (e.g. the
	@# lite volume on a machine that never ran lite) is not an error that could
	@# mask a real failure to remove a volume that IS there.
	@for v in $(VOLUMES); do \
	  if docker volume inspect "$$v" >/dev/null 2>&1; then \
	    docker volume rm "$$v" >/dev/null && echo "  removed volume $$v"; \
	  fi; \
	done
	@echo 'Reset. The next "make up" re-creates topics and the bucket from nothing.'

# ─── Inspection ──────────────────────────────────────────────────────────────

ps:
	$(DC) ps

logs:
	$(DC) logs -f --tail=100

health:
	@docker compose -f $(COMPOSE_FILE) --profile full --profile core --profile lite ps \
	  --format '{{.Name}}\t{{.State}}\t{{.Status}}' 2>/dev/null | sort || true

topics:
	@docker run --rm --network $(NET) $(KAFKA_IMAGE) \
	  /opt/kafka/bin/kafka-topics.sh --bootstrap-server $(BOOTSTRAP) --describe

buckets:
	@docker run --rm --network $(NET) --entrypoint wget $(KAFKA_IMAGE) \
	  -q -O- "http://fake-gcs:4443/storage/v1/b?project=pulse-local"
	@echo

kafka-shell:
	docker run --rm -it --network $(NET) --entrypoint bash $(KAFKA_IMAGE)

# ─── Footprint: a laptop constraint is a real constraint ─────────────────────

footprint:
	@echo "Per-container usage for the running profile:"
	@docker stats --no-stream \
	  --format 'table {{.Name}}\t{{.MemUsage}}\t{{.MemPerc}}\t{{.CPUPerc}}'
	@echo
	@echo "Docker host total:"
	@docker info --format '  cpus={{.NCPU}} mem={{.MemTotal}}'

# ─── Digests: re-resolve, report, never auto-write ──────────────────────────
# Bumping an image is a deliberate act: run this, then edit images.lock and
# compose/compose.yaml together, then `make reset && make up && make verify`.

digests:
	@./bootstrap/resolve-digests.sh

# ─── Verification suite (see README "Verification") ─────────────────────────

verify:
	@./bootstrap/verify.sh
