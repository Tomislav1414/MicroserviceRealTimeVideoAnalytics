
# Orchestrates the whole pipeline across this repo's per-service
# docker-compose.yaml files. `infra` owns the `vms-local` network that
# every other service joins as external, so it must come up first and go
# down last. Detectors need risingwave's kafka-init to have created their
# `<type>-detections` topics first (KAFKA_AUTO_CREATE_TOPICS_ENABLE=false in
# infra), so risingwave precedes them here.
services := "infra mock-rtsp grabber decoder risingwave cardetector humandetector firedetector session-sse sessionapi frontend"

# Start every service, in dependency order.
up:
    #!/usr/bin/env bash
    set -euo pipefail
    for svc in {{services}}; do
        echo "==> starting $svc"
        (cd "$svc" && docker compose up -d --build --wait)
    done

# Stop every service, in reverse dependency order, and wipe named volumes
# (Postgres/Kafka/pgadmin data) so every restart starts from a clean slate --
# this is a dev/demo stack, not something with data worth persisting across
# restarts, and RisingWave itself already resets on restart (--in-memory).
down:
    #!/usr/bin/env bash
    set -euo pipefail
    for svc in $(echo "{{services}}" | tr ' ' '\n' | tac); do
        echo "==> stopping $svc"
        (cd "$svc" && docker compose down -v)
    done

# Stop then start everything.
restart: down up

# Show every running container on the pipeline's network.
status:
    docker ps --filter network=vms-local --format "table {{{{.Names}}}}\t{{{{.Status}}}}\t{{{{.Ports}}}}"

# Tail logs for one service, e.g. `just logs grabber`.
logs service:
    cd {{service}} && docker compose logs -f --tail=100
