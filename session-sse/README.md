# session-sse

Reads the shared `sessions` Kafka topic (written by each detector's
RisingWave pipeline — see [`risingwave`](../risingwave)) and fans it out to
HTTP clients over Server-Sent Events. This is the last stage before the
frontend (frontend not built yet — this service is the thing it will
connect to).

## Why no consumer group

The reader uses a plain partition assignment (`Partition: 0`, no `GroupID`),
not a consumer group. Two reasons:

- This service is a singleton in-memory fan-out hub — the client list and
  history cache live in process memory, so running multiple replicas
  wouldn't share either, and there's nothing to divide among them anyway
  (`sessions` has a single partition).
- A consumer group would actually break the intended behavior: kafka-go
  auto-commits offsets by default, so `StartOffset: FirstOffset` only
  applies once, before a group has any committed offset. Every later
  restart would resume mid-stream instead of replaying the whole topic, so
  the in-memory history cache used for the SSE history replay (see below)
  would only ever be correct on the very first run.

## History replay

A client connecting to `/events` immediately receives the current state of
every session in the in-memory cache (as pseudo-STARTED/DANGER_ZONE_ENTRY
frames), then stays subscribed for live updates. The cache is keyed by
`session_id`; a later event for the same ID (e.g. `ENDED`) overwrites the
earlier one, and closed sessions (`ENDED`, `DANGER_ZONE_EXIT`) are evicted
oldest-first once the cache exceeds `MAX_CACHE_SESSIONS`, so memory doesn't
grow unbounded over a long-running demo. Open sessions are never evicted.

## Run

Requires `infra` (Kafka) and `risingwave` (producing `sessions`) running
first.

```bash
docker compose up --build
curl -N http://localhost:8095/events        # all cameras
curl -N http://localhost:8095/events?camera_id=cam/hum_det
```

## Configuration (env vars)

| Var | Default | Description |
|---|---|---|
| `KAFKA_BROKERS` | `kafka:9092` | Comma-separated Kafka bootstrap brokers. |
| `SESSIONS_TOPIC` | `sessions` | Topic written by each detector's RisingWave pipeline. |
| `HTTP_ADDR` | `:8095` | HTTP listen address. |
| `MAX_CACHE_SESSIONS` | `5000` | Max cached sessions before oldest-closed eviction kicks in. |

## HTTP API

- `GET /events` — SSE stream. Optional `?camera_id=` filters to one camera.
  Event names: `started`, `ended`, `danger_zone_entry`, `danger_zone_exit`.
  Each frame's `data` is the same JSON shape published to the `sessions`
  topic (`kind`, `session_id`, `camera_id`, `detector`, `start_time`,
  `end_time`, `count`, `zone_id`, `zone_name` — fields present depend on
  `kind`).
- `GET /healthz` — liveness check.

No frontend/dashboard page yet — this service is API-only for now.
