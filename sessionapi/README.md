# sessionapi

Thin REST API over Postgres for browsing past detection sessions. Exists
because the frontend can't query Postgres directly — this is the only
service that does. It deliberately does **not** expose live/in-progress
session state: that's already covered by [`session-sse`](../session-sse)'s
`/events` stream, so this only ever reads the append-only
`<detector>_detection_sessions_log` tables (written by each detector's
RisingWave pipeline, see [`risingwave`](../risingwave)).

## Run

Requires `infra` (Postgres) and `risingwave` (populating the
`*_detection_sessions_log` tables) running first.

```bash
docker compose up --build
curl 'http://localhost:8090/sessions?detector=human&limit=10'
```

## Configuration (env vars)

| Var | Default | Description |
|---|---|---|
| `POSTGRESQL_HOST` | `postgres` | Postgres host. |
| `POSTGRESQL_PORT` | `5432` | Postgres port. |
| `POSTGRESQL_USER` | `vms` | Postgres user. |
| `POSTGRESQL_PASS` | `vms` | Postgres password. |
| `POSTGRESQL_DB` | `vms` | Postgres database. |
| `DETECTOR_TYPES` | `human,car` | Comma-separated allowlist of detector types — must match `risingwave`'s `DETECTOR_TYPES`. Used both to validate the `detector` query param and as the source of the `<detector>_detection_sessions_log` table name, so only a known-good value is ever interpolated into SQL. |
| `HTTP_ADDR` | `:8090` | HTTP listen address. |
| `DEFAULT_LIMIT` | `50` | Default page size for `/sessions` when `limit` isn't given. |
| `MAX_LIMIT` | `500` | Hard cap on `limit`, regardless of what's requested. |

## HTTP API

- `GET /sessions?detector=human&camera_id=cam/hum_det&limit=50&before=<RFC3339>`
  - `detector` (required) — must be one of `DETECTOR_TYPES`, else `400`.
  - `camera_id` (optional) — exact match filter.
  - `limit` (optional) — defaults to `DEFAULT_LIMIT`, capped at `MAX_LIMIT`.
  - `before` (optional) — RFC3339 timestamp cursor; returns sessions that
    started before it. Combine with the last row's `start_time` from a
    previous page to paginate further back.
  - Returns a JSON array ordered by `start_time` descending:
    `{"session_id", "camera_id", "start_time", "end_time", "count"}`
    (`end_time` is only ever present since this table is append-only on
    session end).
- `GET /healthz` — pings Postgres; `503` if unreachable, unlike this repo's
  other services' unconditional-`200` healthz, because Postgres access is
  this service's entire job.
