# frontend

Operator console for the pipeline: live camera view, real-time
notifications, and session history. React + Vite + TypeScript, no
server-side rendering — this is an internal SPA with no SEO/public-facing
concerns.

It talks to three other services directly from the browser (no proxying):

- **mediamtx** — live video via WebRTC/WHEP (`lib/whep.ts`), bypassing the
  Kafka pipeline entirely for pixel delivery. Low-latency, and decoupled
  from the detection path on purpose.
- **[`session-sse`](../session-sse)** — one unfiltered `EventSource` for
  every camera's `started`/`ended`/`danger_zone_entry`/`danger_zone_exit`
  events, mounted once at the app root (`context/NotificationsContext.tsx`)
  so it survives navigating between pages. This is what lets a
  notification for camera A show up while you're looking at camera B.
- **[`sessionapi`](../sessionapi)** — REST history of ended sessions
  (`pages/HistoryPage.tsx`); "live" state is deliberately not duplicated
  here, `session-sse` already covers it.

## Cameras are hardcoded

`src/config/cameras.ts` is a plain array, not fetched from anywhere — see
the comment there for why (grabber's camera list / Kafka partition count
are fixed at boot, so dynamic camera registration is out of scope). To add
a camera: wire a new `mock-cam-*` service in
`mock-rtsp/docker-compose.yaml`, add its ID to the grabber's `CAMERAS` env
var, then add an entry to `config/cameras.ts`.

## Run (dev)

```bash
cp .env.example .env   # defaults already point at localhost's published ports
npm install
npm run dev
```

Requires `infra`, `mock-rtsp`, `grabber`, `decoder`, `risingwave`,
`session-sse`, and `sessionapi` running for anything to actually show up —
this frontend has nothing to render on its own.

## Run (container)

```bash
docker compose up --build
```

**Gotcha**: Vite inlines `VITE_*` env vars into the built JS at build time,
not container start time — they're passed as Docker build args (see
`Dockerfile`), not `environment:` entries. Changing one means rebuilding
the image (`docker compose up --build`), not just restarting the
container.

## Configuration (env vars, build-time only)

| Var | Default | Description |
|---|---|---|
| `VITE_MEDIAMTX_WHEP_BASE` | `http://localhost:8889` | mediamtx's WHEP endpoint base. |
| `VITE_SESSION_SSE_BASE` | `http://localhost:8095` | session-sse's SSE endpoint base. |
| `VITE_SESSION_API_BASE` | `http://localhost:8090` | sessionapi's REST endpoint base. |
