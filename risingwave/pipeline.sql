

DROP SINK IF EXISTS __DETECTOR___zone_exit_sink;
DROP SINK IF EXISTS __DETECTOR___zone_entry_sink;
DROP MATERIALIZED VIEW IF EXISTS __DETECTOR___zone_session_ended;
DROP MATERIALIZED VIEW IF EXISTS __DETECTOR___zone_session_live;
DROP VIEW IF EXISTS __DETECTOR___zone_islanded;
DROP VIEW IF EXISTS __DETECTOR___zone_tagged;
DROP MATERIALIZED VIEW IF EXISTS __DETECTOR___zone_hits;
DROP SINK IF EXISTS __DETECTOR___sessions_topic_sink;
DROP SINK IF EXISTS __DETECTOR___session_live_topic_sink;
DROP SINK IF EXISTS __DETECTOR___session_ended_sink;
DROP SINK IF EXISTS __DETECTOR___session_live_sink;
DROP SINK IF EXISTS __DETECTOR___detections_sink;
DROP MATERIALIZED VIEW IF EXISTS __DETECTOR___session_ended;
DROP MATERIALIZED VIEW IF EXISTS __DETECTOR___session_live;
DROP MATERIALIZED VIEW IF EXISTS __DETECTOR___clock;
DROP VIEW IF EXISTS __DETECTOR___session_progress;
DROP VIEW IF EXISTS __DETECTOR___islanded;
DROP VIEW IF EXISTS __DETECTOR___tagged;
DROP VIEW IF EXISTS __DETECTOR___qualified;
DROP SOURCE IF EXISTS __DETECTOR___detections;

-- ---------------------------------------------------------------------------
-- Source: this detector type's own `__DETECTOR__-detections` topic.
-- ---------------------------------------------------------------------------
CREATE SOURCE __DETECTOR___detections (
    cam_id          VARCHAR,
    ts              TIMESTAMPTZ,
    detection_count INT,
    detections      JSONB,
    WATERMARK FOR ts AS ts - INTERVAL '1' SECOND
) WITH (
    connector = 'kafka',
    topic = '__DETECTOR__-detections',
    properties.bootstrap.server = '__KAFKA_BOOTSTRAP__',
    scan.startup.mode = 'latest'
) FORMAT PLAIN ENCODE JSON;

-- ---------------------------------------------------------------------------
-- Confidence threshold (tunable)
-- ---------------------------------------------------------------------------
CREATE VIEW __DETECTOR___qualified AS
SELECT DISTINCT d.cam_id, d.ts
FROM __DETECTOR___detections AS d,
     jsonb_array_elements(d.detections) AS e
WHERE (e->>'confidence')::DOUBLE PRECISION >= __CONFIDENCE_THRESHOLD__
  AND d.ts > NOW() - INTERVAL '__STATE_RETENTION__';

-- ---------------------------------------------------------------------------
-- Session-gap islanding for the live view
-- ---------------------------------------------------------------------------
CREATE VIEW __DETECTOR___tagged AS
SELECT
    cam_id,
    ts,
    CASE
        WHEN LAG(ts) OVER (PARTITION BY cam_id ORDER BY ts) IS NULL
          OR ts - LAG(ts) OVER (PARTITION BY cam_id ORDER BY ts)
               > INTERVAL '__SESSION_GAP_SECONDS__' SECOND
        THEN 1 ELSE 0
    END AS is_new
FROM __DETECTOR___qualified;

CREATE VIEW __DETECTOR___islanded AS
SELECT *,
    SUM(is_new) OVER (PARTITION BY cam_id ORDER BY ts) AS session_seq
FROM __DETECTOR___tagged;

-- ---------------------------------------------------------------------------
-- Per-detection position within its session. seq_in_session picks out the
-- detection that confirms a session as a real incident (see session_live's
-- confirmed_at below) instead of treating the first, possibly noisy,
-- detection as confirmation.
-- ---------------------------------------------------------------------------
CREATE VIEW __DETECTOR___session_progress AS
SELECT
    cam_id,
    ts,
    session_seq,
    ROW_NUMBER() OVER (PARTITION BY cam_id, session_seq ORDER BY ts) AS seq_in_session
FROM __DETECTOR___islanded;

-- ---------------------------------------------------------------------------
-- MV #1 — session_live: "when did it start", updated live
-- ---------------------------------------------------------------------------
CREATE MATERIALIZED VIEW __DETECTOR___session_live AS
SELECT
    cam_id,
    CONCAT('__DETECTOR__', '_', cam_id, '_', CAST(MIN(ts) AS TEXT)) AS session_id,
    MIN(ts) AS start_time,
    MAX(ts) AS last_seen,
    COUNT(*) AS running_count,
    MAX(CASE WHEN seq_in_session = __MIN_INCIDENT_DETECTIONS__ THEN ts END) AS confirmed_at
FROM __DETECTOR___session_progress
GROUP BY cam_id, session_seq;

-- ---------------------------------------------------------------------------
-- Event-time clock.
-- ---------------------------------------------------------------------------
CREATE MATERIALIZED VIEW __DETECTOR___clock AS
SELECT MAX(ts) AS max_ts FROM __DETECTOR___detections;

-- ---------------------------------------------------------------------------
-- MV #2 — session_ended: "when did it end", in EVENT TIME
-- ---------------------------------------------------------------------------
CREATE MATERIALIZED VIEW __DETECTOR___session_ended AS
SELECT
    cam_id,
    session_id,
    start_time,
    last_seen     AS end_time,
    running_count AS count
FROM __DETECTOR___session_live
WHERE last_seen < (SELECT max_ts - INTERVAL '__SESSION_GAP_SECONDS__ seconds' FROM __DETECTOR___clock)
  AND running_count >= __MIN_INCIDENT_DETECTIONS__;

-- ---------------------------------------------------------------------------
-- Danger zones. detector_zones is a shared (untemplated) table applied once
-- by shared.sql, not per-detector — joined here via a temporal join so
-- editing a zone polygon later doesn't retroactively rewrite already-emitted
-- zone sessions. Unlike the main sessionizer above, there's no
-- MIN_INCIDENT_DETECTIONS/confirmed_at confirmation gate: zone entry fires
-- on the very first qualifying overlapping detection, trading false-positive
-- exposure for zero alerting delay (danger zones are safety-critical).
-- ---------------------------------------------------------------------------
-- MATERIALIZED (not a plain VIEW): RisingWave rejects a temporal join
-- (FOR SYSTEM_TIME AS OF PROCTIME()) inside a plain CREATE VIEW with
-- "do not support temporal join for batch queries" — it's only valid
-- directly inside a streaming context (materialized view or sink).
CREATE MATERIALIZED VIEW __DETECTOR___zone_hits AS
SELECT DISTINCT cam_id, ts, zone_id, zone_name
FROM (
    SELECT
        dz.cam_id, dz.ts,
        zone->>'zone_id' AS zone_id,
        zone->>'zone_name' AS zone_name,
        -- Impure UDF calls aren't allowed directly in WHERE on a retract
        -- stream ("may lead to inconsistent results") — RisingWave requires
        -- computing it in the SELECT list here and filtering on the alias
        -- in the outer query instead.
        bbox_overlaps_zone(
            (e->>'x1')::DOUBLE PRECISION, (e->>'y1')::DOUBLE PRECISION,
            (e->>'x2')::DOUBLE PRECISION, (e->>'y2')::DOUBLE PRECISION,
            -- WITH ORDINALITY + ORDER BY: without it RisingWave warns
            -- "array_agg without ORDER BY may produce non-deterministic
            -- result" — polygon vertex order is meaningful (it's a
            -- boundary walk, not a set), so an unordered reassembly could
            -- silently scramble the shape.
            ARRAY(
                SELECT val::DOUBLE PRECISION
                FROM jsonb_array_elements_text(zone->'polygon') WITH ORDINALITY AS t(val, idx)
                ORDER BY idx
            )
        ) AS is_zone_hit,
        (e->>'confidence')::DOUBLE PRECISION AS confidence
    FROM (
        -- RisingWave's temporal join requires the ON clause to compare the
        -- lookup table's distribution key columns against columns from the
        -- left side, not literals — 'human' is templated in directly, so a
        -- literal comparison on detector_type fails with "Temporal join
        -- requires the equivalence join condition includes the key columns
        -- that form the distribution key". Projecting it as a column here
        -- first works around that.
        SELECT d.cam_id, d.ts, d.detections, '__DETECTOR__'::varchar AS detector_type
        FROM __DETECTOR___detections AS d
    ) AS dz
    JOIN detector_zones FOR SYSTEM_TIME AS OF PROCTIME() AS z
      ON z.camera_id = dz.cam_id AND z.detector_type = dz.detector_type,
         jsonb_array_elements(dz.detections) AS e,
         jsonb_array_elements(z.zones) AS zone
) AS scored
WHERE is_zone_hit
  AND confidence >= __CONFIDENCE_THRESHOLD__
  AND ts > NOW() - INTERVAL '__STATE_RETENTION__';

-- Session-gap islanding, same pattern as the main sessionizer above, but
-- partitioned by (cam_id, zone_id) so independent zones on one camera track
-- independently.
CREATE VIEW __DETECTOR___zone_tagged AS
SELECT
    cam_id,
    ts,
    zone_id,
    zone_name,
    CASE
        WHEN LAG(ts) OVER (PARTITION BY cam_id, zone_id ORDER BY ts) IS NULL
          OR ts - LAG(ts) OVER (PARTITION BY cam_id, zone_id ORDER BY ts)
               > INTERVAL '__SESSION_GAP_SECONDS__' SECOND
        THEN 1 ELSE 0
    END AS is_new
FROM __DETECTOR___zone_hits;

CREATE VIEW __DETECTOR___zone_islanded AS
SELECT *,
    SUM(is_new) OVER (PARTITION BY cam_id, zone_id ORDER BY ts) AS session_seq
FROM __DETECTOR___zone_tagged;

CREATE MATERIALIZED VIEW __DETECTOR___zone_session_live AS
SELECT
    cam_id,
    zone_id,
    zone_name,
    CONCAT('__DETECTOR__', '_zone_', zone_id, '_', cam_id, '_', CAST(MIN(ts) AS TEXT)) AS session_id,
    MIN(ts) AS start_time,
    MAX(ts) AS last_seen
FROM __DETECTOR___zone_islanded
GROUP BY cam_id, zone_id, zone_name, session_seq;

CREATE MATERIALIZED VIEW __DETECTOR___zone_session_ended AS
SELECT
    cam_id,
    zone_id,
    zone_name,
    session_id,
    start_time,
    last_seen AS end_time
FROM __DETECTOR___zone_session_live
WHERE last_seen < (SELECT max_ts - INTERVAL '__SESSION_GAP_SECONDS__ seconds' FROM __DETECTOR___clock);

-- ---------------------------------------------------------------------------
-- Sinks
-- ---------------------------------------------------------------------------

-- ---------------------------------------------------------------------------
-- Sink #1: detections_sink -> Postgres table `__DETECTOR___detections`
-- ---------------------------------------------------------------------------

CREATE SINK __DETECTOR___detections_sink AS
SELECT cam_id, ts, detection_count, detections
FROM __DETECTOR___detections
WHERE detection_count > 0
WITH (
    connector = 'postgres',
    host = '__POSTGRESQL_HOST__',
    port = '__POSTGRESQL_PORT__',
    user = '__POSTGRESQL_USER__',
    password = '__POSTGRESQL_PASS__',
    database = '__POSTGRESQL_DB__',
    table = '__DETECTOR___detections',
    type = 'append-only',
    force_append_only = 'true'
);

----------------------------------------------------------------------------
-- Sink #2: session_live_sink -> Postgres table `__DETECTOR___detection_sessions_live`
-- ---------------------------------------------------------------------------

CREATE SINK __DETECTOR___session_live_sink AS
SELECT
    session_id,
    cam_id        AS camera_id,
    start_time,
    last_seen,
    running_count AS count
FROM __DETECTOR___session_live
WITH (
    connector = 'postgres',
    host = '__POSTGRESQL_HOST__',
    port = '__POSTGRESQL_PORT__',
    user = '__POSTGRESQL_USER__',
    password = '__POSTGRESQL_PASS__',
    database = '__POSTGRESQL_DB__',
    table = '__DETECTOR___detection_sessions_live',
    type = 'upsert',
    primary_key = 'session_id'
);

----------------------------------------------------------------------------
-- Sink #3: session_ended_sink -> Postgres table `__DETECTOR___detection_sessions_log`
-- ---------------------------------------------------------------------------

CREATE SINK __DETECTOR___session_ended_sink AS
SELECT
    session_id,
    cam_id     AS camera_id,
    start_time,
    end_time,
    count
FROM __DETECTOR___session_ended
WITH (
    connector = 'postgres',
    host = '__POSTGRESQL_HOST__',
    port = '__POSTGRESQL_PORT__',
    user = '__POSTGRESQL_USER__',
    password = '__POSTGRESQL_PASS__',
    database = '__POSTGRESQL_DB__',
    table = '__DETECTOR___detection_sessions_log',
    type = 'append-only',
    force_append_only = 'true'
);



----------------------------------------------------------------------------
-- Sink #4: session_live_topic_sink -> Kafka topic `sessions`
-- Sink #5: sessions_topic_sink -> Kafka topic `sessions`
--
-- Both sinks stay pointed at the single shared `sessions` topic across every
-- detector type (not `__DETECTOR__-sessions`) so a single consumer can fan
-- out STARTED/ENDED events for all detector types from one stream. The
-- literal 'human' is replaced by the templated detector name so consumers
-- can tell them apart via the `detector` field.
-- ---------------------------------------------------------------------------

-- STARTED fires exactly once per session, once it's been confirmed as a real
-- incident (its __MIN_INCIDENT_DETECTIONS__-th detection), not at the first,
-- possibly noisy, detection. confirmed_at is frozen inside session_live's
-- GROUP BY, but session_live's row keeps re-emitting on every later
-- detection in the same session (last_seen/running_count still change) --
-- a STATIC filter like `confirmed_at IS NOT NULL` passes every one of those
-- re-emissions too, sinking a duplicate STARTED per still-open session
-- (confirmed by testing: an ongoing session fired STARTED repeatedly with
-- only `IS NOT NULL`). The dynamic `< max_ts - INTERVAL` comparison against
-- the clock MV is what makes this fire exactly once: RisingWave tracks it as
-- a threshold/temporal filter and only forwards the false->true transition,
-- not later re-passes. 1 second is the practical floor here -- RisingWave's
-- interval literal in this position rejected both a MILLISECOND unit and a
-- fractional-second value ("0.3"), only whole SECOND values parsed.
--
-- Deliberately an ungrouped scalar subquery, NOT a JOIN against clock: a
-- plain JOIN between two streaming views loses RisingWave's dynamic-filter
-- once-only optimization entirely and re-emits on every update to either
-- side (confirmed empirically: switching this to a JOIN made a session
-- resend STARTED dozens of times, once per incoming detection). The
-- tradeoff is clock is a single cross-camera value, so this camera's gate
-- can occasionally be held up by a completely different camera's stream
-- stalling -- accepted as a known limitation rather than "fixed" with a join.
CREATE SINK __DETECTOR___session_live_topic_sink AS
SELECT
    'STARTED' AS kind,
    session_id,
    cam_id       AS camera_id,
    '__DETECTOR__' AS detector,
    replace(CAST(start_time AT TIME ZONE 'UTC' AS TEXT), ' ', 'T') || 'Z' AS start_time
FROM __DETECTOR___session_live
WHERE confirmed_at IS NOT NULL
  AND confirmed_at < (SELECT max_ts - INTERVAL '1 second' FROM __DETECTOR___clock)
WITH (
    connector = 'kafka',
    topic = 'sessions',
    properties.bootstrap.server = '__KAFKA_BOOTSTRAP__',
    primary_key = 'session_id'
) FORMAT PLAIN ENCODE JSON (
    force_append_only = 'true'
);

CREATE SINK __DETECTOR___sessions_topic_sink AS
SELECT
    'ENDED'   AS kind,
    session_id,
    cam_id       AS camera_id,
    '__DETECTOR__' AS detector,
    replace(CAST(start_time AT TIME ZONE 'UTC' AS TEXT), ' ', 'T') || 'Z' AS start_time,
    replace(CAST(end_time   AT TIME ZONE 'UTC' AS TEXT), ' ', 'T') || 'Z' AS end_time,
    count
FROM __DETECTOR___session_ended
WITH (
    connector = 'kafka',
    topic = 'sessions',
    properties.bootstrap.server = '__KAFKA_BOOTSTRAP__',
    primary_key = 'session_id'
) FORMAT PLAIN ENCODE JSON (
    force_append_only = 'true'
);

----------------------------------------------------------------------------
-- Sink #6: zone_entry_sink -> Kafka topic `sessions`
-- Sink #7: zone_exit_sink -> Kafka topic `sessions`
-- ---------------------------------------------------------------------------

CREATE SINK __DETECTOR___zone_entry_sink AS
SELECT
    'DANGER_ZONE_ENTRY' AS kind,
    session_id,
    cam_id         AS camera_id,
    '__DETECTOR__' AS detector,
    zone_id,
    zone_name,
    replace(CAST(start_time AT TIME ZONE 'UTC' AS TEXT), ' ', 'T') || 'Z' AS start_time
FROM __DETECTOR___zone_session_live
WITH (
    connector = 'kafka',
    topic = 'sessions',
    properties.bootstrap.server = '__KAFKA_BOOTSTRAP__',
    primary_key = 'session_id'
) FORMAT PLAIN ENCODE JSON (
    force_append_only = 'true'
);

CREATE SINK __DETECTOR___zone_exit_sink AS
SELECT
    'DANGER_ZONE_EXIT' AS kind,
    session_id,
    cam_id         AS camera_id,
    '__DETECTOR__' AS detector,
    zone_id,
    zone_name,
    replace(CAST(start_time AT TIME ZONE 'UTC' AS TEXT), ' ', 'T') || 'Z' AS start_time,
    replace(CAST(end_time   AT TIME ZONE 'UTC' AS TEXT), ' ', 'T') || 'Z' AS end_time
FROM __DETECTOR___zone_session_ended
WITH (
    connector = 'kafka',
    topic = 'sessions',
    properties.bootstrap.server = '__KAFKA_BOOTSTRAP__',
    primary_key = 'session_id'
) FORMAT PLAIN ENCODE JSON (
    force_append_only = 'true'
);
