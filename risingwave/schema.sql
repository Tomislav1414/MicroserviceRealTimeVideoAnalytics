-- Templated per detector type: __DETECTOR__ is substituted (by init.sh) with
-- a detector name (e.g. "human", "vehicle"), giving each detector type its
-- own Postgres tables/views. Instantiate once per entry in DETECTOR_TYPES.

CREATE TABLE IF NOT EXISTS __DETECTOR___detections (
    cam_id          TEXT        NOT NULL,
    ts              TIMESTAMPTZ NOT NULL,
    detection_count INT         NOT NULL,
    detections      JSONB
);
CREATE INDEX IF NOT EXISTS __DETECTOR___detections_cam_ts
    ON __DETECTOR___detections (cam_id, ts);

CREATE TABLE IF NOT EXISTS __DETECTOR___detection_sessions_log (
    session_id  TEXT        NOT NULL,
    camera_id   TEXT        NOT NULL,
    start_time  TIMESTAMPTZ NOT NULL,
    end_time    TIMESTAMPTZ,
    count       BIGINT      NOT NULL DEFAULT 0
);
CREATE INDEX IF NOT EXISTS __DETECTOR___detection_sessions_log_session ON __DETECTOR___detection_sessions_log (session_id);
CREATE INDEX IF NOT EXISTS __DETECTOR___detection_sessions_log_cam_start ON __DETECTOR___detection_sessions_log (camera_id, start_time DESC);


CREATE TABLE IF NOT EXISTS __DETECTOR___detection_sessions_live (
    session_id  TEXT        PRIMARY KEY,
    camera_id   TEXT        NOT NULL,
    start_time  TIMESTAMPTZ NOT NULL,
    last_seen   TIMESTAMPTZ,
    count       BIGINT      NOT NULL DEFAULT 0
);
CREATE INDEX IF NOT EXISTS __DETECTOR___detection_sessions_live_cam ON __DETECTOR___detection_sessions_live (camera_id, last_seen DESC);

CREATE OR REPLACE VIEW __DETECTOR___detection_sessions AS
SELECT DISTINCT ON (session_id)
    session_id, camera_id, start_time, end_time, count
FROM __DETECTOR___detection_sessions_log
ORDER BY session_id, end_time DESC, count DESC;

CREATE OR REPLACE VIEW __DETECTOR___active_sessions AS
SELECT session_id, camera_id, start_time, last_seen, count
FROM __DETECTOR___detection_sessions_live
WHERE last_seen > NOW() - INTERVAL '__SESSION_GAP_SECONDS__ seconds';
