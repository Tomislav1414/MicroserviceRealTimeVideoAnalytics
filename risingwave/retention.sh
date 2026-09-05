#!/bin/sh
# Runs forever: every INTERVAL_SECONDS, deletes rows older than
# RETENTION_MINUTES from each detector type's raw detections and
# session-log tables, so a long-running demo doesn't grow Postgres
# unbounded. Only touches the two tables schema.sql creates per detector
# (<type>_detections, <type>_detection_sessions_log) -- <type>_detection_sessions_live
# is RisingWave-managed upsert state for currently-open sessions and is left
# alone.
set -eu

PG="${POSTGRES_DSN:-postgresql://vms:vms@postgres:5432/vms}"
DETECTOR_TYPES="${DETECTOR_TYPES:-human,car}"
RETENTION_MINUTES="${RETENTION_MINUTES:-15}"
INTERVAL_SECONDS="${INTERVAL_SECONDS:-60}"

echo "Waiting for Postgres ..."
until psql "$PG" -c 'SELECT 1' >/dev/null 2>&1; do sleep 2; done

echo "Retention loop started: detector_types=${DETECTOR_TYPES} retention=${RETENTION_MINUTES}m interval=${INTERVAL_SECONDS}s"

while true; do
    OLD_IFS="$IFS"
    IFS=','
    for DETECTOR in $DETECTOR_TYPES; do
        IFS="$OLD_IFS"
        DELETED_DET=$(psql "$PG" -v ON_ERROR_STOP=1 -t -c \
            "DELETE FROM ${DETECTOR}_detections WHERE ts < NOW() - INTERVAL '${RETENTION_MINUTES} minutes'" 2>&1)
        DELETED_LOG=$(psql "$PG" -v ON_ERROR_STOP=1 -t -c \
            "DELETE FROM ${DETECTOR}_detection_sessions_log WHERE start_time < NOW() - INTERVAL '${RETENTION_MINUTES} minutes'" 2>&1)
        echo "[$(date -u +%FT%TZ)] ${DETECTOR}: ${DELETED_DET# } / ${DELETED_LOG# }"
        IFS=','
    done
    IFS="$OLD_IFS"
    sleep "$INTERVAL_SECONDS"
done
