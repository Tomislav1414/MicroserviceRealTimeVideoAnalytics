#!/bin/sh
# One-shot init for the RisingWave sessionizer.
#   1. waits for Postgres and RisingWave to accept connections
#   2. applies shared.sql once (detector_zones table, bbox_overlaps_zone UDF)
#   3. for each detector type in DETECTOR_TYPES, templates schema.sql and
#      pipeline.sql with that type's name and registers them
#
# SESSION_GAP_SECONDS, CONFIDENCE_THRESHOLD, and MIN_INCIDENT_DETECTIONS are
# per-detector tunable: <VAR>_<DETECTOR_UPPER> overrides <VAR> for that
# detector type only, e.g. CONFIDENCE_THRESHOLD_POSE=0.8 with DETECTOR_TYPES
# containing "pose". Falls back to the plain <VAR> (or its own default) when
# no override is set, so a single-detector setup needs no per-type config.
# STATE_RETENTION stays global across all detector types.
#
# Idempotent: schema.sql uses IF NOT EXISTS; pipeline.sql drops-then-recreates,
# so `docker compose restart risingwave-init` re-applies edits to either file.
set -e

PG="${POSTGRES_DSN:-postgresql://detector:detector@postgres:5432/detections}"
RW="${RISINGWAVE_DSN:-postgresql://root@risingwave:4566/dev}"
GAP_DEFAULT="${SESSION_GAP_SECONDS:-10}"
CONF_DEFAULT="${CONFIDENCE_THRESHOLD:-0.6}"
RETENTION="${STATE_RETENTION:-2 hours}"
MIN_DETECTIONS_DEFAULT="${MIN_INCIDENT_DETECTIONS:-3}"
DETECTOR_TYPES="${DETECTOR_TYPES:-human}"
KAFKA_BS="${KAFKA_BOOTSTRAP:-kafka:29092}"
# Discrete pieces for the Postgres sinks in pipeline.sql (CREATE SINK ... WITH host=/user=/...
# can't take a single DSN). Defaults match POSTGRES_DSN's default above, so docker-compose
# behavior is unchanged; k8s sets these explicitly (host won't resolve as "postgres" there,
# and the password is a generated secret, not the literal string "detector").
PGHOST="${POSTGRESQL_HOST:-postgres}"
PGPORT="${POSTGRESQL_PORT:-5432}"
PGUSER="${POSTGRESQL_USER:-detector}"
PGPASS="${POSTGRESQL_PASS:-detector}"
PGDB="${POSTGRESQL_DB:-detections}"

echo "Waiting for Postgres ..."
until psql "$PG" -c 'SELECT 1' >/dev/null 2>&1; do sleep 2; done

echo "Waiting for RisingWave ..."
until psql "$RW" -c 'SELECT 1' >/dev/null 2>&1; do sleep 2; done

echo "Applying shared RisingWave objects (detector_zones table, bbox_overlaps_zone UDF) ..."
psql "$RW" -v ON_ERROR_STOP=1 -f /shared.sql

OLD_IFS="$IFS"
IFS=','
for DETECTOR in $DETECTOR_TYPES; do
    IFS="$OLD_IFS"

    UPPER=$(printf '%s' "$DETECTOR" | tr 'a-z-' 'A-Z_')
    eval "GAP=\${SESSION_GAP_SECONDS_${UPPER}:-\$GAP_DEFAULT}"
    eval "CONF=\${CONFIDENCE_THRESHOLD_${UPPER}:-\$CONF_DEFAULT}"
    eval "MIN_DETECTIONS=\${MIN_INCIDENT_DETECTIONS_${UPPER}:-\$MIN_DETECTIONS_DEFAULT}"

    echo "Applying Postgres sink schema for detector=${DETECTOR} (gap=${GAP}s) ..."
    sed -e "s/__DETECTOR__/${DETECTOR}/g" \
        -e "s/__SESSION_GAP_SECONDS__/${GAP}/g" \
        /schema.sql > "/tmp/schema.${DETECTOR}.sql"
    psql "$PG" -v ON_ERROR_STOP=1 -f "/tmp/schema.${DETECTOR}.sql"

    echo "Registering RisingWave pipeline for detector=${DETECTOR} (gap=${GAP}s, confidence>=${CONF}, retention=${RETENTION}, min_detections=${MIN_DETECTIONS}, kafka=${KAFKA_BS}, pg=${PGUSER}@${PGHOST}:${PGPORT}/${PGDB}) ..."
    sed -e "s/__DETECTOR__/${DETECTOR}/g" \
        -e "s/__SESSION_GAP_SECONDS__/${GAP}/g" \
        -e "s/__CONFIDENCE_THRESHOLD__/${CONF}/g" \
        -e "s/__STATE_RETENTION__/${RETENTION}/g" \
        -e "s/__MIN_INCIDENT_DETECTIONS__/${MIN_DETECTIONS}/g" \
        -e "s/__KAFKA_BOOTSTRAP__/${KAFKA_BS}/g" \
        -e "s/__POSTGRESQL_HOST__/${PGHOST}/g" \
        -e "s/__POSTGRESQL_PORT__/${PGPORT}/g" \
        -e "s/__POSTGRESQL_USER__/${PGUSER}/g" \
        -e "s/__POSTGRESQL_PASS__/${PGPASS}/g" \
        -e "s/__POSTGRESQL_DB__/${PGDB}/g" \
        /pipeline.sql > "/tmp/pipeline.${DETECTOR}.sql"
    psql "$RW" -v ON_ERROR_STOP=1 -f "/tmp/pipeline.${DETECTOR}.sql"

    IFS=','
done
IFS="$OLD_IFS"

echo "Pipeline registered for detector types: ${DETECTOR_TYPES}."
