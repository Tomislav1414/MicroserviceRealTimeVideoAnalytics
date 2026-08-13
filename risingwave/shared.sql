-- Objects shared across all detector types (not templated per-detector,
-- applied once against RisingWave before the DETECTOR_TYPES loop in init.sh).

-- ---------------------------------------------------------------------------
-- Danger zone config. Real config storage/CRUD (e.g. mirrored via Postgres
-- CDC from a future detectorsvc-owned table) isn't built yet, so for now
-- this is a plain RisingWave table you INSERT test zones into directly.
-- IF NOT EXISTS (not drop-then-recreate) so restarting risingwave-init
-- doesn't wipe out zones you've already configured.
--
-- PK is exactly (detector_type, camera_id) — RisingWave's temporal join
-- requires the join condition to match the lookup table's full distribution
-- key, so it can't be a partial key like (detector_type, camera_id) with
-- zone_id left out. To still support multiple zones per camera+detector,
-- all of a camera+detector's zones live as a JSON array in one row instead
-- of one row per zone:
--   zones = [{"zone_id": "...", "zone_name": "...", "polygon": [x1,y1,x2,y2,...]}, ...]
-- polygon coordinates are in the same pixel space as detection bounding boxes.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS detector_zones (
    detector_type VARCHAR NOT NULL,
    camera_id     VARCHAR NOT NULL,
    zones         JSONB NOT NULL,
    PRIMARY KEY (detector_type, camera_id)
);

-- ---------------------------------------------------------------------------
-- bbox_overlaps_zone: does a detection's bounding box overlap a zone polygon
-- at all (not just a single anchor point)? Covers convex and concave
-- polygons via three checks: a polygon vertex inside the bbox, a bbox corner
-- inside the polygon (ray casting), or a polygon edge crossing a bbox edge.
--
-- Must never raise: RisingWave nulls out the *entire row* of a materialized
-- view if a UDF throws inside it, so malformed input (bad polygon, wrong
-- array length) fails closed (returns false) instead.
--
-- Embedded Python UDFs may be disabled by default on your RisingWave
-- version/deployment (v2.1.5+/2.2.4+/2.3.0+) — check the node config if
-- CREATE FUNCTION below fails.
--
-- CASCADE: after the first successful run, every detector's zone_hits
-- materialized view (pipeline.sql) depends on this function, so a plain
-- DROP fails on re-runs with "function used by N other objects". CASCADE
-- drops those dependents too; harmless because pipeline.sql unconditionally
-- recreates its entire zone_* chain immediately after shared.sql runs.
-- ---------------------------------------------------------------------------
DROP FUNCTION IF EXISTS bbox_overlaps_zone(
    DOUBLE PRECISION, DOUBLE PRECISION, DOUBLE PRECISION, DOUBLE PRECISION, DOUBLE PRECISION[]
) CASCADE;
CREATE FUNCTION bbox_overlaps_zone(
    x1 DOUBLE PRECISION, y1 DOUBLE PRECISION,
    x2 DOUBLE PRECISION, y2 DOUBLE PRECISION,
    zone_points DOUBLE PRECISION[]
) RETURNS BOOLEAN LANGUAGE python AS $$
def bbox_overlaps_zone(x1, y1, x2, y2, zone_points):
    try:
        pts = list(zip(zone_points[0::2], zone_points[1::2]))
        if len(pts) < 3:
            return False

        def point_in_poly(px, py):
            inside = False
            n = len(pts)
            for i in range(n):
                ax, ay = pts[i]
                bx, by = pts[(i + 1) % n]
                if ((ay > py) != (by > py)) and \
                   (px < (bx - ax) * (py - ay) / (by - ay) + ax):
                    inside = not inside
            return inside

        def segments_intersect(p1, p2, p3, p4):
            def ccw(a, b, c):
                return (c[1]-a[1])*(b[0]-a[0]) > (b[1]-a[1])*(c[0]-a[0])
            return ccw(p1,p3,p4) != ccw(p2,p3,p4) and ccw(p1,p2,p3) != ccw(p1,p2,p4)

        # 1. any polygon vertex inside the bbox?
        for px, py in pts:
            if x1 <= px <= x2 and y1 <= py <= y2:
                return True

        # 2. any bbox corner inside the polygon?
        corners = [(x1,y1), (x2,y1), (x2,y2), (x1,y2)]
        for cx, cy in corners:
            if point_in_poly(cx, cy):
                return True

        # 3. any polygon edge crosses any bbox edge?
        bbox_edges = [(corners[i], corners[(i+1) % 4]) for i in range(4)]
        n = len(pts)
        for i in range(n):
            pe = (pts[i], pts[(i+1) % n])
            for be in bbox_edges:
                if segments_intersect(pe[0], pe[1], be[0], be[1]):
                    return True

        return False
    except Exception:
        return False
$$;
