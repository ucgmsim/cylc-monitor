-- The CMT dashboard's views, over the tables of auto_scan's cmt.db (src/auto_scan/cmt/).
-- plugins/scan_upload.py runs this after every upload to /-/scan/upload/cmt, in the
-- upload's transaction, and at startup. Statements are separated by ";" at the end of a
-- line.
--
-- In cmt.db a run is one of the three CMT runs of an event (runs.resolution is its name:
-- all_stations, azimuth, drop_outliers_dev) and runs.written is when its cmt_solution.csv
-- was written. Metric names are documented in auto_scan's src/auto_scan/cmt/extract.py; a pivot shows
-- NULL for a name not (yet) recorded, so a renamed metric empties a column rather than
-- breaking the view.

DROP VIEW IF EXISTS findings_open;
DROP VIEW IF EXISTS findings_recent;
DROP VIEW IF EXISTS evaluation_history;
DROP VIEW IF EXISTS metric_values;
DROP VIEW IF EXISTS metric_latest;
DROP VIEW IF EXISTS cmt_by_run;
DROP VIEW IF EXISTS cmt_event_summary;
DROP VIEW IF EXISTS cmt_timings;

-- Findings not yet closed (verified or dismissed), most severe first.
CREATE VIEW findings_open AS
SELECT f.id, f.severity, f.kind, f.status, f.check_id, f.subject, f.title, f.summary,
       f.first_event, f.last_event, f.n_failing, f.opened_at, f.updated_at, f.issue_url, f.issue_repo,
       CASE f.severity WHEN 'critical' THEN 0 WHEN 'high' THEN 1 WHEN 'medium' THEN 2 ELSE 3 END AS severity_rank
FROM findings f
WHERE f.status NOT IN ('verified', 'dismissed');

-- Every finding with its whole life: when opened, filed, closed and verified.
CREATE VIEW findings_recent AS
SELECT f.id, f.severity, f.kind, f.status, f.check_id, f.subject, f.title, f.first_event, f.last_event,
       f.n_failing, f.opened_at, f.updated_at, f.issue_url, f.issue_repo, f.issue_closed_at,
       f.verified_event, f.verified_at
FROM findings f;

-- Every evaluation, with its event: a check's history, passing or failing. `run` is the CMT
-- run, or '' for a check of the whole event.
CREATE VIEW evaluation_history AS
SELECT v.check_id, v.subject, v.event_id, e.origin_time, e.magnitude, v.resolution AS run,
       v.failing, v.value, v.threshold, v.summary, v.evaluated_at
FROM evaluations v LEFT JOIN events e USING (event_id);

-- Any metric's history: one row a value, with its event's time.
CREATE VIEW metric_values AS
SELECT m.metric, m.resolution AS run, m.event_id, e.origin_time, e.magnitude, m.value, m.unit, m.detail
FROM metrics m LEFT JOIN events e USING (event_id);

-- Each metric's value on the latest event that has it, per run, with the count of events.
CREATE VIEW metric_latest AS
SELECT metric, run, event_id, origin_time, value, unit, n_events
FROM (
    SELECT m.metric, m.resolution AS run, m.event_id, e.origin_time, m.value, m.unit,
           count(*) OVER (PARTITION BY m.metric, m.resolution) AS n_events,
           row_number() OVER (PARTITION BY m.metric, m.resolution ORDER BY e.origin_time DESC) AS k
    FROM metrics m LEFT JOIN events e USING (event_id)
    WHERE m.value IS NOT NULL
)
WHERE k = 1;

-- One row a CMT run: its status, fit, mechanism, centroid, posterior and time.
CREATE VIEW cmt_by_run AS
SELECT r.event_id, e.origin_time, e.magnitude, e.location, r.resolution AS run, r.status, r.written,
       max(CASE WHEN m.metric = 'cmt.selected' THEN m.value END) AS selected,
       max(CASE WHEN m.metric = 'cmt.deviatoric' THEN m.value END) AS deviatoric,
       max(CASE WHEN m.metric = 'cmt.n_stations' THEN m.value END) AS n_stations,
       round(max(CASE WHEN m.metric = 'cmt.vr' THEN m.value END), 3) AS vr,
       round(max(CASE WHEN m.metric = 'cmt.mw' THEN m.value END), 2) AS mw,
       round(max(CASE WHEN m.metric = 'cmt.dc_percent' THEN m.value END), 1) AS dc_percent,
       round(max(CASE WHEN m.metric = 'cmt.clvd_percent' THEN m.value END), 1) AS clvd_percent,
       round(max(CASE WHEN m.metric = 'cmt.iso_percent' THEN m.value END), 1) AS iso_percent,
       max(CASE WHEN m.metric = 'cmt.centroid_depth_km' THEN m.value END) AS centroid_depth_km,
       max(CASE WHEN m.metric = 'cmt.on_grid_edge' THEN m.value END) AS on_grid_edge,
       round(max(CASE WHEN m.metric = 'cmt.posterior.boundary_probability' THEN m.value END), 3)
           AS posterior_boundary_probability,
       round(max(CASE WHEN m.metric = 'cmt.posterior.effective_cells' THEN m.value END), 2) AS posterior_effective_cells,
       round(max(CASE WHEN m.metric = 'cmt.reduced_chi_square' THEN m.value END), 2) AS reduced_chi_square,
       max(CASE WHEN m.metric = 'cmt.posterior.variance_scale' THEN m.value END) AS variance_scale,
       round(max(CASE WHEN m.metric = 'cmt.kagan_to_selected_deg' THEN m.value END), 1) AS kagan_to_selected_deg,
       round(max(CASE WHEN m.metric = 'cmt.jackknife.max_kagan_deg' THEN m.value END), 1) AS jackknife_max_kagan_deg,
       round(max(CASE WHEN m.metric = 'cmt.station.negative_vr_fraction' THEN m.value END), 3)
           AS negative_vr_fraction,
       max(CASE WHEN m.metric = 'cmt.mouse_components' THEN m.value END) AS mouse_components,
       round(max(CASE WHEN m.metric = 'cmt.timing.total_s' THEN m.value END), 1) AS total_s,
       round(max(CASE WHEN m.metric = 'cmt.timing.greens_functions_s' THEN m.value END), 1) AS greens_functions_s,
       round(max(CASE WHEN m.metric = 'cmt.timing.other_s' THEN m.value END), 1) AS other_s
FROM runs r
LEFT JOIN events e USING (event_id)
LEFT JOIN metrics m ON m.event_id = r.event_id AND m.resolution = r.resolution
GROUP BY r.event_id, r.resolution;

-- One row an event: the main solution against GeoNet's, how its checks went, and its time.
CREATE VIEW cmt_event_summary AS
SELECT e.event_id, e.origin_time, e.magnitude AS mw_posted, e.location, e.depth_km,
       (SELECT resolution FROM metrics m WHERE m.event_id = e.event_id AND m.metric = 'cmt.selected' AND m.value = 1)
           AS selected_run,
       (SELECT round(value, 2) FROM metrics m WHERE m.event_id = e.event_id AND m.resolution = '' AND m.metric = 'cmt.mw')
           AS mw,
       (SELECT round(value, 2) FROM metrics m WHERE m.event_id = e.event_id AND m.resolution = ''
        AND m.metric = 'cmt.geonet_magnitude') AS geonet_magnitude,
       (SELECT round(value, 2) FROM metrics m WHERE m.event_id = e.event_id AND m.resolution = ''
        AND m.metric = 'cmt.mw_minus_geonet') AS mw_minus_geonet,
       (SELECT value FROM metrics m WHERE m.event_id = e.event_id AND m.resolution = '' AND m.metric = 'cmt.n_runs_ok')
           AS n_runs_ok,
       (SELECT value FROM metrics m WHERE m.event_id = e.event_id AND m.resolution = '' AND m.metric = 'cmt.n_runs')
           AS n_runs,
       (SELECT round(m.value, 3) FROM metrics m JOIN metrics s ON s.event_id = m.event_id
        AND s.resolution = m.resolution AND s.metric = 'cmt.selected' AND s.value = 1
        WHERE m.event_id = e.event_id AND m.metric = 'cmt.vr') AS vr,
       (SELECT m.value FROM metrics m JOIN metrics s ON s.event_id = m.event_id
        AND s.resolution = m.resolution AND s.metric = 'cmt.selected' AND s.value = 1
        WHERE m.event_id = e.event_id AND m.metric = 'cmt.n_stations') AS n_stations,
       (SELECT round(value / 60.0, 1) FROM metrics m WHERE m.event_id = e.event_id AND m.resolution = ''
        AND m.metric = 'cmt.time_to_solution_s') AS minutes_to_solution,
       (SELECT round(value / 60.0, 1) FROM metrics m WHERE m.event_id = e.event_id AND m.resolution = ''
        AND m.metric = 'cmt.timing.total_s') AS pipeline_minutes,
       (SELECT count(*) FROM evaluations v WHERE v.event_id = e.event_id AND v.failing) AS n_failing_checks,
       (SELECT count(*) FROM evaluations v WHERE v.event_id = e.event_id) AS n_checks,
       (SELECT min(written) FROM runs r WHERE r.event_id = e.event_id) AS computed_at
FROM events e;

-- Stage timings (timings.csv): one row an event, run ('pipeline' for the pipeline's own
-- stages) and stage, without the totals.
CREATE VIEW cmt_timings AS
SELECT m.event_id, e.origin_time, CASE m.resolution WHEN '' THEN 'pipeline' ELSE m.resolution END AS run,
       substr(m.metric, length('cmt.timing.') + 1, length(m.metric) - length('cmt.timing.') - length('_s')) AS stage,
       round(m.value, 2) AS seconds
FROM metrics m LEFT JOIN events e USING (event_id)
WHERE m.metric LIKE 'cmt.timing.%' AND m.metric != 'cmt.timing.total_s' AND m.value IS NOT NULL;
