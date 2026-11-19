-- The scan dashboard's views, over the tables of auto_scan's scan.db (auto_scan/db.py).
-- plugins/scan_upload.py runs this after every upload, in the upload's transaction, and at
-- startup. Statements are separated by ";" at the end of a line. stdev() is an aggregate
-- the plugin registers on every connection (sample standard deviation).
--
-- Metric names are owned by auto_scan's extractors (src/auto_scan/extract/*.py, each
-- documented in its module docstring). The generic views (metric_values, metric_latest)
-- show any metric; the pivots below name specific ones and show NULL for a name not
-- (yet) recorded, so a renamed metric empties a column rather than breaking the view.

DROP VIEW IF EXISTS findings_open;
DROP VIEW IF EXISTS findings_recent;
DROP VIEW IF EXISTS evaluation_history;
DROP VIEW IF EXISTS run_status;
DROP VIEW IF EXISTS metric_values;
DROP VIEW IF EXISTS metric_latest;
DROP VIEW IF EXISTS residuals_by_event;
DROP VIEW IF EXISTS event_terms_by_resolution;
DROP VIEW IF EXISTS station_bias;
DROP VIEW IF EXISTS event_summary;
DROP VIEW IF EXISTS perf_by_run;
DROP VIEW IF EXISTS timers_top;
DROP VIEW IF EXISTS job_failures;
DROP VIEW IF EXISTS science_event_terms;
DROP VIEW IF EXISTS xres_by_event;

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

-- Every evaluation, with its event's time: a check's history, passing or failing.
CREATE VIEW evaluation_history AS
SELECT v.check_id, v.subject, v.event_id, e.cycle, e.origin_time, e.magnitude, v.resolution,
       v.failing, v.value, v.threshold, v.summary, v.evaluated_at
FROM evaluations v LEFT JOIN events e USING (event_id);

-- One row a run, with its event.
CREATE VIEW run_status AS
SELECT r.event_id, e.cycle, e.origin_time, e.magnitude, e.location, r.resolution, r.status, r.written,
       r.defaults_version, r.scanned_at,
       round((julianday(r.written) - julianday(e.origin_time)) * 24, 2) AS hours_to_results
FROM runs r LEFT JOIN events e USING (event_id);

-- Any metric's history: one row a value, with its event's time.
CREATE VIEW metric_values AS
SELECT m.metric, m.resolution, m.event_id, e.cycle, e.origin_time, e.magnitude, m.value, m.unit, m.detail
FROM metrics m LEFT JOIN events e USING (event_id);

-- Each metric's value on the latest event that has it, per resolution, with the count of events.
CREATE VIEW metric_latest AS
SELECT metric, resolution, event_id, origin_time, value, unit, n_events
FROM (
    SELECT m.metric, m.resolution, m.event_id, e.origin_time, m.value, m.unit,
           count(*) OVER (PARTITION BY m.metric, m.resolution) AS n_events,
           row_number() OVER (PARTITION BY m.metric, m.resolution ORDER BY e.origin_time DESC) AS k
    FROM metrics m LEFT JOIN events e USING (event_id)
    WHERE m.value IS NOT NULL
)
WHERE k = 1;

-- Per event, resolution and IM: the event term (mean ln(obs/sim) over the stations whose
-- record passed the quality filter) and the within-event spread.
CREATE VIEW residuals_by_event AS
SELECT s.event_id, e.cycle, e.origin_time, e.magnitude, s.resolution, s.im,
       count(*) AS n_stations,
       round(avg(s.residual), 4) AS event_term,
       round(stdev(s.residual), 4) AS within_sigma
FROM (
    SELECT * FROM station_residuals WHERE coalesce(quality_ok, 1) = 1 AND residual IS NOT NULL
) s
LEFT JOIN events e USING (event_id)
GROUP BY s.event_id, s.resolution, s.im;

-- The event term per resolution side by side: the resolutions should agree at the
-- periods every one of them resolves (mismatch_* is finer minus 0.25 Hz).
CREATE VIEW event_terms_by_resolution AS
SELECT event_id, cycle, origin_time, magnitude, im,
       max(CASE WHEN resolution = '0_25hz' THEN event_term END) AS event_term_0_25hz,
       max(CASE WHEN resolution = '0_5hz' THEN event_term END) AS event_term_0_5hz,
       max(CASE WHEN resolution = '1hz' THEN event_term END) AS event_term_1hz,
       round(max(CASE WHEN resolution = '0_5hz' THEN event_term END)
             - max(CASE WHEN resolution = '0_25hz' THEN event_term END), 4) AS mismatch_0_5hz,
       round(max(CASE WHEN resolution = '1hz' THEN event_term END)
             - max(CASE WHEN resolution = '0_25hz' THEN event_term END), 4) AS mismatch_1hz
FROM residuals_by_event
GROUP BY event_id, im;

-- Per station, resolution and IM: the mean residual across events after removing each
-- event's term (a site term), its spread and the number of events.
CREATE VIEW station_bias AS
SELECT station, resolution, im,
       count(DISTINCT event_id) AS n_events,
       round(avg(residual - event_term), 4) AS site_term,
       round(stdev(residual - event_term), 4) AS site_sigma,
       round(avg(residual), 4) AS mean_residual,
       round(avg(rrup_km), 1) AS mean_rrup_km,
       max(vs30) AS vs30
FROM (
    SELECT station, resolution, im, event_id, residual, rrup_km, vs30,
           avg(residual) OVER (PARTITION BY event_id, resolution, im) AS event_term
    FROM station_residuals
    WHERE coalesce(quality_ok, 1) = 1 AND residual IS NOT NULL
)
GROUP BY station, resolution, im;

-- Per run and IM, the science extractor's event terms (science.residual.<stat>.<IM>):
-- ln(observed / simulated) over quality-ok real stations, and the same against the
-- empirical model (science.gmm_residual.median.<IM>, ln(gmm / simulated)).
CREATE VIEW science_event_terms AS
SELECT t.event_id, e.cycle, e.origin_time, e.magnitude, t.resolution, t.im,
       round(t.value, 4) AS event_term,
       round((SELECT value FROM metrics x WHERE x.event_id = t.event_id AND x.resolution = t.resolution
              AND x.metric = 'science.residual.mean.' || t.im), 4) AS mean,
       round((SELECT value FROM metrics x WHERE x.event_id = t.event_id AND x.resolution = t.resolution
              AND x.metric = 'science.residual.mad_sigma.' || t.im), 4) AS mad_sigma,
       (SELECT value FROM metrics x WHERE x.event_id = t.event_id AND x.resolution = t.resolution
        AND x.metric = 'science.residual.n.' || t.im) AS n_stations,
       round((SELECT value FROM metrics x WHERE x.event_id = t.event_id AND x.resolution = t.resolution
              AND x.metric = 'science.gmm_residual.median.' || t.im), 4) AS gmm_event_term
FROM (
    SELECT event_id, resolution, value, substr(metric, length('science.residual.median.') + 1) AS im
    FROM metrics WHERE metric LIKE 'science.residual.median.%'
) t
LEFT JOIN events e USING (event_id);

-- Per event and resolution pair (<coarse>-<fine>), the long-period mismatch between the
-- two runs at the sites both hold: the median over IMs of median ln(IM_fine / IM_coarse).
CREATE VIEW xres_by_event AS
SELECT m.event_id, e.cycle, e.origin_time, e.magnitude,
       substr(m.metric, length('science.xres.') + 1, length(m.metric) - length('science.xres.') - length('.median'))
           AS pair,
       round(m.value, 4) AS median_mismatch,
       round((SELECT x.value FROM metrics x WHERE x.event_id = m.event_id AND x.resolution = m.resolution
              AND x.metric = substr(m.metric, 1, length(m.metric) - length('.median')) || '.mad_sigma'), 4)
           AS mad_sigma,
       (SELECT x.value FROM metrics x WHERE x.event_id = m.event_id AND x.resolution = m.resolution
        AND x.metric = substr(m.metric, 1, length(m.metric) - length('.median')) || '.n') AS n_sites
FROM metrics m LEFT JOIN events e USING (event_id)
WHERE m.metric LIKE 'science.xres.%.median' AND m.resolution = '';

-- One row an event: each resolution's status, how its checks went, and headline numbers.
CREATE VIEW event_summary AS
SELECT e.event_id, e.cycle, e.origin_time, e.magnitude, e.location, e.depth_km,
       (SELECT status FROM runs r WHERE r.event_id = e.event_id AND r.resolution = '0_25hz') AS status_0_25hz,
       (SELECT status FROM runs r WHERE r.event_id = e.event_id AND r.resolution = '0_5hz') AS status_0_5hz,
       (SELECT status FROM runs r WHERE r.event_id = e.event_id AND r.resolution = '1hz') AS status_1hz,
       (SELECT count(*) FROM evaluations v WHERE v.event_id = e.event_id AND v.failing) AS n_failing_checks,
       (SELECT count(*) FROM evaluations v WHERE v.event_id = e.event_id) AS n_checks,
       (SELECT count(*) FROM findings f WHERE f.first_event = e.event_id) AS n_findings_opened,
       (SELECT count(*) FROM findings f WHERE f.last_event = e.event_id) AS n_findings_failing,
       (SELECT round(value, 3) FROM metrics m WHERE m.event_id = e.event_id AND m.resolution = '1hz'
        AND m.metric = 'science.residual.median.PGV') AS event_term_pgv_1hz,
       (SELECT round(value, 3) FROM metrics m WHERE m.event_id = e.event_id AND m.resolution = '1hz'
        AND m.metric = 'science.residual.band_median.lf') AS event_term_lf_1hz,
       (SELECT round(value, 3) FROM metrics m WHERE m.event_id = e.event_id AND m.resolution = '1hz'
        AND m.metric = 'science.residual.band_median.hf') AS event_term_hf_1hz,
       (SELECT value FROM metrics m WHERE m.event_id = e.event_id AND m.resolution = '1hz'
        AND m.metric = 'science.residual.n_stations') AS n_stations_1hz,
       (SELECT round(value, 3) FROM metrics m WHERE m.event_id = e.event_id AND m.resolution = ''
        AND m.metric = 'science.xres.0_25hz-1hz.median') AS xres_0_25hz_1hz,
       (SELECT round(value / 3600.0, 2) FROM metrics m WHERE m.event_id = e.event_id AND m.resolution = '1hz'
        AND m.metric = 'ops.time_to_results_from_origin_s') AS hours_to_results_1hz,
       (SELECT round(value, 1) FROM metrics m WHERE m.event_id = e.event_id AND m.resolution = ''
        AND m.metric = 'ops.core_hours') AS core_hours,
       (SELECT value FROM metrics m WHERE m.event_id = e.event_id AND m.resolution = ''
        AND m.metric = 'ops.jobs_failed') AS jobs_failed
FROM events e;

-- One row a run: what SW4 cost, against its estimate, and how fast it went (perf.* and
-- ops.* metrics pivoted; see auto_scan/extract/perf.py for each).
CREATE VIEW perf_by_run AS
SELECT r.event_id, e.cycle, e.origin_time, e.magnitude, r.resolution, r.status, r.written,
       max(CASE WHEN m.metric = 'perf.cores' THEN m.value END) AS cores,
       max(CASE WHEN m.metric = 'perf.nodes' THEN m.value END) AS nodes,
       max(CASE WHEN m.metric = 'perf.points' THEN m.value END) AS points,
       max(CASE WHEN m.metric = 'perf.steps' THEN m.value END) AS steps,
       max(CASE WHEN m.metric = 'perf.completed' THEN m.value END) AS completed,
       max(CASE WHEN m.metric = 'perf.updates_per_core_s' THEN m.value END) AS updates_per_core_s,
       max(CASE WHEN m.metric = 'perf.timestep_wall_s' THEN m.value END) AS timestep_wall_s,
       max(CASE WHEN m.metric = 'perf.estimated_core_hours_effective' THEN m.value END) AS estimated_core_hours,
       max(CASE WHEN m.metric = 'perf.central_core_hours' THEN m.value END) AS central_core_hours,
       max(CASE WHEN m.metric = 'perf.actual_core_hours' THEN m.value END) AS actual_core_hours,
       max(CASE WHEN m.metric = 'perf.job.core_hours' THEN m.value END) AS job_core_hours,
       max(CASE WHEN m.metric = 'perf.budget_core_hours' THEN m.value END) AS budget_core_hours,
       max(CASE WHEN m.metric = 'perf.cost_ratio' THEN m.value END) AS cost_ratio,
       max(CASE WHEN m.metric = 'perf.central_cost_ratio' THEN m.value END) AS central_cost_ratio,
       max(CASE WHEN m.metric = 'perf.domain_fraction' THEN m.value END) AS domain_fraction,
       max(CASE WHEN m.metric = 'perf.mpi_fraction' THEN m.value END) AS mpi_fraction,
       max(CASE WHEN m.metric = 'perf.comm_wait_fraction' THEN m.value END) AS comm_wait_fraction,
       max(CASE WHEN m.metric = 'perf.rank_imbalance' THEN m.value END) AS rank_imbalance,
       max(CASE WHEN m.metric = 'perf.io_share' THEN m.value END) AS io_share,
       max(CASE WHEN m.metric = 'perf.memory.peak_fraction' THEN m.value END) AS memory_peak_fraction,
       max(CASE WHEN m.metric = 'perf.job.queue_s' THEN m.value END) AS queue_s,
       max(CASE WHEN m.metric = 'perf.job.run_s' THEN m.value END) AS run_s,
       max(CASE WHEN m.metric = 'perf.job.time_limit_fraction' THEN m.value END) AS time_limit_fraction,
       max(CASE WHEN m.metric = 'perf.job.contention_ratio' THEN m.value END) AS contention_ratio,
       round(max(CASE WHEN m.metric = 'ops.time_to_results_from_origin_s' THEN m.value END) / 3600.0, 2)
           AS hours_to_results,
       round(max(CASE WHEN m.metric = 'ops.time_to_results_s' THEN m.value END) / 3600.0, 2)
           AS hours_to_results_from_submit
FROM runs r
LEFT JOIN events e USING (event_id)
LEFT JOIN metrics m ON m.event_id = r.event_id AND m.resolution = r.resolution
GROUP BY r.event_id, r.resolution;

-- Each run's SW4 timer regions, slowest first (rank 1 is the run's largest max_s).
CREATE VIEW timers_top AS
SELECT t.event_id, e.origin_time, t.resolution, t.region, t.depth, t.calls_total, t.ranks,
       t.min_s, t.avg_s, t.max_s, t.wall_avg_s, t.mpi_avg_s, t.mpi_max_s, t.omp_wait_frac, t.omp_imb_max,
       round(t.max_s / nullif(t.avg_s, 0), 3) AS imbalance,
       rank() OVER (PARTITION BY t.event_id, t.resolution ORDER BY t.max_s DESC) AS rank
FROM sw4_timers t LEFT JOIN events e USING (event_id);

-- Failed jobs of the scanned workflow, classed by the tail of job.err (the Cylc monitor
-- keeps the last 20 lines of a failed job's job.err).
CREATE VIEW job_failures AS
SELECT j.cycle, j.name, j.submit_num, j.state, j.time_run_exit, j.job_err,
       CASE
           WHEN j.state = 'submit-failed' THEN 'submit failed'
           WHEN j.job_err LIKE '%DUE TO TIME LIMIT%' OR j.job_err LIKE '%TIMEOUT%' THEN 'time limit'
           WHEN j.job_err LIKE '%oom%kill%' OR j.job_err LIKE '%OUT_OF_MEMORY%' OR j.job_err LIKE '%Out Of Memory%'
               THEN 'out of memory'
           WHEN j.job_err LIKE '%jobs-kill%' OR j.job_err LIKE '%CANCELLED%' THEN 'killed or cancelled'
           WHEN j.job_err LIKE '%cloudflare%' OR j.job_err LIKE '%error code: 1010%' OR j.job_err LIKE '%HTTP Error%'
               THEN 'HTTP'
           WHEN j.job_err LIKE '%rclone%' OR j.job_err LIKE '%dropbox%' THEN 'upload'
           WHEN j.job_err LIKE '%Traceback%' THEN 'Python exception'
           WHEN j.job_err LIKE '%MPI_ABORT%' OR j.job_err LIKE '%Segmentation fault%' OR j.job_err LIKE '%signal 11%'
               THEN 'crash'
           WHEN j.job_err IS NULL OR j.job_err = '' THEN 'no job.err'
           ELSE 'other'
       END AS failure_class
FROM jobs j
WHERE j.state IN ('failed', 'submit-failed');
