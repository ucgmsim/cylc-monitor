-- The tables of auto_scan's scan.db (ucgmsim/auto_scan, src/auto_scan/db.py), schema version 1,
-- for tests/test_server.py to build a snapshot from.
-- One row a simulated event (a Cylc cycle).
CREATE TABLE IF NOT EXISTS events (
    event_id TEXT PRIMARY KEY,          -- GeoNet public id, e.g. 2026p866048
    cycle INTEGER,                      -- the workflow's cycle point
    event_dir TEXT NOT NULL,            -- <cycle>-<event_id> under results_url
    origin_time TEXT,                   -- ISO 8601, UTC
    magnitude REAL,                     -- the CMT's Mw, as simulated
    latitude REAL, longitude REAL, depth_km REAL,
    location TEXT                       -- "15 km south-east of Arthur's Pass"
);

-- One row a run: an event at one resolution, as its manifest.json describes it.
CREATE TABLE IF NOT EXISTS runs (
    event_id TEXT NOT NULL REFERENCES events,
    resolution TEXT NOT NULL,           -- 0_25hz, 0_5hz or 1hz
    status TEXT,                        -- the manifest's: succeeded, failed:<task>, ...
    written TEXT,                       -- when the manifest was written, ISO 8601 UTC
    defaults_version TEXT,
    manifest_sha256 TEXT,               -- of manifest.json: a changed one is rescanned
    scanned_at TEXT,                    -- when the collector last ingested it
    PRIMARY KEY (event_id, resolution)
);

-- The commit of every source a run was built from (the manifest's `sources`).
CREATE TABLE IF NOT EXISTS run_sources (
    event_id TEXT NOT NULL,
    resolution TEXT NOT NULL,
    source TEXT NOT NULL,               -- the manifest's key: workflow, sw4, qcore-utils, ...
    url TEXT, ref TEXT,
    commit_sha TEXT,                    -- a commit, or a content hash for data sources
    PRIMARY KEY (event_id, resolution, source)
);

-- Scalars about a run, in long form: what every check reads.
-- `resolution` is '' for a metric of the event as a whole.
CREATE TABLE IF NOT EXISTS metrics (
    event_id TEXT NOT NULL,
    resolution TEXT NOT NULL DEFAULT '',
    metric TEXT NOT NULL,               -- dotted name: science.residual.mean.PGA, perf.mpi_fraction
    value REAL,
    unit TEXT,
    detail TEXT,                        -- JSON, anything that explains the value
    PRIMARY KEY (event_id, resolution, metric)
);

-- Observed against simulated at a real station, one row an intensity measure.
CREATE TABLE IF NOT EXISTS station_residuals (
    event_id TEXT NOT NULL,
    resolution TEXT NOT NULL,
    station TEXT NOT NULL,
    im TEXT NOT NULL,                   -- PGA, PGV, pSA_0.5, ...
    observed REAL, simulated REAL,
    gmm REAL,                           -- an empirical model's median, when known
    residual REAL,                      -- ln(observed / simulated)
    rrup_km REAL, vs30 REAL,
    quality_ok INTEGER,                 -- 1 when the observed record passes the quality filter
    PRIMARY KEY (event_id, resolution, station, im)
);

-- SW4's timer regions (sw4_timers.csv), one row a region of a run.
CREATE TABLE IF NOT EXISTS sw4_timers (
    event_id TEXT NOT NULL,
    resolution TEXT NOT NULL,
    region TEXT NOT NULL,               -- solve/time_stepping/predictor/evalRHS, MPI_Barrier, ...
    depth INTEGER,
    calls_total INTEGER, ranks INTEGER,
    min_s REAL, avg_s REAL, max_s REAL, wall_avg_s REAL,
    mpi_min_s REAL, mpi_avg_s REAL, mpi_max_s REAL,
    omp_wait_frac REAL, omp_imb_avg REAL, omp_imb_max REAL,
    PRIMARY KEY (event_id, resolution, region)
);

-- The Cylc monitor's `jobs` view for the scanned workflow, copied whole each run.
CREATE TABLE IF NOT EXISTS jobs (
    workflow TEXT NOT NULL, cycle TEXT NOT NULL, name TEXT NOT NULL, submit_num INTEGER NOT NULL,
    try_num INTEGER, job_id TEXT, platform TEXT, runner TEXT, state TEXT,
    time_submit TEXT, time_run TEXT, time_run_exit TEXT, queue_s REAL, run_s REAL,
    cores INTEGER, nodes INTEGER, chunks INTEGER, mem_gb REAL, time_limit_min REAL,
    account TEXT, partition TEXT, core_hours REAL, rsu REAL, max_rss_gb REAL,
    mem_allocated_gb REAL, cpu_hours REAL, cpu_efficiency REAL, job_err TEXT,
    PRIMARY KEY (workflow, cycle, name, submit_num)
);

-- A problem a check found: one row from first sighting to verified fix.
-- (check_id, subject) names the problem; at most one finding per pair is not `verified`.
CREATE TABLE IF NOT EXISTS findings (
    id INTEGER PRIMARY KEY,
    check_id TEXT NOT NULL,             -- e.g. perf.cost_model_overestimate
    subject TEXT NOT NULL DEFAULT '',   -- what it is about: a resolution, a station, a task
    kind TEXT NOT NULL,                 -- science, perf or ops
    severity TEXT NOT NULL,             -- critical, high, medium or low
    status TEXT NOT NULL,               -- open, filed, fix_pending, verified, regressed, dismissed
    title TEXT NOT NULL,
    summary TEXT,                       -- the latest failing evaluation's
    evidence TEXT,                      -- JSON, the latest failing evaluation's
    suspect_sources TEXT,               -- JSON list of run_sources.source names
    first_event TEXT, last_event TEXT,  -- the first and the latest event it failed on
    n_failing INTEGER DEFAULT 0,
    opened_at TEXT, updated_at TEXT,
    issue_url TEXT, issue_repo TEXT,    -- set by `auto-scan link`
    issue_closed_at TEXT,
    verified_event TEXT, verified_at TEXT
);
CREATE INDEX IF NOT EXISTS findings_key ON findings (check_id, subject, status);

-- Every evaluation of a check on a run, passing or failing: a finding's history,
-- and what the verification step reads.
CREATE TABLE IF NOT EXISTS evaluations (
    check_id TEXT NOT NULL,
    subject TEXT NOT NULL DEFAULT '',
    event_id TEXT NOT NULL,
    resolution TEXT NOT NULL DEFAULT '',
    failing INTEGER NOT NULL,
    value REAL, threshold REAL,
    summary TEXT,
    evaluated_at TEXT,
    PRIMARY KEY (check_id, subject, event_id, resolution)
);

CREATE TABLE IF NOT EXISTS meta (key TEXT PRIMARY KEY, value TEXT);
