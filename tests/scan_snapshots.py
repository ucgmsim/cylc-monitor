"""Small snapshots of auto_scan's two databases, scan.db and cmt.db, for the scan_upload tests.

The tables are auto_scan's (``SCHEMA`` in auto_scan's ``src/auto_scan/db.py``, schema
version 1), copied here so the tests don't need auto_scan. The rows are made up, with
one or more in every table the views read.
"""

import sqlite3
from pathlib import Path

SCHEMA = """
CREATE TABLE events (
    event_id TEXT PRIMARY KEY, cycle INTEGER, event_dir TEXT NOT NULL, origin_time TEXT, magnitude REAL,
    latitude REAL, longitude REAL, depth_km REAL, location TEXT
);
CREATE TABLE runs (
    event_id TEXT NOT NULL REFERENCES events, resolution TEXT NOT NULL, status TEXT, written TEXT,
    defaults_version TEXT, manifest_sha256 TEXT, scanned_at TEXT,
    PRIMARY KEY (event_id, resolution)
);
CREATE TABLE run_sources (
    event_id TEXT NOT NULL, resolution TEXT NOT NULL, source TEXT NOT NULL, url TEXT, ref TEXT, commit_sha TEXT,
    PRIMARY KEY (event_id, resolution, source)
);
CREATE TABLE metrics (
    event_id TEXT NOT NULL, resolution TEXT NOT NULL DEFAULT '', metric TEXT NOT NULL, value REAL, unit TEXT,
    detail TEXT,
    PRIMARY KEY (event_id, resolution, metric)
);
CREATE TABLE station_residuals (
    event_id TEXT NOT NULL, resolution TEXT NOT NULL, station TEXT NOT NULL, im TEXT NOT NULL,
    observed REAL, simulated REAL, gmm REAL, residual REAL, rrup_km REAL, vs30 REAL, quality_ok INTEGER,
    PRIMARY KEY (event_id, resolution, station, im)
);
CREATE TABLE sw4_timers (
    event_id TEXT NOT NULL, resolution TEXT NOT NULL, region TEXT NOT NULL, depth INTEGER,
    calls_total INTEGER, ranks INTEGER, min_s REAL, avg_s REAL, max_s REAL, wall_avg_s REAL,
    mpi_min_s REAL, mpi_avg_s REAL, mpi_max_s REAL, omp_wait_frac REAL, omp_imb_avg REAL, omp_imb_max REAL,
    PRIMARY KEY (event_id, resolution, region)
);
CREATE TABLE jobs (
    workflow TEXT NOT NULL, cycle TEXT NOT NULL, name TEXT NOT NULL, submit_num INTEGER NOT NULL,
    try_num INTEGER, job_id TEXT, platform TEXT, runner TEXT, state TEXT,
    time_submit TEXT, time_run TEXT, time_run_exit TEXT, queue_s REAL, run_s REAL,
    cores INTEGER, nodes INTEGER, chunks INTEGER, mem_gb REAL, time_limit_min REAL,
    account TEXT, partition TEXT, core_hours REAL, rsu REAL, max_rss_gb REAL,
    mem_allocated_gb REAL, cpu_hours REAL, cpu_efficiency REAL, job_err TEXT,
    PRIMARY KEY (workflow, cycle, name, submit_num)
);
CREATE TABLE findings (
    id INTEGER PRIMARY KEY, check_id TEXT NOT NULL, subject TEXT NOT NULL DEFAULT '', kind TEXT NOT NULL,
    severity TEXT NOT NULL, status TEXT NOT NULL, title TEXT NOT NULL, summary TEXT, evidence TEXT,
    suspect_sources TEXT, first_event TEXT, last_event TEXT, n_failing INTEGER DEFAULT 0,
    opened_at TEXT, updated_at TEXT, issue_url TEXT, issue_repo TEXT, issue_closed_at TEXT,
    verified_event TEXT, verified_at TEXT
);
CREATE INDEX findings_key ON findings (check_id, subject, status);
CREATE TABLE evaluations (
    check_id TEXT NOT NULL, subject TEXT NOT NULL DEFAULT '', event_id TEXT NOT NULL,
    resolution TEXT NOT NULL DEFAULT '', failing INTEGER NOT NULL, value REAL, threshold REAL,
    summary TEXT, evaluated_at TEXT,
    PRIMARY KEY (check_id, subject, event_id, resolution)
);
CREATE TABLE meta (key TEXT PRIMARY KEY, value TEXT);
INSERT INTO meta VALUES ('schema_version', '1');
"""

CMT_EVENT = "2026p866048"
CMT_RUNS = ("all_stations", "azimuth", "drop_outliers_dev")


def connect(path: Path) -> sqlite3.Connection:
    """A new, empty database at ``path`` with auto_scan's tables."""
    db = sqlite3.connect(path)
    db.executescript(SCHEMA)
    return db


def set_metric(db: sqlite3.Connection, event_id: str, resolution: str, metric: str, value: float) -> None:
    db.execute("INSERT OR REPLACE INTO metrics VALUES (?, ?, ?, ?, NULL, NULL)", (event_id, resolution, metric, value))


def make_scan_snapshot(path: Path, n_events: int = 6) -> None:
    """A small scan.db with a row in every table the scan views read."""
    db = connect(path)
    for i in range(n_events):
        event = f"2026p{i:03d}"
        db.execute(
            "INSERT INTO events VALUES (?, ?, ?, ?, ?, -43.5, 172.6, 10.0, 'near Christchurch')",
            (event, i + 1, f"{i + 1}-{event}", f"2026-11-{i + 1:02d}T00:00:00+00:00", 4.5 + i / 10),
        )
        for resolution in ("0_25hz", "1hz"):
            db.execute(
                "INSERT INTO runs VALUES (?, ?, 'succeeded', ?, 'v1', 'abc', ?)",
                (event, resolution, f"2026-11-{i + 1:02d}T05:00:00+00:00", f"2026-11-{i + 1:02d}T06:00:00+00:00"),
            )
            db.execute(
                "INSERT INTO run_sources VALUES (?, ?, 'sw4', 'https://github.com/ucgmsim/sw4.git', 'main', ?)",
                (event, resolution, f"sha{i}"),
            )
            for im in ("PGA", "PGV", "pSA_1.0"):
                set_metric(db, event, resolution, f"science.residual.median.{im}", 0.1 * i)
                set_metric(db, event, resolution, f"science.residual.mean.{im}", 0.1 * i + 0.01)
                set_metric(db, event, resolution, f"science.residual.mad_sigma.{im}", 0.6)
                set_metric(db, event, resolution, f"science.residual.n.{im}", 20)
                for s in range(4):
                    db.execute(
                        "INSERT INTO station_residuals VALUES (?, ?, ?, ?, 1.0, 1.0, NULL, ?, 30.0, 400.0, 1)",
                        (event, resolution, f"ST{s}", im, 0.1 * s + 0.05 * i),
                    )
            set_metric(db, event, resolution, "science.residual.band_median.lf", -0.2)
            set_metric(db, event, resolution, "science.residual.n_stations", 25)
            set_metric(db, event, resolution, "perf.updates_per_core_s", 2.0e6 - i * 1e4)
            set_metric(db, event, resolution, "perf.cores", 1024)
            set_metric(db, event, resolution, "perf.actual_core_hours", 100.0 + i)
            set_metric(db, event, resolution, "perf.estimated_core_hours_effective", 150.0)
            set_metric(db, event, resolution, "perf.cost_ratio", 150.0 / (100.0 + i))
            set_metric(db, event, resolution, "perf.mpi_fraction", 0.2)
            set_metric(db, event, resolution, "ops.time_to_results_from_origin_s", 5 * 3600.0)
            db.execute(
                "INSERT INTO sw4_timers (event_id, resolution, region, depth, calls_total, ranks, min_s, avg_s, max_s)"
                " VALUES (?, ?, 'solve', 0, 1, 64, 90, 100, 110), (?, ?, 'solve/time_stepping', 1, 1, 64, 80, 90, 99)",
                (event, resolution, event, resolution),
            )
        set_metric(db, event, "", "science.xres.0_25hz-1hz.median", 0.05)
        set_metric(db, event, "", "science.xres.0_25hz-1hz.mad_sigma", 0.1)
        set_metric(db, event, "", "science.xres.0_25hz-1hz.n", 300)
        set_metric(db, event, "", "ops.core_hours", 250.0)
        db.execute(
            "INSERT INTO evaluations VALUES ('perf.speed', '1hz', ?, '1hz', ?, 1.0, 1.5, 's', '2026-11-19')",
            (event, int(i == n_events - 1)),
        )
    db.execute(
        "INSERT INTO jobs (workflow, cycle, name, submit_num, state, time_run_exit, job_err) VALUES"
        " ('realtime/run1', '3', 'sw4_1hz', 1, 'failed', '2026-11-03T03:00:00Z', 'slurmstepd: error: *** JOB 1 DUE TO TIME LIMIT'),"
        " ('realtime/run1', '4', 'upload_1hz', 1, 'failed', '2026-11-04T03:00:00Z', 'Traceback (most recent call last)'),"
        " ('realtime/run1', '4', 'sw4_1hz', 1, 'succeeded', '2026-11-04T02:00:00Z', NULL)"
    )
    db.execute(
        "INSERT INTO findings (check_id, subject, kind, severity, status, title, first_event, last_event, n_failing,"
        " opened_at, updated_at, issue_url) VALUES"
        " ('perf.speed', '1hz', 'perf', 'high', 'open', 'SW4 slowed', '2026p005', '2026p005', 1, '2026-11-19', '2026-11-19', NULL),"
        " ('science.bias', 'PGV', 'science', 'medium', 'verified', 'PGV bias', '2026p001', '2026p002', 2, '2026-11-02',"
        " '2026-11-10', 'https://github.com/ucgmsim/sw4/issues/1')"
    )
    db.commit()
    db.close()


def make_cmt_snapshot(path: Path) -> None:
    """A small cmt.db: two events, each with three CMT runs, their metrics, checks and a finding.

    In cmt.db a run is one of an event's CMT runs (``runs.resolution`` is its name), and
    an event's own metrics have resolution ``''``.
    """
    db = connect(path)
    for day, event in enumerate([CMT_EVENT, "2026p866049"], start=1):
        db.execute(
            "INSERT INTO events (event_id, event_dir, origin_time, magnitude, location)"
            " VALUES (?, ?, ?, 4.8, 'near Seddon')",
            (event, event, f"2026-11-1{day}T10:30:48+00:00"),
        )
        for run in CMT_RUNS:
            db.execute(
                "INSERT INTO runs (event_id, resolution, status, written) VALUES (?, ?, 'ok', ?)",
                (event, run, f"2026-11-1{day}T10:58:20+00:00"),
            )
            deviatoric = run == "drop_outliers_dev"
            for metric, value in {
                "cmt.run_ok": 1,
                "cmt.selected": float(run == "all_stations"),
                "cmt.deviatoric": float(deviatoric),
                "cmt.n_stations": 4,
                "cmt.vr": 0.62,
                "cmt.mw": 5.2,
                "cmt.dc_percent": 80.0,
                "cmt.clvd_percent": 20.0 if deviatoric else 15.0,
                "cmt.iso_percent": 0.0 if deviatoric else 5.0,
                "cmt.centroid_depth_km": 12.0,
                "cmt.on_grid_edge": 0,
                "cmt.posterior.effective_cells": 3.5,
                "cmt.posterior.boundary_probability": 0.01,
                "cmt.posterior.variance_scale": 1.2,
                "cmt.reduced_chi_square": 1.5,
                "cmt.kagan_to_selected_deg": 0.0 if run == "all_stations" else 12.0,
                "cmt.jackknife.max_kagan_deg": 9.0,
                "cmt.station.negative_vr_fraction": 0.0,
                "cmt.mouse_components": 0,
                "cmt.timing.mouse_detection_s": 4.0,
                "cmt.timing.process_data_s": 30.0,
                "cmt.timing.other_s": 6.0,
                "cmt.timing.total_s": 40.0,
            }.items():
                set_metric(db, event, run, metric, value)
        for metric, value in {
            "cmt.n_runs": 3,
            "cmt.n_runs_ok": 3,
            "cmt.mw": 5.2,
            "cmt.geonet_magnitude": 5.33,
            "cmt.mw_minus_geonet": 5.2 - 5.33,
            "cmt.time_to_solution_s": 1650.0,
            "cmt.timing.download_s": 300.0,
            "cmt.timing.cleanup_s": 2.0,
            "cmt.timing.total_s": 1500.0,
        }.items():
            set_metric(db, event, "", metric, value)
        db.execute(
            "INSERT INTO evaluations VALUES ('cmt.vr_low', 'all_stations', ?, 'all_stations', 0, 0.62, 0.4,"
            " 'VR fine', '2026-11-19'), ('cmt.mw_vs_geonet', '', ?, '', ?, -0.13, 0.1, 'Mw low', '2026-11-19')",
            (event, event, int(day == 2)),
        )
    db.execute(
        "INSERT INTO findings (check_id, subject, kind, severity, status, title, first_event, last_event, n_failing,"
        " opened_at, updated_at) VALUES ('cmt.mw_vs_geonet', '', 'science', 'medium', 'open', 'Mw below GeoNet',"
        " '2026p866049', '2026p866049', 1, '2026-11-19', '2026-11-19')"
    )
    db.commit()
    db.close()
