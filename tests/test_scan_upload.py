"""auto_scan's side of the server (server/plugins/scan_upload.py): the upload's merge, the scan
and cmt views, and the queries of the scan and cmt dashboards. The upload over HTTP is in
test_server.py.
"""

import gzip
import re
import sqlite3
import statistics
from pathlib import Path

import pytest
import scan_upload as plugin
from scan_snapshots import CMT_EVENT, make_cmt_snapshot, make_scan_snapshot

SERVER = Path(__file__).resolve().parent.parent / "server"


@pytest.fixture
def snapshot(tmp_path: Path) -> Path:
    path = tmp_path / "upload.db"
    make_scan_snapshot(path)
    return path


@pytest.fixture
def cmt_snapshot(tmp_path: Path) -> Path:
    path = tmp_path / "cmt-upload.db"
    make_cmt_snapshot(path)
    return path


@pytest.fixture
def target(tmp_path: Path) -> sqlite3.Connection:
    """The server's scan database, holding an older upload's leftovers."""
    conn = sqlite3.connect(tmp_path / "scan.db")
    conn.execute("CREATE TABLE stale (x)")
    conn.execute("CREATE TABLE events (event_id TEXT)")
    conn.execute("INSERT INTO events VALUES ('old')")
    conn.execute("CREATE VIEW stale_view AS SELECT * FROM stale")
    conn.commit()
    plugin.add_functions(conn)
    return conn


def names(conn: sqlite3.Connection, kind: str) -> set[str]:
    return {r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type = ?", (kind,))}


def test_merge_replaces_tables_and_makes_views(target: sqlite3.Connection, snapshot: Path) -> None:
    counts = plugin.merge(target, str(snapshot))
    assert counts["events"] == 6 and counts["runs"] == 12 and counts["findings"] == 2
    assert "stale" not in names(target, "table") and "stale_view" not in names(target, "view")
    assert {"findings_open", "event_summary", "perf_by_run", "station_bias", "timers_top"} <= names(target, "view")
    assert "findings_key" in names(target, "index")
    assert target.execute("SELECT count(*) FROM events WHERE event_id = 'old'").fetchone()[0] == 0
    assert not target.in_transaction
    assert target.execute("PRAGMA database_list").fetchall()[-1][1] == "main"  # snapshot detached


def test_merge_is_idempotent(target: sqlite3.Connection, snapshot: Path) -> None:
    first = plugin.merge(target, str(snapshot))
    assert plugin.merge(target, str(snapshot)) == first


def test_every_view_runs(target: sqlite3.Connection, snapshot: Path) -> None:
    plugin.merge(target, str(snapshot))
    for view in sorted(names(target, "view")):
        target.execute(f"SELECT * FROM {view}").fetchall()


def test_view_values(target: sqlite3.Connection, snapshot: Path) -> None:
    plugin.merge(target, str(snapshot))
    target.row_factory = sqlite3.Row
    row = target.execute("SELECT * FROM event_summary WHERE event_id = '2026p003'").fetchone()
    assert row["status_1hz"] == "succeeded" and row["status_0_5hz"] is None
    assert row["event_term_pgv_1hz"] == pytest.approx(0.3)
    assert row["event_term_lf_1hz"] == pytest.approx(-0.2)
    assert row["xres_0_25hz_1hz"] == pytest.approx(0.05)
    assert row["hours_to_results_1hz"] == pytest.approx(5.0)
    assert row["n_checks"] == 1 and row["n_failing_checks"] == 0

    term = target.execute(
        "SELECT * FROM science_event_terms WHERE event_id = '2026p002' AND resolution = '1hz' AND im = 'pSA_1.0'"
    ).fetchone()
    assert term["event_term"] == pytest.approx(0.2)
    assert term["mean"] == pytest.approx(0.21) and term["mad_sigma"] == pytest.approx(0.6)
    assert term["n_stations"] == 20

    xres = target.execute("SELECT * FROM xres_by_event WHERE event_id = '2026p000'").fetchone()
    assert (xres["pair"], xres["median_mismatch"], xres["mad_sigma"], xres["n_sites"]) == ("0_25hz-1hz", 0.05, 0.1, 300)

    perf = target.execute("SELECT * FROM perf_by_run WHERE event_id = '2026p001' AND resolution = '1hz'").fetchone()
    assert perf["updates_per_core_s"] == pytest.approx(1.99e6)
    assert perf["cores"] == 1024 and perf["actual_core_hours"] == pytest.approx(101.0)
    assert perf["hours_to_results"] == pytest.approx(5.0)

    bias = target.execute(
        "SELECT * FROM station_bias WHERE station = 'ST3' AND resolution = '1hz' AND im = 'PGA'"
    ).fetchone()
    # Each event's residuals are 0.05 i + (0, .1, .2, .3): ST3's site term is 0.3 - 0.15.
    assert bias["n_events"] == 6 and bias["site_term"] == pytest.approx(0.15)
    assert bias["site_sigma"] == pytest.approx(0.0, abs=1e-9)

    by_event = target.execute(
        "SELECT * FROM residuals_by_event WHERE event_id = '2026p000' AND resolution = '1hz' AND im = 'PGA'"
    ).fetchone()
    assert by_event["event_term"] == pytest.approx(0.15)
    assert by_event["within_sigma"] == pytest.approx(statistics.stdev([0, 0.1, 0.2, 0.3]), abs=1e-4)

    classes = dict(target.execute("SELECT name, failure_class FROM job_failures").fetchall())
    assert classes == {"sw4_1hz": "time limit", "upload_1hz": "Python exception"}

    timers = target.execute("SELECT region, rank FROM timers_top WHERE event_id = '2026p000' AND resolution = '1hz'")
    assert dict(timers.fetchall()) == {"solve": 1, "solve/time_stepping": 2}

    open_ = target.execute("SELECT id, status FROM findings_open").fetchall()
    assert [tuple(r) for r in open_] == [(1, "open")]


def test_merge_rejects_unknown_schema_version(target: sqlite3.Connection, snapshot: Path) -> None:
    conn = sqlite3.connect(snapshot)
    conn.execute("UPDATE meta SET value = '99' WHERE key = 'schema_version'")
    conn.commit()
    conn.close()
    with pytest.raises(ValueError, match="schema_version"):
        plugin.merge(target, str(snapshot))
    assert "stale" in names(target, "table")  # untouched
    assert not target.in_transaction


def test_merge_rejects_other_databases(target: sqlite3.Connection, tmp_path: Path) -> None:
    cylc = tmp_path / "cylc.db"
    conn = sqlite3.connect(cylc)
    conn.execute("CREATE TABLE task_jobs (x)")
    conn.commit()
    conn.close()
    with pytest.raises(ValueError, match="not a scan.db"):
        plugin.merge(target, str(cylc))

    junk = tmp_path / "junk.db"
    junk.write_bytes(b"SQLite format 3\x00" + b"\x00" * 50)
    with pytest.raises((ValueError, sqlite3.DatabaseError)):
        plugin.merge(target, str(junk))
    assert "stale" in names(target, "table")


def test_merge_rolls_back_a_failing_view(target: sqlite3.Connection, snapshot: Path) -> None:
    with pytest.raises(sqlite3.OperationalError):
        plugin.merge(target, str(snapshot), views_sql="CREATE VIEW broken AS SELEC 1;\n")
    assert "stale" in names(target, "table")
    assert target.execute("SELECT event_id FROM events").fetchall() == [("old",)]


def test_views_noop_without_tables(tmp_path: Path) -> None:
    conn = sqlite3.connect(tmp_path / "empty.db")
    plugin.views(conn)
    assert names(conn, "view") == set()


def test_views_remade(target: sqlite3.Connection, snapshot: Path) -> None:
    plugin.merge(target, str(snapshot))
    target.execute("DROP VIEW event_summary")
    plugin.views(target)
    assert "event_summary" in names(target, "view")


def test_gunzip_limited() -> None:
    raw = b"SQLite format 3\x00" + b"x" * 1000
    assert plugin.gunzip_limited(gzip.compress(raw)) == raw
    assert plugin.gunzip_limited(raw) == raw
    with pytest.raises(ValueError, match="too big"):
        plugin.gunzip_limited(gzip.compress(b"\x00" * 5_000_000), limit=1_000_000)
    with pytest.raises(ValueError, match="too big"):
        plugin.gunzip_limited(raw, limit=10)
    with pytest.raises(ValueError, match="gzip"):
        plugin.gunzip_limited(b"\x1f\x8b" + b"garbage")


def test_stdev_aggregate() -> None:
    conn = sqlite3.connect(":memory:")
    plugin.add_functions(conn)
    conn.execute("CREATE TABLE t (x)")
    values = [0.3, -1.2, 2.5, 0.0, 0.7]
    conn.executemany("INSERT INTO t VALUES (?)", [(v,) for v in values] + [(None,), ("text",)])
    assert conn.execute("SELECT stdev(x) FROM t").fetchone()[0] == pytest.approx(statistics.stdev(values))
    assert conn.execute("SELECT stdev(x) FROM t WHERE x = 0.3").fetchone()[0] is None


def test_statements_split() -> None:
    sql = "-- a comment;\n-- only\nCREATE VIEW a AS SELECT 1;\n\nCREATE VIEW b AS\nSELECT 2;\n"
    parts = plugin.statements(sql)
    assert len(parts) == 2
    assert "VIEW a AS" in parts[0] and "VIEW b AS" in parts[1]


def dashboard(name: str) -> dict:
    """One of datasette.yaml's dashboards, checked for a well-formed layout over its own database."""
    yaml = pytest.importorskip("yaml")
    config = yaml.safe_load((SERVER / "datasette.yaml").read_text())
    board = config["plugins"]["datasette-dashboards"][name]
    assert {cell for row in board["layout"] for cell in row} == set(board["charts"]), "every chart laid out, no more"
    assert len({len(row) for row in board["layout"]}) == 1, "layout rows of equal width"
    for spec in [*board["charts"].values(), *board["filters"].values()]:
        assert spec["db"] == name
    for spec in board["charts"].values():
        assert spec["library"] in {"metric", "table", "vega-lite"}
    return board


def queries(board: dict) -> dict[str, str]:
    found = {f"filter {name}": spec["query"] for name, spec in board["filters"].items()}
    return found | {f"chart {name}": spec["query"] for name, spec in board["charts"].items()}


def fill(query: str, options: dict[str, str]) -> str:
    """datasette-dashboards' replacement: keep a [[ ... :name ... ]] block when the filter is set."""

    def keep(match: re.Match[str]) -> str:
        variable = re.search(r":(\w+)", match.group(0))
        return match.group(0)[2:-2] if variable and options.get(variable.group(1)) else ""

    return re.sub(r"\[\[[^\]]*\]\]", keep, query)


def test_datasette_yaml_has_the_auto_scan_dashboards() -> None:
    yaml = pytest.importorskip("yaml")
    config = yaml.safe_load((SERVER / "datasette.yaml").read_text())
    assert {"cylc", "scan", "cmt"} <= set(config["plugins"]["datasette-dashboards"])
    assert "scan-writer" in config["allow"]["id"]


@pytest.mark.parametrize("options", [{}, {"resolution": "1hz"}, {"resolution": "1hz", "im": "PGV"}])
def test_scan_dashboard_queries_run(target: sqlite3.Connection, snapshot: Path, options: dict[str, str]) -> None:
    board = dashboard("scan")
    plugin.merge(target, str(snapshot))
    for name, query in queries(board).items():
        assert target.execute(fill(query, options), options).fetchall() is not None, name


def test_scan_dashboard_has_data(target: sqlite3.Connection, snapshot: Path) -> None:
    found = queries(dashboard("scan"))
    plugin.merge(target, str(snapshot))
    options = {"resolution": "1hz"}
    for name in ("chart event-terms", "chart xres", "chart speed", "chart cost", "chart mpi", "chart time-to-results"):
        assert target.execute(fill(found[name], options), options).fetchall(), f"{name} is empty"


@pytest.mark.parametrize("options", [{}, {"run": "all_stations"}])
def test_cmt_views_and_dashboard(tmp_path: Path, cmt_snapshot: Path, options: dict[str, str]) -> None:
    target = sqlite3.connect(tmp_path / "cmt.db")
    plugin.add_functions(target)
    counts = plugin.merge(target, str(cmt_snapshot), database="cmt")
    assert counts["events"] == 2 and counts["runs"] == 6
    assert {"findings_open", "cmt_by_run", "cmt_event_summary", "cmt_timings"} <= names(target, "view")
    assert "event_summary" not in names(target, "view")  # the simulation views are not made here
    target.row_factory = sqlite3.Row
    row = target.execute("SELECT * FROM cmt_event_summary WHERE event_id = ?", (CMT_EVENT,)).fetchone()
    assert row["selected_run"] == "all_stations" and row["geonet_magnitude"] == pytest.approx(5.33)
    assert row["n_stations"] == 4 and row["n_runs_ok"] == 3 and row["minutes_to_solution"] == pytest.approx(27.5)
    assert row["n_checks"] == 2 and row["n_failing_checks"] == 0
    run = target.execute(
        "SELECT * FROM cmt_by_run WHERE event_id = ? AND run = 'drop_outliers_dev'", (CMT_EVENT,)
    ).fetchone()
    assert run["deviatoric"] == 1 and run["iso_percent"] == pytest.approx(0.0)
    stages = {r["stage"] for r in target.execute("SELECT stage FROM cmt_timings WHERE run = 'all_stations'")}
    assert stages == {"mouse_detection", "process_data", "other"}
    for view in sorted(names(target, "view")):
        target.execute(f"SELECT * FROM {view}").fetchall()
    board = dashboard("cmt")
    for name, query in queries(board).items():
        assert target.execute(fill(query, options), options).fetchall() is not None, name
    stages_query = fill(board["charts"]["stages"]["query"], options)
    assert target.execute(stages_query, options).fetchall(), "the stages chart is empty"
    for name in ("chart magnitude", "chart vr", "chart chi2", "chart time-to-solution", "chart findings"):
        assert target.execute(fill(queries(board)[name], options), options).fetchall(), f"{name} is empty"
    plugin.views(target, database="cmt")  # startup remakes them
