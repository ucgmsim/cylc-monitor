"""End to end: Datasette 1.0a41 with server/'s config and plugins on a temp database,
uploaded to by the client and read with the reader token.


"""

from __future__ import annotations

import gzip
import json
import re
import shutil
import socket
import sqlite3
import subprocess
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path

import make_token
import pytest

from cylc_monitor import status, upload

SERVER = Path(__file__).resolve().parent.parent / "server"
RUN = Path(__file__).resolve().parent / "data" / "run"
PBS = Path(__file__).resolve().parent / "data" / "pbs"
SCAN_SCHEMA = Path(__file__).resolve().parent / "data" / "scan_schema.sql"
SECRET = "test-secret"


def free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def request(url: str, token: str | None = None, data: bytes | None = None) -> tuple[int, bytes]:
    headers = {"Authorization": f"Bearer {token}"} if token else {}
    try:
        with urllib.request.urlopen(urllib.request.Request(url, data=data, headers=headers), timeout=10) as response:
            return response.status, response.read()
    except urllib.error.HTTPError as error:
        return error.code, error.read()


def token(kind: str) -> str:
    command = [
        sys.executable,
        "-m",
        "datasette",
        "create-token",
        *make_token.TOKENS[kind],
        "--secret",
        SECRET,
    ]
    return subprocess.run(command, check=True, capture_output=True, text=True).stdout.strip()


@pytest.fixture(scope="module")
def monitor(tmp_path_factory: pytest.TempPathFactory):
    directory = tmp_path_factory.mktemp("monitor")
    port = free_port()
    env = {
        "DATASETTE_SECRET": SECRET,
        "ADMIN_PASSWORD_HASH": "pbkdf2_sha256$1$x$y",
        "PATH": "/usr/bin:/bin",
    }
    process = subprocess.Popen(
        [sys.executable, "-m", "datasette", "serve", str(directory / "cylc.db"), str(directory / "scan.db"), "--create",
         "--config", str(SERVER / "datasette.yaml"), "--plugins-dir", str(SERVER / "plugins"), "--static", f"assets:{SERVER / 'static'}",
         "--host", "127.0.0.1", "--port", str(port), "--setting", "force_https_urls", "1"],
        env=env, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
    )  # fmt: skip
    url = f"http://127.0.0.1:{port}"
    try:
        for _ in range(100):
            try:
                request(url)
                break
            except OSError:
                if process.poll() is not None:
                    assert process.stdout is not None
                    pytest.fail(process.stdout.read().decode())
                time.sleep(0.1)
        yield {
            "url": url,
            "db": directory / "cylc.db",
            "writer": token("writer"),
            "reader": token("reader"),
            "scan_writer": token("scan-writer"),
        }
    finally:
        process.terminate()
        process.wait(10)


def sql(monitor: dict, query: str) -> list[dict]:
    status_code, body = request(
        f"{monitor['url']}/cylc.json?{urllib.parse.urlencode({'sql': query, '_shape': 'array'})}",
        monitor["reader"],
    )
    assert status_code == 200, body
    return json.loads(body)


def snapshot(tmp_path: Path, workflow: str, state: str = "running", pbs: bool = False) -> Path:
    run = tmp_path / workflow.replace("/", "_")
    if not run.exists():
        shutil.copytree(RUN, run)
        if pbs:  # the sim jobs' scripts as Cylc's PBS job runner writes them
            shutil.copytree(PBS, run, dirs_exist_ok=True)
    path = tmp_path / f"{run.name}.db"
    path.unlink(missing_ok=True)
    upload.Snapshot(run, workflow, 300).build(path, state, "host", 1)
    return path


def test_only_the_writer_may_upload(monitor: dict, tmp_path: Path) -> None:
    body = snapshot(tmp_path, "w/run1").read_bytes()
    target = f"{monitor['url']}/-/cylc/upload?workflow=w/run1"
    assert request(target, None, body)[0] == 403
    assert request(target, monitor["reader"], body)[0] == 403
    assert request(f"{monitor['url']}/-/cylc/upload?workflow=../x", monitor["writer"], body)[0] == 400
    assert request(target, monitor["writer"], b"not sqlite")[0] == 400
    assert request(f"{monitor['url']}/-/dashboards/cylc")[0] == 403


def test_a_second_upload_replaces_the_first(monitor: dict, tmp_path: Path) -> None:
    for state in ("running", "running", "stopped"):
        upload.upload(
            monitor["url"],
            monitor["writer"],
            "w/run1",
            snapshot(tmp_path, "w/run1", state),
        )
    upload.upload(monitor["url"], monitor["writer"], "w/run2", snapshot(tmp_path, "w/run2"))
    counts = sql(
        monitor,
        "select workflow, count(*) as n from task_jobs group by workflow order by workflow",
    )
    assert counts == [{"workflow": "w/run1", "n": 8}, {"workflow": "w/run2", "n": 8}]
    assert sql(monitor, "select workflow, status from monitor order by workflow") == [
        {"workflow": "w/run1", "status": "stopped"},
        {"workflow": "w/run2", "status": "running"},
    ]
    # One database each for the monitor and the scan: the snapshots never become
    # databases (or files) of their own.
    assert {p.name for p in monitor["db"].parent.iterdir()} <= {
        "cylc.db",
        "cylc.db-wal",
        "cylc.db-shm",
        "scan.db",
        "scan.db-wal",
        "scan.db-shm",
    }


def test_jobs_view(monitor: dict, tmp_path: Path) -> None:
    upload.upload(monitor["url"], monitor["writer"], "w/run2", snapshot(tmp_path, "w/run2"))
    sim = sql(
        monitor,
        "select * from jobs where workflow = 'w/run2' and name = 'sim' and cycle = '1'",
    )[0]
    assert (
        sim["state"],
        sim["cores"],
        sim["mem_gb"],
        sim["account"],
        sim["queue_s"],
    ) == ("succeeded", 8, 3.91, "test0001", 10)
    assert sim["core_hours"] == pytest.approx(8 * sim["run_s"] / 3600, abs=0.01)
    assert sim["max_rss_gb"] > 0 and sim["cpu_hours"] > 0  # Cylc's profiler was on for this run
    flaky = sql(
        monitor,
        "select submit_num, state, job_err from jobs where workflow = 'w/run2' and name = 'flaky' and cycle = '1' order by submit_num",
    )
    assert [(j["submit_num"], j["state"]) for j in flaky] == [
        (1, "failed"),
        (2, "succeeded"),
    ]
    assert "first try fails" in flaky[0]["job_err"]


def test_jobs_view_pbs(monitor: dict, tmp_path: Path) -> None:
    upload.upload(monitor["url"], monitor["writer"], "w/pbs", snapshot(tmp_path, "w/pbs", "stopped", pbs=True))
    sims = sql(
        monitor,
        "select cycle, runner, cores, nodes, chunks, mem_gb, time_limit_min, account, partition, run_s, core_hours, rsu"
        " from jobs where workflow = 'w/pbs' and name = 'sim' order by cycle",
    )
    whole, part = sims
    assert (whole["runner"], whole["cores"], whole["nodes"], whole["chunks"], whole["mem_gb"]) == (
        "pbs",
        768,
        2,
        2,
        1400,
    )
    assert (whole["time_limit_min"], whole["account"], whole["partition"]) == (10, "test0001", "high_mem_shortq")
    assert whole["core_hours"] == pytest.approx(768 * whole["run_s"] / 3600, abs=0.01)
    assert whole["rsu"] == pytest.approx((1.4 * 768 + 0.2 * 1400) * whole["run_s"] / 3600, abs=0.01)
    assert (part["runner"], part["cores"], part["nodes"], part["mem_gb"]) == ("pbs", 8, 1, 3.91)
    # Slurm jobs alongside are as they were.
    assert (
        sql(monitor, "select runner, chunks from jobs where workflow = 'w/run2' and name = 'sim'")
        == [{"runner": "slurm", "chunks": None}] * 2
    )


def test_views_over_an_older_uploaders_tables(tmp_path: Path) -> None:
    import sqlite3

    import cylc_upload

    path = snapshot(tmp_path, "w/old")
    with sqlite3.connect(path) as old:  # as an uploader from before PBS support made it
        old.execute("alter table job_resources drop column runner")
        old.execute("alter table job_resources drop column chunks")
    conn = sqlite3.connect(f"file:{tmp_path / 'cylc.db'}", uri=True, isolation_level=None)  # as Datasette does
    cylc_upload.merge(conn, str(path), "w/old")
    rows = conn.execute("select cores, runner, chunks from jobs where name = 'sim'").fetchall()
    assert rows == [(8, None, None)] * 2


def test_dashboard_queries_run(monitor: dict) -> None:
    import yaml

    charts = yaml.safe_load((SERVER / "datasette.yaml").read_text())["plugins"]["datasette-dashboards"]["cylc"][
        "charts"
    ]
    for chart in charts.values():
        query = chart["query"].replace("[[", "/*").replace("]]", "*/")
        sql(monitor, query)  # asserts 200
    status_code, body = request(f"{monitor['url']}/-/dashboards/cylc", monitor["reader"])
    assert status_code == 200
    # Behind a TLS proxy the charts' data URLs must be https, or the browser blocks them.
    assert b"'https://127.0.0.1" in body and b"'http://127.0.0.1" not in body


def test_status_ignores_a_retry_that_succeeded(monitor: dict, tmp_path: Path, capsys: pytest.CaptureFixture) -> None:
    token_file = tmp_path / "reader"
    token_file.write_text(monitor["reader"])
    assert status.main(["--url", monitor["url"], "--token-file", str(token_file)]) == 0
    out = capsys.readouterr().out
    assert "w/run2: RUNNING" in out and "w/run1" not in out  # run1 stopped
    assert "FAILED" not in out


def test_dashboard_table_assets_are_served(monitor: dict) -> None:
    status_code, body = request(f"{monitor['url']}/-/dashboards/cylc", monitor["reader"])
    assert status_code == 200, body
    assert b"/assets/dashboard.css" in body
    status_code, body = request(f"{monitor['url']}/assets/dashboard.css", monitor["reader"])
    assert status_code == 200 and b".dashboard-card-chart td" in body
    assert b"/assets/dashboard.js" in request(f"{monitor['url']}/-/dashboards/cylc", monitor["reader"])[1]
    status_code, body = request(f"{monitor['url']}/assets/dashboard.js", monitor["reader"])
    assert status_code == 200 and b"aria-sort" in body


def scan_snapshot(tmp_path: Path, schema_version: str = "1", findings: int = 1) -> bytes:
    """A gzipped scan.db as auto_scan uploads it: one event, one run, a metric and some findings."""
    path = tmp_path / "scan-snapshot.db"
    path.unlink(missing_ok=True)
    db = sqlite3.connect(path)
    db.executescript(SCAN_SCHEMA.read_text())
    db.execute("INSERT INTO meta VALUES ('schema_version', ?)", (schema_version,))
    db.execute(
        "INSERT INTO events (event_id, cycle, event_dir, origin_time, magnitude) VALUES (?, ?, ?, ?, ?)",
        ("2026p1", 1, "1-2026p1", "2026-11-17T10:30:48+00:00", 4.8),
    )
    db.execute(
        "INSERT INTO runs (event_id, resolution, status, written) VALUES ('2026p1', '0_5hz', 'succeeded', ?)",
        ("2026-11-17T20:40:24+00:00",),
    )
    db.execute("INSERT INTO metrics VALUES ('2026p1', '0_5hz', 'perf.updates_per_core_s', 5.2e5, NULL, NULL)")
    for i in range(findings):
        db.execute(
            "INSERT INTO findings (check_id, subject, kind, severity, status, title, first_event, last_event, opened_at)"
            " VALUES (?, '0_5hz', 'perf', 'high', 'open', 'A finding', '2026p1', '2026p1', '2026-11-18T00:00:00+00:00')",
            (f"perf.check_{i}",),
        )
    db.commit()
    db.close()
    return gzip.compress(path.read_bytes())


def scan_sql(monitor: dict, query: str) -> list[dict]:
    status_code, body = request(
        f"{monitor['url']}/scan.json?{urllib.parse.urlencode({'sql': query, '_shape': 'array'})}",
        monitor["reader"],
    )
    assert status_code == 200, body
    return json.loads(body)


def test_only_the_scan_writer_may_upload_the_scan_database(monitor: dict, tmp_path: Path) -> None:
    body = scan_snapshot(tmp_path)
    target = f"{monitor['url']}/-/scan/upload"
    assert request(target, None, body)[0] == 403
    assert request(target, monitor["reader"], body)[0] == 403
    assert request(target, monitor["writer"], body)[0] == 403
    status_code, response = request(target, monitor["scan_writer"], body)
    assert status_code == 200, response
    assert json.loads(response)["ok"] is True
    # The scan-writer's token is no use on the monitor's own upload.
    assert request(f"{monitor['url']}/-/cylc/upload?workflow=w/run1", monitor["scan_writer"], b"x")[0] == 403


def test_a_scan_upload_replaces_the_database_and_makes_the_views(monitor: dict, tmp_path: Path) -> None:
    target = f"{monitor['url']}/-/scan/upload"
    assert request(target, monitor["scan_writer"], scan_snapshot(tmp_path, findings=3))[0] == 200
    assert scan_sql(monitor, "SELECT count(*) AS n FROM findings") == [{"n": 3}]
    assert scan_sql(monitor, "SELECT count(*) AS n FROM findings_open") == [{"n": 3}]
    # A later snapshot replaces the earlier one whole: it is a copy, not a merge.
    assert request(target, monitor["scan_writer"], scan_snapshot(tmp_path, findings=1))[0] == 200
    assert scan_sql(monitor, "SELECT count(*) AS n FROM findings") == [{"n": 1}]
    assert scan_sql(monitor, "SELECT event_id FROM events") == [{"event_id": "2026p1"}]
    # The monitor's own database is untouched by it.
    assert request(f"{monitor['url']}/cylc.json", monitor["reader"])[0] == 200


def test_a_scan_upload_this_server_does_not_know_changes_nothing(monitor: dict, tmp_path: Path) -> None:
    target = f"{monitor['url']}/-/scan/upload"
    assert request(target, monitor["scan_writer"], scan_snapshot(tmp_path, findings=2))[0] == 200
    status_code, response = request(target, monitor["scan_writer"], scan_snapshot(tmp_path, schema_version="999"))
    assert status_code == 400, response
    assert request(target, monitor["scan_writer"], b"not a database")[0] == 400
    assert scan_sql(monitor, "SELECT count(*) AS n FROM findings") == [{"n": 2}]


def test_the_scan_dashboard_and_its_charts_load(monitor: dict, tmp_path: Path) -> None:
    assert request(f"{monitor['url']}/-/scan/upload", monitor["scan_writer"], scan_snapshot(tmp_path))[0] == 200
    assert request(f"{monitor['url']}/-/dashboards/scan", monitor["reader"])[0] == 200
    import yaml

    charts = yaml.safe_load((SERVER / "datasette.yaml").read_text())["plugins"]["datasette-dashboards"]["scan"][
        "charts"
    ]
    for name, chart in charts.items():
        if "query" not in chart:
            continue
        # A filter's [[ ... ]] clause is dropped when the filter is unset, as the dashboard does.
        query = re.sub(r"\[\[.*?\]\]", "", chart["query"], flags=re.DOTALL)
        status_code, body = request(
            f"{monitor['url']}/{chart['db']}.json?{urllib.parse.urlencode({'sql': query, '_shape': 'array'})}",
            monitor["reader"],
        )
        assert status_code == 200, (name, body)
