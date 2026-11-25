"""End to end: Datasette 1.0a41 with server/'s config and plugins on a temp database,
uploaded to by the client and read with the reader token.


"""

from __future__ import annotations

import gzip
import json
import shutil
import socket
import subprocess
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path

import make_token
import pytest
from datasette.app import Datasette
from scan_snapshots import make_cmt_snapshot, make_scan_snapshot

from cylc_monitor import queue, status, upload

SERVER = Path(__file__).resolve().parent.parent / "server"
RUN = Path(__file__).resolve().parent / "data" / "run"
PBS = Path(__file__).resolve().parent / "data" / "pbs"
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
        "JOEL_PASSWORD_HASH": "pbkdf2_sha256$1$x$y",
        "PATH": "/usr/bin:/bin",
    }
    process = subprocess.Popen(
        [sys.executable, "-m", "datasette", "serve", *(str(directory / f"{db}.db") for db in ("cylc", "scan", "cmt", "queue")), "--create",
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
            "scan-writer": token("scan-writer"),
            "queue-writer": token("queue-writer"),
        }
    finally:
        process.terminate()
        process.wait(10)


def sql(monitor: dict, query: str, db: str = "cylc") -> list[dict]:
    status_code, body = request(
        f"{monitor['url']}/{db}.json?{urllib.parse.urlencode({'sql': query, '_shape': 'array'})}",
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
    # The snapshots never become databases (or files) of their own.
    assert {p.name for p in monitor["db"].parent.iterdir()} <= {
        f"{db}.db{suffix}" for db in ("cylc", "scan", "cmt", "queue") for suffix in ("", "-wal", "-shm")
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


def test_auto_scan_upload(monitor: dict, tmp_path: Path) -> None:
    scan, cmt = tmp_path / "scan-upload.db", tmp_path / "cmt-upload.db"
    make_scan_snapshot(scan)
    make_cmt_snapshot(cmt)
    target = f"{monitor['url']}/-/scan/upload"
    body = gzip.compress(scan.read_bytes())
    for actor in (None, "reader", "writer"):  # only the scan-writer may upload
        assert request(target, monitor.get(actor or ""), body)[0] == 403
    assert request(target, monitor["scan-writer"], b"not sqlite")[0] == 400
    assert request(f"{target}/cylc", monitor["scan-writer"], body)[0] == 404

    status_code, reply = request(target, monitor["scan-writer"], body)
    assert status_code == 200 and json.loads(reply)["rows"]["events"] == 6
    status_code, reply = request(f"{target}/cmt", monitor["scan-writer"], gzip.compress(cmt.read_bytes()))
    assert status_code == 200 and json.loads(reply)["rows"]["runs"] == 6

    assert sql(monitor, "select count(*) as n from event_summary", "scan") == [{"n": 6}]
    assert sql(monitor, "select count(*) as n from cmt_event_summary", "cmt") == [{"n": 2}]
    for board in ("scan", "cmt"):
        status_code, page = request(f"{monitor['url']}/-/dashboards/{board}", monitor["reader"])
        assert status_code == 200, page


def test_queue_permissions(monitor: dict) -> None:
    jobs = f"{monitor['url']}/queue/jobs"
    row = json.dumps({"row": {"event_id": "perm1"}}).encode()

    def post(path: str, actor: str | None, body: bytes) -> int:
        headers = {"Content-Type": "application/json"} | (
            {"Authorization": f"Bearer {monitor[actor]}"} if actor else {}
        )
        try:
            with urllib.request.urlopen(urllib.request.Request(path, data=body, headers=headers), timeout=10) as r:
                return r.status
        except urllib.error.HTTPError as error:
            return error.code

    for actor in (None, "reader", "writer", "scan-writer"):  # only the queue-writer may insert
        assert post(f"{jobs}/-/insert", actor, row) == 403
    assert post(f"{jobs}/-/insert", "queue-writer", row) == 201
    job = sql(monitor, "select max(id) as id from jobs where event_id = 'perm1'", "queue")[0]["id"]
    assert post(f"{jobs}/{job}/-/update", "reader", b'{"update": {"status": "x"}}') == 403
    assert post(f"{jobs}/{job}/-/delete", "queue-writer", b"{}") == 403
    assert post(f"{jobs}/{job}/-/update", "queue-writer", b'{"update": {"status": "cancelled"}}') == 200
    assert (
        post(f"{monitor['url']}/queue/-/create", "queue-writer", b'{"table": "t", "columns": [{"name": "a"}]}') == 403
    )
    # The queue-writer reads the queue, and nothing else; the reader reads it too.
    assert request(f"{jobs}.json", monitor["queue-writer"])[0] == 200
    assert request(f"{monitor['url']}/cylc/jobs.json", monitor["queue-writer"])[0] == 403
    assert request(f"{monitor['url']}/cylc.json?sql=select+1", monitor["queue-writer"])[0] == 403
    assert request(f"{jobs}.json", monitor["reader"])[0] == 200


def test_queue_round_trip(monitor: dict, tmp_path: Path, capsys: pytest.CaptureFixture) -> None:
    q = queue.connect(url=monitor["url"], token=monitor["queue-writer"])
    cmt = {"Mw": 5.2, "strike1": 30}
    first = q.submit("2026p000001", cmt)
    assert (first["event_id"], first["status"], first["cmt"], first["task_id"]) == ("2026p000001", "queued", cmt, None)
    second = q.submit("2026p000002", {"Mw": 4.5})
    assert second["id"] > first["id"]
    assert q.get(10**9) is None
    assert q.latest("nope") is None

    claimed = q.claim("realtime/run1//1")
    assert claimed is not None
    assert (claimed["id"], claimed["status"], claimed["task_id"]) == (first["id"], "running", "realtime/run1//1")
    stored = q.get(first["id"])
    assert stored is not None
    assert (stored["status"], stored["task_id"]) == ("running", "realtime/run1//1")
    assert stored["updated"] >= stored["created"]

    q.update(first["id"], status="succeeded", detail="https://simdb/x")
    assert (q.get(first["id"]) or {}).get("detail") == "https://simdb/x"
    assert [j["id"] for j in q.jobs(status="queued")] == [second["id"]]

    # Submitting the event again, with a new CMT, queues it again: a new job, behind the
    # one already queued, and the first stays as its history.
    again = q.submit("2026p000001", {"Mw": 5.3})
    assert again["id"] > second["id"] and again["status"] == "queued"
    assert [j["status"] for j in q.jobs(event_id="2026p000001")] == ["succeeded", "queued"]
    assert (q.latest("2026p000001") or {}).get("cmt") == {"Mw": 5.3}
    assert (q.claim() or {}).get("id") == second["id"]
    assert (q.claim() or {}).get("cmt") == {"Mw": 5.3}
    assert q.claim() is None

    token_file = tmp_path / "queue-token"
    token_file.write_text(monitor["queue-writer"])
    cli = ["--url", monitor["url"], "--token-file", str(token_file)]
    cmt_file = tmp_path / "cmt.json"
    cmt_file.write_text(json.dumps(cmt))
    assert queue.main([*cli, "submit", "2026p000003", "--cmt", str(cmt_file), "detail=from the cli"]) == 0
    job = json.loads(capsys.readouterr().out)
    assert job["detail"] == "from the cli"
    assert queue.main([*cli, "claim", "--task-id", "realtime/run1//3"]) == 0
    assert json.loads(capsys.readouterr().out)["cmt"] == cmt
    assert queue.main([*cli, "set", str(job["id"]), "status=failed"]) == 0
    assert queue.main([*cli, "get", str(job["id"])]) == 0
    assert json.loads(capsys.readouterr().out)["status"] == "failed"
    assert queue.main([*cli, "latest", "2026p000003"]) == 0
    assert json.loads(capsys.readouterr().out)["id"] == job["id"]
    assert queue.main([*cli, "list", "--event", "2026p000003"]) == 0
    assert [j["id"] for j in json.loads(capsys.readouterr().out)] == [job["id"]]
    assert queue.main([*cli, "claim"]) == 1


def as_person(monitor: dict, person: str, path: str) -> int:
    """GET path with the cookie /-/login sets once a password is right: the actor, signed."""
    cookie = Datasette(secret=SECRET).sign({"a": {"id": person}}, "actor")
    headers = {"Cookie": f"ds_actor={cookie}"}
    try:
        with urllib.request.urlopen(urllib.request.Request(monitor["url"] + path, headers=headers), timeout=10) as r:
            return r.status
    except urllib.error.HTTPError as error:
        return error.code


@pytest.mark.parametrize("person", ["admin", "joel"])
def test_logged_in_people_see_everything(monitor: dict, person: str) -> None:
    for path in ("/", "/-/dashboards/cylc", "/-/dashboards/scan", "/-/dashboards/cmt", "/queue/jobs"):
        assert as_person(monitor, person, path) == 200, path


def test_a_stranger_with_a_signed_cookie_sees_nothing(monitor: dict) -> None:
    assert as_person(monitor, "mallory", "/-/dashboards/cylc") == 403
