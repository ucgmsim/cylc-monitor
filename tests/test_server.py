"""End to end: Datasette 1.0a41 with server/'s config, plugin and template on a temp
database, written to by the client and read with the reader token.

Skipped unless datasette is importable (it is in the dev group: `uv run pytest`).
"""

from __future__ import annotations

import datetime as dt
import json
import socket
import sqlite3
import subprocess
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path

import pytest

pytest.importorskip("datasette")
yaml = pytest.importorskip("yaml")

import cylc_monitor_event as event  # noqa: E402
import cylc_monitor_status as status  # noqa: E402
import make_token  # noqa: E402

SERVER = Path(__file__).resolve().parent.parent / "server"
SECRET = "test-secret"


def free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def request(url: str, token: str | None = None, payload: dict | None = None) -> tuple[int, bytes]:
    headers = {"Authorization": f"Bearer {token}"} if token else {}
    data = None
    if payload is not None:
        data = json.dumps(payload).encode()
        headers["Content-Type"] = "application/json"
    try:
        with urllib.request.urlopen(urllib.request.Request(url, data=data, headers=headers), timeout=10) as response:
            return response.status, response.read()
    except urllib.error.HTTPError as error:
        return error.code, error.read()


def token(kind: str) -> str:
    command = [sys.executable, "-m", "datasette", "create-token", *make_token.TOKENS[kind], "--secret", SECRET]
    return subprocess.run(command, check=True, capture_output=True, text=True).stdout.strip()


@pytest.fixture(scope="module")
def monitor(tmp_path_factory: pytest.TempPathFactory):
    directory = tmp_path_factory.mktemp("monitor")
    db = directory / "cylc.db"
    connection = sqlite3.connect(db)
    connection.executescript((SERVER / "schema.sql").read_text())
    connection.close()
    # The shipped config, with a column-folding rule to test.
    config = yaml.safe_load((SERVER / "datasette.yaml").read_text())
    config["plugins"]["cylc-status"]["columns"] = [{"match": r"sim_(\w+)", "column": "sim"}]
    (directory / "datasette.json").write_text(json.dumps(config))
    port = free_port()
    env = {"DATASETTE_SECRET": SECRET, "ADMIN_PASSWORD_HASH": "pbkdf2_sha256$1$x$y", "PATH": "/usr/bin:/bin"}
    process = subprocess.Popen(
        [sys.executable, "-m", "datasette", "serve", str(db), "--config", str(directory / "datasette.json"),
         "--plugins-dir", str(SERVER / "plugins"), "--template-dir", str(SERVER / "templates"),
         "--host", "127.0.0.1", "--port", str(port)],
        env=env, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
    )  # fmt: skip
    url = f"http://127.0.0.1:{port}"
    try:
        for _ in range(100):
            try:
                request(f"{url}/-/cylc")
                break
            except OSError:
                if process.poll() is not None:
                    pytest.fail(process.stdout.read().decode())
                time.sleep(0.1)
        writer_file = directory / "writer.token"
        writer_file.write_text(token("writer"))
        reader_file = directory / "reader.token"
        reader_file.write_text(token("reader"))
        yield {"url": url, "writer": writer_file, "reader": reader_file}
    finally:
        process.terminate()
        process.wait(10)


def read(path: Path) -> str:
    return path.read_text().strip()


def ago(seconds: int) -> str:
    return (dt.datetime.now(dt.timezone.utc) - dt.timedelta(seconds=seconds)).strftime("%Y-%m-%dT%H:%M:%SZ")


def test_anonymous_visitors_get_nothing(monitor) -> None:
    url = monitor["url"]
    for path in ("/-/cylc", "/-/cylc.json", "/cylc/events.json", "/cylc/heartbeats.json"):
        assert request(url + path)[0] == 403, path
    assert request(f"{url}/cylc/events/-/insert", payload={"row": {"workflow": "x", "event": "y"}})[0] == 403


def test_the_writer_may_insert_and_upsert_but_not_read(monitor) -> None:
    url, writer = monitor["url"], read(monitor["writer"])
    code, body = request(f"{url}/cylc/events/-/insert", writer, {"row": {"workflow": "w-write", "event": "startup"}})
    assert code == 201, body
    beat = {"workflow": "w-write", "seen_at": ago(0), "status": "running", "interval_s": 300}
    assert request(f"{url}/cylc/heartbeats/-/upsert", writer, {"rows": [beat]})[0] == 200
    assert request(f"{url}/cylc/heartbeats/-/upsert", writer, {"rows": [{**beat, "status": "stopped"}]})[0] == 200
    for path in ("/-/cylc.json", "/-/cylc", "/cylc/events.json", "/cylc/heartbeats.json", "/cylc/-/query.json?sql=select+1"):
        assert request(url + path, writer)[0] == 403, path
    # Nothing beyond inserting and upserting: no deleting, no dropping, no other tables.
    assert request(f"{url}/cylc/events/1/-/delete", writer, {})[0] == 403
    assert request(f"{url}/cylc/events/-/drop", writer, {"confirm": True})[0] == 403
    assert request(f"{url}/cylc/-/create", writer, {"table": "other", "row": {"a": 1}})[0] == 403


def test_the_reader_may_read_but_not_write(monitor) -> None:
    url, reader = monitor["url"], read(monitor["reader"])
    code, body = request(f"{url}/-/cylc.json", reader)
    assert code == 200 and "workflows" in json.loads(body)
    assert request(f"{url}/-/cylc", reader)[0] == 200
    assert request(f"{url}/cylc/task_states.json", reader)[0] == 200
    code, body = request(f"{url}/cylc/-/query.json?sql=select+count(*)+as+n+from+events", reader)
    assert code == 200, body
    assert request(f"{url}/cylc/events/-/insert", reader, {"row": {"workflow": "x", "event": "y"}})[0] == 403
    assert request(f"{url}/cylc/heartbeats/-/upsert", reader, {"rows": [{"workflow": "x", "seen_at": ago(0), "status": "running"}]})[0] == 403


def test_the_client_posts_and_the_page_shows_it(monitor) -> None:
    url, writer = monitor["url"], str(monitor["writer"])
    common = ["--url", url, "--token-file", writer, "--workflow", "w-client", "--labels", "first:second"]
    event.main(["--event", "startup", *common])
    for task, point, ev in [("prepare", "1", "succeeded"), ("sim_first", "1", "started"), ("sim_second", "2", "submitted"),
                            ("prepare", "2", "failed")]:  # fmt: skip
        event.main(["--event", ev, "--task", task, "--point", point, "--job-id", "42", "--try-num", "1", *common])
    state = status.fetch(url, read(monitor["reader"]), "w-client")
    (w,) = state["workflows"]
    assert w["columns"] == ["prepare", "sim"]
    by_cycle = {c["cycle"]: c for c in w["cycles"]}
    assert by_cycle["1"]["label"] == "first" and by_cycle["2"]["label"] == "second"
    assert by_cycle["1"]["cells"]["sim"]["task"] == "sim_first" and by_cycle["1"]["cells"]["sim"]["state"] == "running"
    assert by_cycle["2"]["cells"]["sim"]["state"] == "submitted"
    assert [(p["task"], p["state"]) for p in w["problems"]] == [("prepare", "failed")]
    assert w["counts"] == {"succeeded": 1, "running": 1, "submitted": 1, "failed": 1}
    lines, trouble = status.report(state)
    assert trouble and any("FAILED" in line and "prepare" in line for line in lines)
    # A shutdown marks the heartbeat stopped.
    event.main(["--event", "shutdown", *common])
    (w,) = status.fetch(url, read(monitor["reader"]), "w-client")["workflows"]
    assert w["health"] == "stopped"


def test_the_four_heartbeat_states(monitor) -> None:
    url, writer = monitor["url"], read(monitor["writer"])
    beats = [
        {"workflow": "h-alive", "seen_at": ago(10), "status": "running", "interval_s": 300},
        {"workflow": "h-silent", "seen_at": ago(3 * 300 + 60), "status": "running", "interval_s": 300},
        {"workflow": "h-stopped", "seen_at": ago(10), "status": "stopped"},
        {"workflow": "h-gone", "seen_at": ago(10), "status": "scheduler gone", "interval_s": 300},
    ]
    assert request(f"{url}/cylc/heartbeats/-/upsert", writer, {"rows": beats})[0] == 200
    workflows = status.fetch(url, read(monitor["reader"]))["workflows"]
    health = {w["workflow"]: w["health"] for w in workflows if w["workflow"].startswith("h-")}
    assert health == {"h-alive": "alive", "h-silent": "silent", "h-stopped": "stopped", "h-gone": "scheduler gone"}
    code, page = request(f"{url}/-/cylc", read(monitor["reader"]))
    assert code == 200 and b"scheduler gone" in page and b"No heartbeat for" in page
