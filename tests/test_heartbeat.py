from __future__ import annotations

import os
import socket
import subprocess
from pathlib import Path

import cylc_monitor_heartbeat as heartbeat


def contact(run_dir: Path, pid: int, host: str | None = None) -> None:
    (run_dir / ".service").mkdir(parents=True, exist_ok=True)
    (run_dir / ".service" / "contact").write_text(f"CYLC_WORKFLOW_PID={pid}\nCYLC_WORKFLOW_HOST={host or socket.gethostname()}\n")


def test_a_live_scheduler_is_running(tmp_path: Path) -> None:
    contact(tmp_path, os.getpid())
    assert heartbeat.status(tmp_path, socket.gethostname()) == ("running", os.getpid())


def test_no_contact_file_is_a_clean_stop(tmp_path: Path) -> None:
    assert heartbeat.status(tmp_path, socket.gethostname()) == ("stopped", None)


def test_a_dead_scheduler_process_is_gone(tmp_path: Path) -> None:
    process = subprocess.Popen(["true"])
    process.wait()
    contact(tmp_path, process.pid)
    assert heartbeat.status(tmp_path, socket.gethostname()) == ("scheduler gone", process.pid)


def test_another_hosts_scheduler_is_not_checked_here(tmp_path: Path) -> None:
    contact(tmp_path, 999999, host="somewhere-else.example")
    assert heartbeat.status(tmp_path, socket.gethostname())[0] == "running"


def test_posts_once_and_exits_when_the_scheduler_has_stopped(tmp_path: Path, monkeypatch) -> None:
    posted = []
    monkeypatch.setattr(heartbeat, "post", lambda url, path, payload, token_file=None: posted.append((url, path, payload)))
    heartbeat.main(["--url", "https://monitor.example.org", "--workflow", "wf", "--run-dir", str(tmp_path)])
    assert [(u, p) for u, p, _ in posted] == [("https://monitor.example.org", "heartbeats/-/upsert")]
    assert posted[0][2]["rows"][0]["status"] == "stopped"
    assert (tmp_path / "log" / heartbeat.PID_FILE).read_text() == str(os.getpid())


def test_a_second_heartbeat_for_the_run_exits(tmp_path: Path, monkeypatch) -> None:
    (tmp_path / "log").mkdir()
    (tmp_path / "log" / heartbeat.PID_FILE).write_text(str(os.getpid()))
    posted = []
    monkeypatch.setattr(heartbeat, "post", lambda *a, **k: posted.append(a))
    heartbeat.main(["--url", "https://monitor.example.org", "--workflow", "wf", "--run-dir", str(tmp_path)])
    assert posted == []
