from __future__ import annotations

import json
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path

import cylc_monitor_event as event


def args(*extra: str):
    return event.parse_args(["--event", "submitted", "--workflow", "wf", *extra])


def test_a_task_event_is_a_row() -> None:
    row = event.event_row(args("--event", "started", "--task", "model", "--point", "6", "--job-id", "123", "--try-num", "1", "--submit-num", "1"))
    assert row["event"] == "started" and row["task"] == "model" and row["cycle"] == "6"
    assert row["job_id"] == "123" and row["try_num"] == 1 and row["submit_num"] == 1
    assert row["message"] is None and row["label"] is None and row["log_tail"] is None


def test_cylc_none_counts_as_not_given() -> None:
    row = event.event_row(args("--task", "model", "--point", "1", "--job-id", "None", "--try-num", "None"))
    assert row["job_id"] is None and row["try_num"] is None


def test_label_as_given() -> None:
    assert event.event_row(args("--task", "t", "--point", "20260101T00Z", "--label", "January"))["label"] == "January"


def test_labels_name_integer_cycle_points() -> None:
    assert event.label(args("--task", "t", "--point", "2", "--labels", "alpha:beta:gamma")) == "beta"
    assert event.label(args("--task", "t", "--point", "9", "--labels", "alpha:beta")) is None
    assert event.label(args("--task", "t", "--point", "0", "--labels", "alpha:beta")) is None
    assert event.label(args("--task", "t", "--point", "2026", "--labels", "alpha")) is None


def test_message_as_given_overrides_the_default() -> None:
    assert event.message(args("--event", "failed", "--task", "t", "--message", "it broke")) == "it broke"


def test_failure_carries_the_job_error(tmp_path: Path, monkeypatch) -> None:
    job_dir = tmp_path / "log" / "job" / "4" / "render" / "02"
    job_dir.mkdir(parents=True)
    (job_dir / "job.err").write_text("rendering\nMemoryError\n")
    monkeypatch.setenv("CYLC_WORKFLOW_RUN_DIR", str(tmp_path))
    row = event.event_row(args("--event", "failed", "--task", "render", "--point", "4", "--submit-num", "2"))
    assert row["message"] == "render failed"
    assert "MemoryError" in row["log_tail"]
    retry = event.event_row(args("--event", "retry", "--task", "render", "--point", "4", "--submit-num", "2"))
    assert retry["message"] == "render failed, retrying" and "MemoryError" in retry["log_tail"]


def test_success_carries_no_job_error(tmp_path: Path, monkeypatch) -> None:
    job_dir = tmp_path / "log" / "job" / "1" / "t" / "01"
    job_dir.mkdir(parents=True)
    (job_dir / "job.err").write_text("warning\n")
    monkeypatch.setenv("CYLC_WORKFLOW_RUN_DIR", str(tmp_path))
    assert event.event_row(args("--event", "succeeded", "--task", "t", "--point", "1", "--submit-num", "1"))["log_tail"] is None


def test_workflow_events_have_no_cycle() -> None:
    row = event.event_row(args("--event", "stall", "--point", "3"))
    assert row["message"] == "Workflow stalled" and row["task"] is None and row["cycle"] is None


def test_the_url_may_come_from_the_environment(monkeypatch) -> None:
    monkeypatch.setenv("CYLC_MONITOR_URL", "https://monitor.example.org")
    assert args().url == "https://monitor.example.org"


class Recorder(BaseHTTPRequestHandler):
    seen: list = []

    def do_POST(self) -> None:  # noqa: N802
        body = self.rfile.read(int(self.headers["Content-Length"]))
        Recorder.seen.append((self.path, self.headers["Authorization"], json.loads(body)))
        self.send_response(201)
        self.end_headers()

    def log_message(self, *_) -> None:
        pass


def test_posts_rows_with_the_token_and_marks_a_shutdown_stopped(tmp_path: Path) -> None:
    server = HTTPServer(("127.0.0.1", 0), Recorder)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    token = tmp_path / "token"
    token.write_text("dstok_test\n")
    Recorder.seen = []
    url = f"http://127.0.0.1:{server.server_port}"
    event.main(["--event", "shutdown", "--workflow", "wf", "--url", url, "--token-file", str(token)])
    server.shutdown()
    paths = [p for p, _, _ in Recorder.seen]
    assert paths == ["/cylc/events/-/insert", "/cylc/heartbeats/-/upsert"]
    assert all(auth == "Bearer dstok_test" for _, auth, _ in Recorder.seen)
    assert Recorder.seen[0][2]["row"]["event"] == "shutdown" and Recorder.seen[1][2]["rows"][0]["status"] == "stopped"


def test_the_token_file_may_come_from_the_environment(tmp_path: Path, monkeypatch) -> None:
    token = tmp_path / "token"
    token.write_text("dstok_env\n")
    monkeypatch.setenv("CYLC_MONITOR_TOKEN_FILE", str(token))
    assert event.default_token_file() == token


def test_no_token_posts_nothing(tmp_path: Path, monkeypatch) -> None:
    calls = []
    monkeypatch.setattr(event.urllib.request, "urlopen", lambda *a, **k: calls.append(a))
    assert not event.post("http://127.0.0.1:9", "events/-/insert", {"row": {}}, token_file=tmp_path / "missing")
    assert calls == []


def test_no_url_posts_nothing(tmp_path: Path, monkeypatch) -> None:
    calls = []
    monkeypatch.delenv("CYLC_MONITOR_URL", raising=False)
    monkeypatch.setattr(event, "post", lambda *a, **k: calls.append(a))
    event.main(["--event", "startup", "--workflow", "wf", "--url", "None"])
    assert calls == []


def test_an_unreachable_monitor_is_not_an_error(tmp_path: Path) -> None:
    token = tmp_path / "token"
    token.write_text("dstok_test\n")
    assert not event.post("http://127.0.0.1:9", "events/-/insert", {"row": {}}, token_file=token)
