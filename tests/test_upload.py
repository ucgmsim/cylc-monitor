"""The uploader's own parts: Slurm directives, and the snapshot of a run directory.

tests/data/run is a real Cylc 8.7.0 run (two cycles of prep => sim & flaky, flaky failing
its first try; sim on a Slurm platform asking for --ntasks=4 --cpus-per-task=2
--mem-per-cpu=500M --time=00:10:00): its log/db and job scripts.
"""

from __future__ import annotations

import shutil
import sqlite3
from pathlib import Path

import cylc_monitor_upload as upload
import pytest

RUN = Path(__file__).resolve().parent / "data" / "run"


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        ("10", 10),
        ("1:30", 1.5),
        ("02:00:00", 120),
        ("1-00", 1440),
        ("1-02:30:00", 1590),
        ("2-0:0:30", 2880.5),
    ],
)
def test_minutes(value: str, expected: float) -> None:
    assert upload.minutes(value) == pytest.approx(expected)


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        ("500", 500),
        ("500M", 500),
        ("2G", 2048),
        ("1.5GB", 1536),
        ("512K", 0.5),
        ("x", None),
    ],
)
def test_megabytes(value: str, expected: float | None) -> None:
    assert upload.megabytes(value) == expected


def test_resources_from_the_header_only(tmp_path: Path) -> None:
    script = tmp_path / "job"
    script.write_text(
        "#!/bin/bash -l\n#SBATCH --job-name=sw4.1\n#SBATCH -n 820\n#SBATCH --mem-per-cpu=1900\n"
        "#SBATCH --time 08:00:00\n#SBATCH -A nesi00213\n\necho\n#SBATCH --ntasks=1\n"
    )
    r = upload.resources(script)
    assert (
        r["cores"],
        r["ntasks"],
        r["mem_mb"],
        r["time_limit_min"],
        r["account"],
    ) == (820, 820, 820 * 1900, 480, "nesi00213")


def test_resources_memory_per_node(tmp_path: Path) -> None:
    script = tmp_path / "job"
    script.write_text(
        "#SBATCH --nodes=2\n#SBATCH --ntasks-per-node=4\n#SBATCH --mem=8G\n"
    )
    r = upload.resources(script)
    assert (r["nodes"], r["mem_mb"], r["cores"]) == (2, 16384, None)


def test_resources_none_for_a_background_job() -> None:
    r = upload.resources(RUN / "log" / "job" / "1" / "prep" / "01" / "job")
    assert all(
        r[k] is None for k in ("cores", "mem_mb", "time_limit_min", "directives")
    )


def test_snapshot(tmp_path: Path) -> None:
    run = tmp_path / "run"
    shutil.copytree(RUN, run)
    path = tmp_path / "snapshot.db"
    upload.Snapshot(run, "monlab/run2", 300).build(path, "stopped", "host", None)
    conn = sqlite3.connect(path)
    tables = {
        r[0]
        for r in conn.execute("select name from sqlite_master where type = 'table'")
    }
    assert {
        "task_jobs",
        "task_events",
        "monitor",
        "job_resources",
        "job_logs",
    } <= tables
    assert conn.execute(
        "select workflow, status, interval_s from monitor"
    ).fetchall() == [("monlab/run2", "stopped", 300)]
    sim = conn.execute(
        "select cores, ntasks, cpus_per_task, mem_mb, time_limit_min, account from job_resources where name = 'sim'"
    )
    assert sim.fetchall() == [(8, 4, 2, 4000.0, 10.0, "test0001")] * 2
    logs = conn.execute(
        "select cycle, name, submit_num, job_err from job_logs order by cycle"
    ).fetchall()
    assert [row[:3] for row in logs] == [("1", "flaky", 1), ("2", "flaky", 1)]
    assert "flaky: first try fails" in logs[0][3]
    # The run's own database is untouched.
    assert "monitor" not in {
        r[0]
        for r in sqlite3.connect(run / "log" / "db").execute(
            "select name from sqlite_master"
        )
    }


def test_no_url_or_token_uploads_nothing(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.delenv("CYLC_MONITOR_URL", raising=False)
    calls = []
    monkeypatch.setattr(upload, "upload", lambda *a: calls.append(a))
    upload.main(["--workflow", "w", "--run-dir", str(RUN), "--once"])
    upload.main(
        [
            "--workflow",
            "w",
            "--run-dir",
            str(RUN),
            "--once",
            "--url",
            "http://x",
            "--token-file",
            str(tmp_path / "none"),
        ]
    )
    assert calls == []
