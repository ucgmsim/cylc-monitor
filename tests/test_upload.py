"""The uploader's own parts: Slurm and PBS directives, and the snapshot of a run directory.

tests/data/run is a real Cylc 8.7.0 run (two cycles of prep => sim & flaky, flaky failing
its first try; sim on a Slurm platform asking for --ntasks=4 --cpus-per-task=2
--mem-per-cpu=500M --time=00:10:00): its log/db and job scripts.

tests/data/pbs holds the same run's sim job scripts as Cylc 8.7's PBS job runner would
write them (synthesised: the #SBATCH block swapped for #PBS lines in pbs.py's order). Laid
over tests/data/run, it makes a PBS run:
    1/sim  a whole-node job: -l walltime=600 (Cylc writes its execution time limit in
           seconds), -l select=2:ncpus=384:mpiprocs=48:ompthreads=4:mem=700gb,place=scatter:excl,
           -q high_mem_shortq, -P test0001;
    2/sim  a part-node job: -l walltime=00:10:00, -l select=1:ncpus=8:mem=4000mb, -A test0001.
"""

from __future__ import annotations

import shutil
import sqlite3
from pathlib import Path

import pytest

from cylc_monitor import upload

RUN = Path(__file__).resolve().parent / "data" / "run"
PBS = Path(__file__).resolve().parent / "data" / "pbs"


def pbs_run(tmp_path: Path) -> Path:
    """Copy tests/data/run with tests/data/pbs's job scripts laid over it."""
    run = tmp_path / "pbs-run"
    shutil.copytree(RUN, run)
    shutil.copytree(PBS, run, dirs_exist_ok=True)
    return run


def pbs_script(tmp_path: Path, *lines: str) -> Path:
    script = tmp_path / "job"
    script.write_text("#!/bin/bash -l\n# DIRECTIVES:\n" + "".join(f"#PBS {line}\n" for line in lines) + "echo\n")
    return script


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
        "#SBATCH --time 08:00:00\n#SBATCH -A abc12345\n\necho\n#SBATCH --ntasks=1\n"
    )
    r = upload.resources(script)
    assert (
        r["cores"],
        r["ntasks"],
        r["mem_mb"],
        r["time_limit_min"],
        r["account"],
    ) == (820, 820, 820 * 1900, 480, "abc12345")


def test_resources_memory_per_node(tmp_path: Path) -> None:
    script = tmp_path / "job"
    script.write_text("#SBATCH --nodes=2\n#SBATCH --ntasks-per-node=4\n#SBATCH --mem=8G\n")
    r = upload.resources(script)
    assert (r["nodes"], r["mem_mb"], r["cores"]) == (2, 16384, None)


def test_resources_none_for_a_background_job() -> None:
    r = upload.resources(RUN / "log" / "job" / "1" / "prep" / "01" / "job")
    assert all(r[k] is None for k in ("cores", "mem_mb", "time_limit_min", "directives"))


def test_slurm_jobs_say_slurm() -> None:
    r = upload.resources(RUN / "log" / "job" / "1" / "sim" / "01" / "job")
    assert r == {
        "cores": 8,
        "ntasks": 4,
        "cpus_per_task": 2,
        "nodes": None,
        "mem_mb": 4000.0,
        "time_limit_min": 10.0,
        "account": "test0001",
        "partition": None,
        "directives": "--job-name=sim.1.monlab/run2 --output=cylc-run/monlab/run2/log/job/1/sim/01/job.out "
        "--error=cylc-run/monlab/run2/log/job/1/sim/01/job.err --ntasks=4 --cpus-per-task=2 --mem-per-cpu=500M "
        "--time=00:10:00 --account=test0001",
        "runner": "slurm",
        "chunks": None,
    }
    assert upload.resources(RUN / "log" / "job" / "1" / "prep" / "01" / "job")["runner"] is None


@pytest.mark.parametrize(
    ("value", "expected"),
    [("600", 10), ("90.5", 90.5 / 60), ("10:30", 10.5), ("02:00:00", 120), ("48:00:00", 2880), ("1-00", None)],
)
def test_pbs_minutes(value: str, expected: float | None) -> None:
    assert upload.pbs_minutes(value) == (None if expected is None else pytest.approx(expected))


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        ("700gb", 700 * 1024),
        ("700GB", 700 * 1024),
        ("4000mb", 4000),
        ("2048kb", 2),
        ("1tb", 1024 * 1024),
        ("1048576", 1),  # bytes, PBS's default unit
        ("1048576b", 1),
        ("4g", None),  # PBS wants the b
        ("8kw", None),  # words
    ],
)
def test_pbs_megabytes(value: str, expected: float | None) -> None:
    assert upload.pbs_megabytes(value) == expected


def test_pbs_whole_node_job() -> None:
    r = upload.resources(PBS / "log" / "job" / "1" / "sim" / "01" / "job")
    assert r == {
        "cores": 2 * 384,  # ncpus as PBS counts them: both SMT siblings on Cascade
        "ntasks": 2 * 48,
        "cpus_per_task": 4,
        "nodes": 2,
        "mem_mb": 2 * 700 * 1024,
        "time_limit_min": 10,
        "account": "test0001",
        "partition": "high_mem_shortq",
        "directives": "-N sim.1.monlab-run2 -o cylc-run/monlab/run2/log/job/1/sim/01/job.out "
        "-e cylc-run/monlab/run2/log/job/1/sim/01/job.err -l walltime=600 "
        "-l select=2:ncpus=384:mpiprocs=48:ompthreads=4:mem=700gb,place=scatter:excl -q high_mem_shortq -P test0001",
        "runner": "pbs",
        "chunks": 2,
    }


def test_pbs_part_node_job() -> None:
    r = upload.resources(PBS / "log" / "job" / "2" / "sim" / "01" / "job")
    assert (r["cores"], r["ntasks"], r["cpus_per_task"], r["nodes"], r["chunks"]) == (8, None, None, 1, 1)
    assert (r["mem_mb"], r["time_limit_min"], r["account"], r["partition"]) == (4000, 10, "test0001", None)


def test_pbs_multi_chunk_select(tmp_path: Path) -> None:
    script = pbs_script(tmp_path, "-l select=2:ncpus=8:mpiprocs=8:mem=16gb+1:ncpus=4:mpiprocs=1:ompthreads=4:mem=8gb")
    r = upload.resources(script)
    assert (r["cores"], r["ntasks"], r["chunks"], r["mem_mb"]) == (20, 17, 3, (2 * 16 + 8) * 1024)
    # Chunks may share a host unless placed apart, and threads differ between chunks.
    assert (r["nodes"], r["cpus_per_task"]) == (None, None)


def test_pbs_select_without_a_count_or_ncpus(tmp_path: Path) -> None:
    r = upload.resources(pbs_script(tmp_path, "-l select=mem=2gb+3:mem=1gb", "-l place=pack"))
    assert (r["cores"], r["chunks"], r["nodes"], r["mem_mb"]) == (4, 4, 1, 5 * 1024)


def test_pbs_memory_unknown_unless_every_chunk_says(tmp_path: Path) -> None:
    r = upload.resources(pbs_script(tmp_path, "-l select=1:ncpus=4:mem=2gb+1:ncpus=4"))
    assert (r["cores"], r["mem_mb"]) == (8, None)


def test_pbs_several_options_and_lines(tmp_path: Path) -> None:
    script = pbs_script(
        tmp_path,
        "-V",
        "-q workq -l walltime=1:30:00",
        "-l select=4:ncpus=2",
        "-l place=scatter:excl,mem=1gb",
        "-A acct -P proj",
    )
    r = upload.resources(script)
    assert (r["cores"], r["nodes"], r["time_limit_min"], r["partition"], r["account"]) == (8, 4, 90, "workq", "acct")
    assert r["mem_mb"] == 1024  # the select gives no mem, so the job-wide one
    assert r["directives"] == (
        "-V -q workq -l walltime=1:30:00 -l select=4:ncpus=2 -l place=scatter:excl,mem=1gb -A acct -P proj"
    )


def test_pbs_job_wide_and_torque_resources(tmp_path: Path) -> None:
    r = upload.resources(pbs_script(tmp_path, "-l ncpus=16,mem=32gb", "-P proj"))
    assert (r["cores"], r["mem_mb"], r["nodes"], r["account"]) == (16, 32 * 1024, None, "proj")
    r = upload.resources(pbs_script(tmp_path, "-l nodes=2:ppn=12", "-l mem=4gb"))
    assert (r["cores"], r["nodes"], r["chunks"], r["mem_mb"]) == (24, 2, 2, 4096)


def test_pbs_nonsense_select_says_nothing(tmp_path: Path) -> None:
    r = upload.resources(pbs_script(tmp_path, "-l select=two:ncpus=4"))
    assert (r["cores"], r["chunks"], r["runner"]) == (None, None, "pbs")


def test_runner_from_the_first_directive(tmp_path: Path) -> None:
    script = tmp_path / "job"
    script.write_text("#SBATCH --ntasks=2\n#PBS -l select=1:ncpus=64\n")
    assert (upload.resources(script)["runner"], upload.resources(script)["cores"]) == ("slurm", 2)
    script.write_text("#PBS -l select=1:ncpus=64\n#SBATCH --ntasks=2\n")
    assert (upload.resources(script)["runner"], upload.resources(script)["cores"]) == ("pbs", 64)
    assert upload.directives(script) == {}


def test_pbs_snapshot(tmp_path: Path) -> None:
    path = tmp_path / "snapshot.db"
    upload.Snapshot(pbs_run(tmp_path), "monlab/pbs", 300).build(path, "stopped", "host", None)
    rows = sqlite3.connect(path).execute(
        "select cycle, runner, cores, chunks, nodes, mem_mb, time_limit_min, partition from job_resources"
        " where name = 'sim' order by cycle"
    )
    assert rows.fetchall() == [
        ("1", "pbs", 768, 2, 2, 2 * 700 * 1024.0, 10.0, "high_mem_shortq"),
        ("2", "pbs", 8, 1, 1, 4000.0, 10.0, None),
    ]


def test_snapshot(tmp_path: Path) -> None:
    run = tmp_path / "run"
    shutil.copytree(RUN, run)
    path = tmp_path / "snapshot.db"
    upload.Snapshot(run, "monlab/run2", 300).build(path, "stopped", "host", None)
    conn = sqlite3.connect(path)
    tables = {r[0] for r in conn.execute("select name from sqlite_master where type = 'table'")}
    assert {
        "task_jobs",
        "task_events",
        "monitor",
        "job_resources",
        "job_logs",
    } <= tables
    assert conn.execute("select workflow, status, interval_s from monitor").fetchall() == [
        ("monlab/run2", "stopped", 300)
    ]
    sim = conn.execute(
        "select cores, ntasks, cpus_per_task, mem_mb, time_limit_min, account from job_resources where name = 'sim'"
    )
    assert sim.fetchall() == [(8, 4, 2, 4000.0, 10.0, "test0001")] * 2
    logs = conn.execute("select cycle, name, submit_num, job_err from job_logs order by cycle").fetchall()
    assert [row[:3] for row in logs] == [("1", "flaky", 1), ("2", "flaky", 1)]
    assert "flaky: first try fails" in logs[0][3]
    # The run's own database is untouched.
    assert "monitor" not in {
        r[0] for r in sqlite3.connect(run / "log" / "db").execute("select name from sqlite_master")
    }


def test_no_url_or_token_uploads_nothing(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("CYLC_MONITOR_URL", raising=False)
    monkeypatch.setattr(upload, "URL_FILE", str(tmp_path / "no-url"))
    calls = []
    monkeypatch.setattr(upload, "upload", lambda *a: calls.append(a))
    assert upload.main(["--workflow", "w", "--run-dir", str(RUN)]) == 1
    args = ["--workflow", "w", "--run-dir", str(RUN), "--url", "http://x", "--token-file", str(tmp_path / "none")]
    assert upload.main(args) == 1
    assert calls == []


def test_upload_by_hand(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    token = tmp_path / "token"
    token.write_text("t")
    calls = []
    monkeypatch.setattr(upload, "upload", lambda *a: calls.append(a[:3]))
    args = ["--workflow", "w", "--run-dir", str(RUN), "--url", "http://x", "--token-file", str(token)]
    assert upload.main(args) == 0
    assert calls == [("http://x", "t", "w")]
