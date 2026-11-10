#!/usr/bin/env python3
"""Upload a Cylc workflow's own database to the Cylc monitor while the scheduler runs.

    cylc_monitor_upload.py --workflow WORKFLOW [--url URL] [--token-file PATH]
                           [--run-dir DIR] [--check 30] [--heartbeat 300] [--daemon] [--once]

Started by the workflow's startup handler on the scheduler host, so it uses only the
standard library (Python 3.8 or later). It reads the run directory and nothing else:

- every --check seconds it looks at the modification time of the workflow's public
  database (<run dir>/log/db); when the database has changed, or --heartbeat seconds
  have passed since the last upload, it uploads a snapshot;
- the snapshot is a consistent copy of log/db (SQLite's backup API, read-only) plus
  three small tables of its own:
    monitor        one row: the scheduler's state (running, stopped, scheduler gone),
                   host, PID, upload time and the heartbeat interval;
    job_resources  each job's Slurm directives, read from its job script
                   (log/job/<cycle>/<task>/<NN>/job): what the job asked for;
    job_logs       the end of job.err for each job that failed;
- it stops after the upload that sees the scheduler stopped (contact file gone) or gone
  (contact file left, process dead).

The monitor merges each snapshot into its own database, replacing the workflow's
previous one, so an upload is idempotent and a lost one is repaired by the next.

With --daemon it detaches first, so the handler returns at once, and logs to
<run dir>/log/cylc-monitor-upload.log; one daemon a run directory (a second exits if
<run dir>/log/cylc-monitor-upload.pid is alive). --once uploads one snapshot and exits.

The URL defaults to $CYLC_MONITOR_URL; the token, the monitor's cylc-writer token, is read
from --token-file (default $CYLC_MONITOR_TOKEN_FILE, else ~/.config/cylc-monitor/token).
Without a URL or a token nothing is uploaded. It exits 0 whatever happens to an upload.
"""

from __future__ import annotations

import argparse
import datetime as dt
import gzip
import os
import re
import socket
import sqlite3
import tempfile
import time
import urllib.parse
import urllib.request
from pathlib import Path

TOKEN_FILE = "~/.config/cylc-monitor/token"
PID_FILE = "cylc-monitor-upload.pid"
LOG_FILE = "cylc-monitor-upload.log"
DIRECTIVE = re.compile(r"^#SBATCH\s+(--?[\w-]+)(?:[= ]\s*(\S+))?")
# Short Slurm options and the long ones they stand for.
SHORT = {
    "-n": "--ntasks",
    "-c": "--cpus-per-task",
    "-N": "--nodes",
    "-t": "--time",
    "-A": "--account",
    "-p": "--partition",
    "-J": "--job-name",
}


def now() -> str:
    return dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def log(message: str) -> None:
    print(f"{now()} {message}", flush=True)


# --- the scheduler's state -----------------------------------------------------------


def alive(pid: int) -> bool:
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    return True


def scheduler(run_dir: Path, host: str) -> tuple[str, int | None]:
    """running, stopped or scheduler gone, from the contact file; and the scheduler's PID."""
    try:
        text = (run_dir / ".service" / "contact").read_text()
    except OSError:
        return "stopped", None
    info = dict(line.split("=", 1) for line in text.splitlines() if "=" in line)
    try:
        pid = int(info.get("CYLC_WORKFLOW_PID", ""))
    except ValueError:
        return "running", None
    if info.get("CYLC_WORKFLOW_HOST", host).split(".")[0] != host.split(".")[0]:
        return "running", pid  # not this host's process to check
    return ("running" if alive(pid) else "scheduler gone"), pid


# --- what each job asked for -----------------------------------------------------------


def minutes(value: str) -> float | None:
    """A Slurm --time ([D-]HH:MM:SS, MM:SS, MM, D-HH...) in minutes."""
    days, _, clock = value.rpartition("-")
    parts = [float(p) for p in clock.split(":")]
    if days:
        parts += [0.0] * (3 - len(parts))
        h, m, s = parts
        return float(days) * 1440 + h * 60 + m + s / 60
    if len(parts) == 1:
        return parts[0]
    if len(parts) == 2:
        return parts[0] + parts[1] / 60
    h, m, s = parts[:3]
    return h * 60 + m + s / 60


def megabytes(value: str) -> float | None:
    """A Slurm memory size (default unit MB) in MB."""
    match = re.fullmatch(r"(\d+(?:\.\d+)?)([KMGT]?)B?", value.upper())
    if not match:
        return None
    scale = {"K": 1 / 1024, "": 1, "M": 1, "G": 1024, "T": 1024 * 1024}[match.group(2)]
    return float(match.group(1)) * scale


def directives(job_script: Path) -> dict[str, str]:
    """The script's #SBATCH directives, long option -> value."""
    found: dict[str, str] = {}
    try:
        with job_script.open(errors="replace") as lines:
            for line in lines:
                if line.startswith("#SBATCH"):
                    match = DIRECTIVE.match(line.strip())
                    if match:
                        option = SHORT.get(match.group(1), match.group(1))
                        found[option] = match.group(2) or ""
                elif found and line.strip() and not line.startswith("#"):
                    break  # past the header
    except OSError:
        pass
    return found


def resources(job_script: Path) -> dict:
    """Cores, nodes, memory and time a job asked for (None where it didn't say)."""
    d = directives(job_script)

    def integer(key: str) -> int | None:
        try:
            return int(d[key].split("-")[0]) if key in d else None
        except ValueError:
            return None

    ntasks, cpus, nodes = (
        integer("--ntasks"),
        integer("--cpus-per-task"),
        integer("--nodes"),
    )
    mem_per_cpu = megabytes(d["--mem-per-cpu"]) if "--mem-per-cpu" in d else None
    mem_per_node = megabytes(d["--mem"]) if "--mem" in d else None
    cores = (ntasks or 1) * (cpus or 1) if (ntasks or cpus) else None
    if mem_per_cpu is not None and cores is not None:
        mem_mb = mem_per_cpu * cores
    elif mem_per_node is not None:
        mem_mb = mem_per_node * (nodes or 1)
    else:
        mem_mb = None
    time_limit = d.get("--time")
    return {
        "cores": cores,
        "ntasks": ntasks,
        "cpus_per_task": cpus,
        "nodes": nodes,
        "mem_mb": mem_mb,
        "time_limit_min": minutes(time_limit) if time_limit else None,
        "account": d.get("--account"),
        "partition": d.get("--partition"),
        "directives": " ".join(f"{k}={v}" if v else k for k, v in d.items()) or None,
    }


def tail(path: Path, lines: int = 20, max_chars: int = 2000) -> str | None:
    try:
        text = path.read_text(errors="replace")
    except OSError:
        return None
    return "\n".join(text.rstrip().splitlines()[-lines:])[-max_chars:] or None


# --- the snapshot ------------------------------------------------------------------------


class Snapshot:
    """Builds snapshots of one run directory, remembering what it has already parsed."""

    def __init__(self, run_dir: Path, workflow: str, interval: int):
        self.run_dir, self.workflow, self.interval = run_dir, workflow, interval
        self.parsed: dict[tuple[str, str, int], dict] = {}

    def build(self, path: Path, state: str, host: str, pid: int | None) -> None:
        source = sqlite3.connect(
            f"file:{self.run_dir / 'log' / 'db'}?mode=ro", uri=True
        )
        target = sqlite3.connect(path)
        try:
            source.backup(target)
        finally:
            source.close()
        with target:
            jobs = target.execute(
                "select cycle, name, submit_num, run_status, submit_status from task_jobs"
            ).fetchall()
            target.execute(
                "create table monitor (workflow text, status text, host text, pid integer, uploaded_at text, interval_s integer)"
            )
            target.execute(
                "insert into monitor values (?, ?, ?, ?, ?, ?)",
                (self.workflow, state, host, pid, now(), self.interval),
            )
            columns = [
                "cores",
                "ntasks",
                "cpus_per_task",
                "nodes",
                "mem_mb",
                "time_limit_min",
                "account",
                "partition",
                "directives",
            ]
            target.execute(
                f"create table job_resources (cycle text, name text, submit_num integer, {', '.join(columns)})"
            )
            target.execute(
                "create table job_logs (cycle text, name text, submit_num integer, job_err text)"
            )
            for cycle, name, submit, run_status, submit_status in jobs:
                job_dir = (
                    self.run_dir / "log" / "job" / str(cycle) / name / f"{submit:02d}"
                )
                key = (str(cycle), name, submit)
                if key not in self.parsed and (job_dir / "job").exists():
                    self.parsed[key] = resources(job_dir / "job")
                if key in self.parsed:
                    values = [self.parsed[key][c] for c in columns]
                    target.execute(
                        f"insert into job_resources values ({', '.join('?' * (3 + len(columns)))})",
                        [*key, *values],
                    )
                if run_status not in (None, 0) or submit_status not in (None, 0):
                    err = tail(job_dir / "job.err") or tail(
                        job_dir / "job-activity.log"
                    )
                    if err:
                        target.execute(
                            "insert into job_logs values (?, ?, ?, ?)", (*key, err)
                        )
        target.execute("vacuum")
        target.close()


def upload(url: str, token: str, workflow: str, path: Path) -> None:
    body = gzip.compress(path.read_bytes())
    request = urllib.request.Request(
        f"{url.rstrip('/')}/-/cylc/upload?{urllib.parse.urlencode({'workflow': workflow})}",
        data=body,
        method="POST",
        headers={
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/vnd.sqlite3",
            "Content-Encoding": "gzip",
        },
    )
    with urllib.request.urlopen(request, timeout=60) as response:
        response.read()


# --- the daemon --------------------------------------------------------------------------


def daemonise(log_path: Path) -> None:
    """Detach from the event handler: fork twice, new session, stdio to the log."""
    if os.fork():
        os._exit(0)
    os.setsid()
    if os.fork():
        os._exit(0)
    out = os.open(log_path, os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0o644)
    null = os.open(os.devnull, os.O_RDONLY)
    os.dup2(null, 0)
    os.dup2(out, 1)
    os.dup2(out, 2)


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument(
        "--url", "--monitor-url", dest="url", default=os.environ.get("CYLC_MONITOR_URL")
    )
    parser.add_argument(
        "--token-file",
        type=Path,
        default=os.environ.get("CYLC_MONITOR_TOKEN_FILE") or TOKEN_FILE,
    )
    parser.add_argument("--workflow", default=os.environ.get("CYLC_WORKFLOW_ID"))
    parser.add_argument(
        "--run-dir", type=Path, default=os.environ.get("CYLC_WORKFLOW_RUN_DIR")
    )
    parser.add_argument(
        "--check", type=int, default=30, help="seconds between looks at log/db"
    )
    parser.add_argument(
        "--heartbeat",
        type=int,
        default=300,
        help="upload at least this often (seconds)",
    )
    parser.add_argument("--daemon", action="store_true")
    parser.add_argument("--once", action="store_true")
    args = parser.parse_args(argv)
    if not args.url or args.url == "None" or not args.workflow or args.run_dir is None:
        return
    try:
        token = Path(args.token_file).expanduser().read_text().strip()
    except OSError:
        return
    run_dir = Path(args.run_dir)
    host = socket.gethostname()
    snapshot = Snapshot(run_dir, args.workflow, args.heartbeat)
    if not args.once:
        pid_file = run_dir / "log" / PID_FILE
        try:
            if alive(int(pid_file.read_text())):
                return
        except (OSError, ValueError):
            pass
        if args.daemon:
            daemonise(run_dir / "log" / LOG_FILE)
        pid_file.write_text(str(os.getpid()))
    db = run_dir / "log" / "db"
    last_mtime, last_upload = None, 0.0
    with tempfile.TemporaryDirectory(prefix="cylc-monitor-") as tmp:
        path = Path(tmp) / "snapshot.db"
        while True:
            state, pid = scheduler(run_dir, host)
            try:
                mtime = db.stat().st_mtime_ns
            except OSError:
                mtime = None
            due = (
                mtime != last_mtime
                or time.monotonic() - last_upload >= args.heartbeat
                or state != "running"
            )
            if due and mtime is not None:
                try:
                    path.unlink(missing_ok=True)
                    snapshot.build(path, state, host, pid)
                    upload(args.url, token, args.workflow, path)
                    last_mtime, last_upload = mtime, time.monotonic()
                except Exception as error:  # noqa: BLE001 - retried at the next check
                    log(f"upload failed: {error}")
            if args.once or state != "running":
                if not args.once:
                    log(f"scheduler {state}; exiting")
                return
            time.sleep(args.check)


def run() -> None:
    try:
        main()
    except Exception as error:  # noqa: BLE001 - a lost upload must not trouble the workflow
        log(f"cylc-monitor-upload: {error}")


if __name__ == "__main__":
    run()
