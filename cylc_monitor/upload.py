"""Snapshot a Cylc workflow's own database and upload it to the Cylc monitor.

    cylc-monitor-upload --workflow WORKFLOW --run-dir DIR [--url URL] [--token-file PATH]

The library behind the `cylc monitor` main-loop plugin (cylc_monitor.mainloop), which
uploads from inside the scheduler; run as a command, it uploads one snapshot by hand
(for a workflow started without the plugin, say). Standard library only.

A snapshot is a consistent copy of <run dir>/log/db (SQLite's backup API, read-only)
plus three small tables of its own:
    monitor        one row: the scheduler's state (running, paused, stalled, stopped),
                   host, PID, upload time and the heartbeat interval;
    job_resources  each job's Slurm (#SBATCH) or PBS (#PBS) directives, read from its
                   job script (log/job/<cycle>/<task>/<NN>/job): what the job asked for;
    job_logs       the end of job.err for each job that failed.
The monitor merges it into its own database, replacing the workflow's previous one.

The URL is --url, else $CYLC_MONITOR_URL, else the first word of
~/.config/cylc-monitor/url. The token (the monitor's cylc-writer token) is read from
--token-file, else $CYLC_MONITOR_TOKEN_FILE, else ~/.config/cylc-monitor/token.
Without both, nothing is uploaded.
"""

import argparse
import datetime as dt
import gzip
import os
import re
import socket
import sqlite3
import sys
import tempfile
import urllib.parse
import urllib.request
from dataclasses import dataclass, field
from pathlib import Path

URL_FILE = "~/.config/cylc-monitor/url"
TOKEN_FILE = "~/.config/cylc-monitor/token"
# A running workflow uploads at least this often, changed or not; the monitor calls it
# `silent` after three of these without one.
HEARTBEAT_S = 300
DIRECTIVE = re.compile(r"^#SBATCH\s+(--?[\w-]+)(?:[= ]\s*(\S+))?")
# The prefix of each job runner's directives, as Cylc writes them.
PREFIXES = {"#SBATCH": "slurm", "#PBS": "pbs"}
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
# The job_resources columns, in order.
RESOURCES = [
    "cores",
    "ntasks",
    "cpus_per_task",
    "nodes",
    "mem_mb",
    "time_limit_min",
    "account",
    "partition",
    "directives",
    "runner",
    "chunks",
]


def now() -> str:
    """Return the time now, in UTC.

    Returns
    -------
    str
        The time as ISO 8601, to the second, e.g. 2026-11-10T09:40:09Z.
    """
    return dt.datetime.now(dt.UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


# --- what each job asked for -------------------------------------------------------


def minutes(value: str) -> float:
    """Convert a Slurm --time value to minutes.

    Parameters
    ----------
    value : str
        The time limit as Slurm takes it: MM, MM:SS, HH:MM:SS, D-HH, D-HH:MM or
        D-HH:MM:SS.

    Returns
    -------
    float
        The time limit in minutes.
    """
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
    """Convert a Slurm memory size to megabytes.

    Parameters
    ----------
    value : str
        The size as Slurm takes it: a number with an optional K, M, G or T suffix
        (default M).

    Returns
    -------
    float or None
        The size in MB, or None if `value` isn't a size.
    """
    match = re.fullmatch(r"(\d+(?:\.\d+)?)([KMGT]?)B?", value.upper())
    if not match:
        return None
    scale = {"K": 1 / 1024, "": 1, "M": 1, "G": 1024, "T": 1024 * 1024}[match.group(2)]
    return float(match.group(1)) * scale


def header(job_script: Path) -> tuple[str | None, list[str]]:
    """Read the directives at the top of a job script, and the job runner they are for.

    Parameters
    ----------
    job_script : Path
        The job script Cylc wrote (log/job/<cycle>/<task>/<NN>/job).

    Returns
    -------
    tuple[str or None, list[str]]
        The job runner (slurm or pbs, from the first directive's prefix; None if the
        script is missing or has none) and its directive lines, prefix and all. Lines
        with the other runner's prefix are left out.
    """
    runner: str | None = None
    lines: list[str] = []
    try:
        with job_script.open(errors="replace") as handle:
            for line in handle:
                prefix = line.split(maxsplit=1)[0] if line.strip() else ""
                if prefix in PREFIXES:
                    runner = runner or PREFIXES[prefix]
                    if PREFIXES[prefix] == runner:
                        lines.append(line.strip())
                elif lines and line.strip() and not line.startswith("#"):
                    break  # past the header
    except OSError:
        pass
    return runner, lines


def directives(job_script: Path) -> dict[str, str]:
    """Read the #SBATCH directives at the top of a job script.

    Parameters
    ----------
    job_script : Path
        The job script Cylc wrote (log/job/<cycle>/<task>/<NN>/job).

    Returns
    -------
    dict[str, str]
        Each directive's long option (short ones translated) and its value, empty for
        a flag. Empty if the script is missing or has no #SBATCH directives.
    """
    runner, lines = header(job_script)
    return slurm_directives(lines) if runner == "slurm" else {}


def slurm_directives(lines: list[str]) -> dict[str, str]:
    """Parse #SBATCH lines.

    Parameters
    ----------
    lines : list[str]
        The #SBATCH lines.

    Returns
    -------
    dict[str, str]
        Each directive's long option (short ones translated) and its value, empty for
        a flag.
    """
    found: dict[str, str] = {}
    for line in lines:
        match = DIRECTIVE.match(line)
        if match:
            option = SHORT.get(match.group(1), match.group(1))
            found[option] = match.group(2) or ""
    return found


def resources(job_script: Path) -> dict[str, int | float | str | None]:
    """Work out what a job asked its job runner for, from its job script.

    Parameters
    ----------
    job_script : Path
        The job script Cylc wrote (log/job/<cycle>/<task>/<NN>/job).

    Returns
    -------
    dict[str, int or float or str or None]
        The RESOURCES columns, from slurm_resources or pbs_resources by the script's
        directives. All None but `runner` (and that too, without directives) for a
        script with none, such as a background job's.
    """
    runner, lines = header(job_script)
    if runner == "pbs":
        return pbs_resources(lines)
    return slurm_resources(slurm_directives(lines))


def slurm_resources(d: dict[str, str]) -> dict[str, int | float | str | None]:
    """Work out what a job asked Slurm for, from its #SBATCH directives.

    Parameters
    ----------
    d : dict[str, str]
        The directives, as slurm_directives reads them.

    Returns
    -------
    dict[str, int or float or str or None]
        The RESOURCES columns: cores (ntasks x cpus-per-task), ntasks, cpus_per_task,
        nodes (the minimum, for a range), mem_mb (per CPU x cores, or per node x
        nodes), time_limit_min, account, partition, and every directive as text;
        runner slurm and chunks None. None where the script doesn't say.
    """

    def integer(key: str) -> int | None:
        """Read an integer directive, the lower end of a range (e.g. --nodes=5-10).

        Parameters
        ----------
        key : str
            The directive's long option.

        Returns
        -------
        int or None
            Its value, or None if not given or not an integer.
        """
        try:
            return int(d[key].split("-")[0]) if key in d else None
        except ValueError:
            return None

    ntasks, cpus, nodes = integer("--ntasks"), integer("--cpus-per-task"), integer("--nodes")
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
        "runner": "slurm" if d else None,
        "chunks": None,
    }


# --- PBS ---------------------------------------------------------------------------


def pbs_minutes(value: str) -> float | None:
    """Convert a PBS walltime to minutes.

    Parameters
    ----------
    value : str
        The walltime as PBS takes it: [[HH:]MM:]SS, each part optionally fractional.
        Cylc writes its execution time limit as whole seconds (e.g. 3600).

    Returns
    -------
    float or None
        The walltime in minutes, or None if `value` isn't one.
    """
    try:
        parts = [float(p) for p in value.split(":")]
    except ValueError:
        return None
    if not 1 <= len(parts) <= 3:
        return None
    seconds = 0.0
    for part in parts:
        seconds = seconds * 60 + part
    return seconds / 60


def pbs_megabytes(value: str) -> float | None:
    """Convert a PBS size to megabytes.

    Parameters
    ----------
    value : str
        The size as PBS takes it: an integer with an optional b, kb, mb, gb, tb or pb
        suffix, in either case (default bytes). 1kb is 1024 bytes. Sizes in words
        (w, kw, ...) aren't converted.

    Returns
    -------
    float or None
        The size in MB, or None if `value` isn't a size in bytes.
    """
    match = re.fullmatch(r"(\d+)([kmgtp]?b)?", value.lower())
    if not match:
        return None
    exponent = {None: -2, "b": -2, "kb": -1, "mb": 0, "gb": 1, "tb": 2, "pb": 3}[match.group(2)]
    return int(match.group(1)) * 1024.0**exponent


def pbs_directives(lines: list[str]) -> tuple[list[tuple[str, str]], dict[str, str]]:
    """Parse #PBS lines into options and -l resources.

    Parameters
    ----------
    lines : list[str]
        The #PBS lines. Each may hold several options (`#PBS -q q -l walltime=60`); a
        -l value may hold several resources joined by commas (`-l select=...,place=...`),
        and -l may be given on several lines.

    Returns
    -------
    tuple[list[tuple[str, str]], dict[str, str]]
        Every option and its value (empty for a flag) in order, -l ones included, and
        the -l resources by name (a later one replacing an earlier).
    """
    options: list[tuple[str, str]] = []
    limits: dict[str, str] = {}
    for line in lines:
        words = line.split()[1:]
        i = 0
        while i < len(words):
            option, value = words[i], ""
            if not option.startswith("-"):
                i += 1
                continue
            if i + 1 < len(words) and not words[i + 1].startswith("-"):
                value = words[i + 1]
                i += 1
            i += 1
            options.append((option, value))
            if option == "-l":
                for item in value.split(","):
                    name, _, setting = item.partition("=")
                    if name:
                        limits[name] = setting
    return options, limits


def pbs_resources(lines: list[str]) -> dict[str, int | float | str | None]:
    """Work out what a job asked PBS for, from its #PBS directives.

    PBS Pro's `select=N:ncpus=C:mpiprocs=P:ompthreads=T:mem=M` asks for N chunks, each
    of C CPUs and M memory; chunks joined by `+` are summed. Without a select, the
    job-wide `-l ncpus` and `-l mem`, or Torque's `-l nodes=N:ppn=C`, are read instead.
    A chunk without ncpus has PBS's default, 1. The job-wide `-l mem` is read only when
    the select doesn't give every chunk's mem.

    Parameters
    ----------
    lines : list[str]
        The #PBS lines.

    Returns
    -------
    dict[str, int or float or str or None]
        The RESOURCES columns: cores (the total ncpus, as PBS counts them: hardware
        threads on a site that counts SMT siblings), ntasks (total mpiprocs),
        cpus_per_task (ompthreads, if every chunk gives the same), chunks, nodes (1 for
        one chunk or place=pack, the chunks for place=scatter, else None, since PBS may
        put several chunks on one host), mem_mb (summed over chunks, if every chunk
        gives one), time_limit_min (walltime), account (-A, else -P), partition (the
        queue, -q), every directive as text, and runner pbs. None where the script
        doesn't say.
    """
    options, limits = pbs_directives(lines)
    flags = dict(options)
    cores: int | None = None
    ntasks: int | None = None
    threads: set[int | None] = set()
    chunks: int | None = None
    nodes: int | None = None
    mem_mb: float | None = None
    no_mem = False
    try:
        if "select" in limits:
            cores, chunks = 0, 0
            for chunk in limits["select"].split("+"):
                count, *settings = chunk.split(":")
                if "=" in count:  # no chunk count: one chunk
                    settings.insert(0, count)
                    count = "1"
                n = int(count)
                spec = dict(s.partition("=")[::2] for s in settings)
                chunks += n
                cores += n * int(spec.get("ncpus", 1))
                if "mpiprocs" in spec:
                    ntasks = (ntasks or 0) + n * int(spec["mpiprocs"])
                threads.add(int(spec["ompthreads"]) if "ompthreads" in spec else None)
                if "mem" in spec:
                    size = pbs_megabytes(spec["mem"])
                    if size is None:
                        raise ValueError(spec["mem"])
                    mem_mb = (mem_mb or 0) + n * size
                else:
                    no_mem = True
            if no_mem:  # some chunks get the site's default: unknown
                mem_mb = None
            place = limits.get("place", "").split(":")
            nodes = 1 if chunks == 1 or "pack" in place else chunks if "scatter" in place else None
        elif "nodes" in limits:  # Torque: nodes=N:ppn=C
            count, *settings = limits["nodes"].split(":")
            spec = dict(s.partition("=")[::2] for s in settings)
            nodes = chunks = int(count) if count.isdigit() else None
            if nodes is not None:
                cores = nodes * int(spec.get("ppn", 1))
    except ValueError:  # a select or nodes PBS wouldn't take: say nothing of it
        cores = ntasks = chunks = nodes = mem_mb = None
        threads.clear()
    if cores is None and "ncpus" in limits and limits["ncpus"].isdigit():
        cores = int(limits["ncpus"])
    if mem_mb is None and "mem" in limits:
        mem_mb = pbs_megabytes(limits["mem"])
    walltime = limits.get("walltime")
    return {
        "cores": cores,
        "ntasks": ntasks,
        "cpus_per_task": threads.pop() if len(threads) == 1 else None,
        "nodes": nodes,
        "mem_mb": mem_mb,
        "time_limit_min": pbs_minutes(walltime) if walltime else None,
        "account": flags.get("-A") or flags.get("-P") or None,
        "partition": flags.get("-q") or None,
        "directives": " ".join(f"{k} {v}" if v else k for k, v in options) or None,
        "runner": "pbs",
        "chunks": chunks,
    }


def tail(path: Path, lines: int = 20, max_chars: int = 2000) -> str | None:
    """Read the end of a log file.

    Parameters
    ----------
    path : Path
        The file.
    lines : int, optional
        How many lines to keep.
    max_chars : int, optional
        At most this many characters of those lines, from the end.

    Returns
    -------
    str or None
        The last lines, or None if the file is missing or empty.
    """
    try:
        text = path.read_text(errors="replace")
    except OSError:
        return None
    return "\n".join(text.rstrip().splitlines()[-lines:])[-max_chars:] or None


# --- the snapshot ------------------------------------------------------------------


@dataclass
class Snapshot:
    """Build snapshots of one run directory, remembering the job scripts already read.

    Parameters
    ----------
    run_dir : Path
        The workflow's run directory.
    workflow : str
        The workflow's ID, e.g. my-flow/run1.
    interval : int
        The heartbeat interval, in seconds, recorded for the monitor.
    """

    run_dir: Path
    workflow: str
    interval: int
    parsed: dict[tuple[str, str, int], dict] = field(default_factory=dict, init=False)

    def build(self, path: Path, state: str, host: str, pid: int | None) -> None:
        """Write a snapshot of the run directory to a new database.

        Parameters
        ----------
        path : Path
            Where to write it; it must not exist.
        state : str
            The scheduler's state: running, paused, stalled or stopped.
        host : str
            The scheduler's host.
        pid : int or None
            The scheduler's process ID, if known.
        """
        source = sqlite3.connect(f"file:{self.run_dir / 'log' / 'db'}?mode=ro", uri=True)
        target = sqlite3.connect(path)
        try:
            source.backup(target)
        finally:
            source.close()
        with target:
            jobs = target.execute("select cycle, name, submit_num, run_status, submit_status from task_jobs").fetchall()
            target.execute(
                "create table monitor (workflow text, status text, host text, pid integer, uploaded_at text, interval_s integer)"
            )
            target.execute(
                "insert into monitor values (?, ?, ?, ?, ?, ?)", (self.workflow, state, host, pid, now(), self.interval)
            )
            target.execute(
                f"create table job_resources (cycle text, name text, submit_num integer, {', '.join(RESOURCES)})"
            )
            target.execute("create table job_logs (cycle text, name text, submit_num integer, job_err text)")
            placeholders = ", ".join("?" * (3 + len(RESOURCES)))
            for cycle, name, submit, run_status, submit_status in jobs:
                job_dir = self.run_dir / "log" / "job" / str(cycle) / name / f"{submit:02d}"
                key = (str(cycle), name, submit)
                if key not in self.parsed and (job_dir / "job").exists():
                    self.parsed[key] = resources(job_dir / "job")
                if key in self.parsed:
                    values = [self.parsed[key][c] for c in RESOURCES]
                    target.execute(f"insert into job_resources values ({placeholders})", [*key, *values])
                if run_status not in (None, 0) or submit_status not in (None, 0):
                    err = tail(job_dir / "job.err") or tail(job_dir / "job-activity.log")
                    if err:
                        target.execute("insert into job_logs values (?, ?, ?, ?)", (*key, err))
        target.execute("vacuum")
        target.close()


def upload(url: str, token: str, workflow: str, path: Path) -> None:
    """Post a snapshot to the monitor, gzipped.

    Parameters
    ----------
    url : str
        The monitor's URL.
    token : str
        The monitor's cylc-writer token.
    workflow : str
        The workflow's ID.
    path : Path
        The snapshot.
    """
    request = urllib.request.Request(
        f"{url.rstrip('/')}/-/cylc/upload?{urllib.parse.urlencode({'workflow': workflow})}",
        data=gzip.compress(path.read_bytes()),
        method="POST",
        headers={
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/vnd.sqlite3",
            "Content-Encoding": "gzip",
        },
    )
    with urllib.request.urlopen(request, timeout=60) as response:
        response.read()


# --- settings and the command line ------------------------------------------------


def settings(url: str | None = None, token_file: str | None = None) -> tuple[str, str] | None:
    """Find the monitor's URL and the writer token.

    Parameters
    ----------
    url : str, optional
        The URL, if given; else $CYLC_MONITOR_URL, else the first word of URL_FILE.
    token_file : str, optional
        The token's file, if given; else $CYLC_MONITOR_TOKEN_FILE, else TOKEN_FILE.

    Returns
    -------
    tuple[str, str] or None
        The URL and the token, or None if either is missing.
    """
    url = url or os.environ.get("CYLC_MONITOR_URL")
    if not url:
        try:
            url = Path(URL_FILE).expanduser().read_text().split()[0]
        except (OSError, IndexError):
            return None
    try:
        token = (
            Path(token_file or os.environ.get("CYLC_MONITOR_TOKEN_FILE") or TOKEN_FILE).expanduser().read_text().strip()
        )
    except OSError:
        return None
    return (url, token) if url != "None" and token else None


def main(argv: list[str] | None = None) -> int:
    """Upload one snapshot of a run directory.

    Parameters
    ----------
    argv : list[str], optional
        The command line's arguments (default: sys.argv's).

    Returns
    -------
    int
        0 once uploaded, 1 without a URL or token.
    """
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--url")
    parser.add_argument("--token-file")
    parser.add_argument("--workflow", required=True, help="the workflow ID, e.g. my-flow/run1")
    parser.add_argument("--run-dir", type=Path, required=True)
    args = parser.parse_args(argv)
    found = settings(args.url, args.token_file)
    if found is None:
        print("no monitor URL or token: nothing uploaded")
        return 1
    # By hand there is no scheduler to ask: running while its contact file exists.
    state = "running" if (args.run_dir / ".service" / "contact").exists() else "stopped"
    with tempfile.TemporaryDirectory(prefix="cylc-monitor-") as tmp:
        path = Path(tmp) / "snapshot.db"
        Snapshot(args.run_dir, args.workflow, HEARTBEAT_S).build(path, state, socket.gethostname(), None)
        upload(*found, args.workflow, path)
    print(f"uploaded {args.workflow} ({state})")
    return 0


def run() -> None:
    """Run `main` as the cylc-monitor-upload command, exiting with its status."""
    sys.exit(main())


if __name__ == "__main__":
    run()
