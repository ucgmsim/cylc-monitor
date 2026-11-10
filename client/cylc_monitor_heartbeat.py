#!/usr/bin/env python3
"""Post a Cylc workflow's scheduler heartbeat to the Cylc monitor while the scheduler runs.

    cylc_monitor_heartbeat.py --url URL --workflow WORKFLOW [--interval 300] [--daemon]
                              [--run-dir DIR] [--token-file PATH]

Started by the workflow's startup handler on the scheduler host (so it uses only the
standard library, Python 3.8 or later; it needs cylc_monitor_event.py beside it, or the
package installed). Every --interval seconds it reads the scheduler's contact file
(<run dir>/.service/contact, the run dir defaulting to $CYLC_WORKFLOW_RUN_DIR) and
upserts the workflow's row of the monitor's `heartbeats` table:

- `running` while the scheduler's process is alive;
- `stopped` when the contact file is gone (a clean shutdown; the event handler's
  shutdown event says so too), then it exits;
- `scheduler gone` when the contact file remains but its process doesn't (a crash
  or kill), then it exits.

If the host itself goes down, the heartbeats just stop: the monitor shows a `running`
heartbeat older than a few intervals as `silent`. One heartbeat a run directory: a
second start (a restart on the same host) exits if the first is still alive
(<run dir>/log/cylc-monitor-heartbeat.pid).

With --daemon it detaches first, so the event handler returns at once, and logs to
<run dir>/log/cylc-monitor-heartbeat.log. It reads only its own run directory and
posts once an interval. URL and token as for cylc_monitor_event.py. It exits 0.
"""

from __future__ import annotations

import argparse
import os
import socket
import sys
import time
from pathlib import Path

try:
    from cylc_monitor_event import now, post
except ImportError:  # copied somewhere else than beside it: look beside this file
    sys.path.insert(0, str(Path(__file__).resolve().parent))
    from cylc_monitor_event import now, post

PID_FILE = "cylc-monitor-heartbeat.pid"
LOG_FILE = "cylc-monitor-heartbeat.log"


def contact(run_dir: Path) -> dict[str, str] | None:
    """The scheduler's contact file, or None if it is gone."""
    try:
        text = (run_dir / ".service" / "contact").read_text()
    except OSError:
        return None
    return dict(line.split("=", 1) for line in text.splitlines() if "=" in line)


def alive(pid: int) -> bool:
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    return True


def status(run_dir: Path, host: str) -> tuple[str, int | None]:
    """running, stopped or scheduler gone, and the scheduler's PID."""
    info = contact(run_dir)
    if info is None:
        return "stopped", None
    try:
        pid = int(info.get("CYLC_WORKFLOW_PID", ""))
    except ValueError:
        return "running", None
    scheduler_host = info.get("CYLC_WORKFLOW_HOST", host).split(".")[0]
    if scheduler_host != host.split(".")[0]:
        # Not this host's process to check: say it runs, and let the heartbeats speak.
        return "running", pid
    return ("running" if alive(pid) else "scheduler gone"), pid


def daemonise(log: Path) -> None:
    """Detach from the event handler: fork twice, new session, stdio to LOG."""
    if os.fork():
        os._exit(0)
    os.setsid()
    if os.fork():
        os._exit(0)
    out = os.open(log, os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0o644)
    null = os.open(os.devnull, os.O_RDONLY)
    os.dup2(null, 0)
    os.dup2(out, 1)
    os.dup2(out, 2)


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--url", "--monitor-url", dest="url", default=os.environ.get("CYLC_MONITOR_URL"))
    parser.add_argument("--token-file", type=Path)
    parser.add_argument("--workflow", required=True)
    parser.add_argument("--interval", type=int, default=300)
    parser.add_argument("--run-dir", type=Path, default=os.environ.get("CYLC_WORKFLOW_RUN_DIR"))
    parser.add_argument("--daemon", action="store_true")
    args = parser.parse_args(argv)
    if not args.url or args.url == "None" or args.run_dir is None:
        return
    run_dir = Path(args.run_dir)
    (run_dir / "log").mkdir(parents=True, exist_ok=True)
    pid_file = run_dir / "log" / PID_FILE
    try:
        if alive(int(pid_file.read_text())):
            return
    except (OSError, ValueError):
        pass
    if args.daemon:
        daemonise(run_dir / "log" / LOG_FILE)
    pid_file.write_text(str(os.getpid()))
    host = socket.gethostname()
    while True:
        state, pid = status(run_dir, host)
        beat = {"workflow": args.workflow, "seen_at": now(), "status": state, "host": host, "pid": pid, "interval_s": args.interval}
        post(args.url, "heartbeats/-/upsert", {"rows": [beat]}, args.token_file)
        if state != "running":
            print(f"{now()} heartbeat: {state}; exiting", flush=True)
            return
        time.sleep(args.interval)


def run() -> None:
    try:
        main()
    except Exception as error:  # noqa: BLE001
        print(f"cylc-monitor-heartbeat: {error}")


if __name__ == "__main__":
    run()
