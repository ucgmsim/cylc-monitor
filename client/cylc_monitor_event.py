#!/usr/bin/env python3
"""Post a Cylc event to the Cylc monitor (a Datasette off the cluster), as one row of its events table.

    cylc_monitor_event.py --url URL --event EVENT --workflow WORKFLOW
                          [--task NAME --point POINT --job-id ID --try-num N --submit-num N]
                          [--label TEXT | --labels A:B:...] [--message TEXT] [--token-file PATH]

Run by a Cylc 8 workflow's event handlers on the scheduler host, so it uses only the
standard library (Python 3.8 or later) and can be copied into the workflow's bin/ or
installed (`cylc-monitor-event`). Each call posts one row: workflow, cycle, task, event,
try and submit numbers, job ID, an optional label for the cycle and a one-line message.
On a failure or retry it also posts the last lines of the task's job.err (from
$CYLC_WORKFLOW_RUN_DIR/log/job/<point>/<task>/<submit>/job.err). A workflow `shutdown`
also marks the workflow's heartbeat `stopped` (cylc_monitor_heartbeat.py posts the rest).

--label is the cycle's label as given; --labels A:B:C labels integer cycle point N with
the Nth (colon-separated: Cylc handler lists are comma-separated, so no argument may
contain a comma). Without --message, failures, retries and workflow events get a
short message of their own; other events none.

The URL defaults to $CYLC_MONITOR_URL; the token, the monitor's cylc-writer token, is read
from --token-file (default $CYLC_MONITOR_TOKEN_FILE, else ~/.config/cylc-monitor/token).
Without a URL or a token, nothing is posted. Cylc fills unknown %(...)s values with
"None", which counts as not given. Once its arguments parse it exits 0 whatever happens
to the post: a lost event shouldn't trouble the workflow.
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import urllib.request
from pathlib import Path

DATABASE = "cylc"
TOKEN_FILE = "~/.config/cylc-monitor/token"

# Task events that are a problem: their row carries the end of job.err.
PROBLEMS = {
    "failed": "{task} failed",
    "submission failed": "{task} could not be submitted",
    "retry": "{task} failed, retrying",
    "submission retry": "{task} submission failed, retrying",
}
WORKFLOW_EVENTS = {
    "startup": "Workflow started",
    "shutdown": "Workflow shut down",
    "stall": "Workflow stalled",
    "abort": "Workflow aborted",
}


def default_token_file() -> Path:
    return Path(os.environ.get("CYLC_MONITOR_TOKEN_FILE") or TOKEN_FILE).expanduser()


def now() -> str:
    return dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def number(value: str | None) -> int | None:
    try:
        return int(value) if value is not None else None
    except ValueError:
        return None


def tail(path: Path, lines: int = 20, max_chars: int = 1500) -> str | None:
    """The end of a log file, if it exists."""
    try:
        text = path.read_text(errors="replace")
    except OSError:
        return None
    return "\n".join(text.rstrip().splitlines()[-lines:])[-max_chars:] or None


def job_err(args: argparse.Namespace) -> str | None:
    """The end of the task's job.err, for this submit."""
    run_dir = os.environ.get("CYLC_WORKFLOW_RUN_DIR")
    if not (run_dir and args.task and args.point and args.submit_num):
        return None
    try:
        submit = f"{int(args.submit_num):02d}"
    except ValueError:
        return None
    return tail(Path(run_dir) / "log" / "job" / args.point / args.task / submit / "job.err")


def label(args: argparse.Namespace) -> str | None:
    """The cycle's label: --label, else the Nth of --labels for integer cycle point N."""
    if args.label:
        return args.label
    if not (args.labels and args.point):
        return None
    try:
        n = int(args.point)
        return args.labels.split(":")[n - 1] if n >= 1 else None
    except (ValueError, IndexError):
        return None


def message(args: argparse.Namespace) -> str | None:
    """The event's one-line message: --message, else one for problems and workflow events."""
    if args.message:
        return args.message
    if not args.task:
        return WORKFLOW_EVENTS.get(args.event)
    if args.event in PROBLEMS:
        return PROBLEMS[args.event].format(task=args.task)
    return None


def event_row(args: argparse.Namespace) -> dict:
    """The event as a row of the monitor's events table."""
    return {
        "sent_at": now(),
        "workflow": args.workflow,
        "cycle": args.point if args.task else None,
        "task": args.task,
        "label": label(args),
        "event": args.event,
        "try_num": number(args.try_num),
        "submit_num": number(args.submit_num),
        "job_id": args.job_id,
        "log_tail": job_err(args) if args.task and args.event in PROBLEMS else None,
        "message": message(args),
    }


def post(url: str, path: str, payload: dict, token_file: Path | None = None) -> bool:
    """POST PAYLOAD to the monitor's Datasette write API (<url>/cylc/<path>), if there is a token.

    True if it was accepted; never raises.
    """
    try:
        token = (token_file or default_token_file()).expanduser().read_text().strip()
    except OSError:
        return False
    request = urllib.request.Request(
        f"{url.rstrip('/')}/{DATABASE}/{path}",
        data=json.dumps(payload).encode(),
        method="POST",
        headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
    )
    try:
        urllib.request.urlopen(request, timeout=20).close()
    except Exception as error:  # noqa: BLE001 - a lost event is never fatal
        print(f"cylc-monitor: could not post to {url}: {error}")
        return False
    return True


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--url", "--monitor-url", dest="url", default=os.environ.get("CYLC_MONITOR_URL"),
                        help="the monitor's URL (default $CYLC_MONITOR_URL); without one, nothing is posted")  # fmt: skip
    parser.add_argument("--token-file", type=Path, help=f"the writer token (default $CYLC_MONITOR_TOKEN_FILE or {TOKEN_FILE})")
    parser.add_argument("--event", required=True, help="%%(event)s")
    parser.add_argument("--workflow", required=True, help="%%(workflow)s")
    parser.add_argument("--task", help="%%(name)s, for task events")
    parser.add_argument("--point", help="%%(point)s, the cycle point")
    parser.add_argument("--job-id", help="%%(job_id)s")
    parser.add_argument("--try-num", help="%%(try_num)s")
    parser.add_argument("--submit-num", help="%%(submit_num)s")
    parser.add_argument("--label", help="a label for the cycle")
    parser.add_argument("--labels", help="labels for integer cycle points 1, 2, ..., colon-separated")
    parser.add_argument("--message", help="a one-line message")
    args = parser.parse_args(argv)
    for name, value in vars(args).items():
        if value in ("None", ""):
            setattr(args, name, None)
    return args


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
    if not args.url:
        return
    post(args.url, "events/-/insert", {"row": event_row(args)}, args.token_file)
    if args.event == "shutdown" and not args.task:
        beat = {"workflow": args.workflow, "seen_at": now(), "status": "stopped", "host": os.uname().nodename}
        post(args.url, "heartbeats/-/upsert", {"rows": [beat]}, args.token_file)


def run() -> None:
    try:
        main()
    except Exception as error:  # noqa: BLE001
        print(f"cylc-monitor-event: {error}")


if __name__ == "__main__":
    run()
