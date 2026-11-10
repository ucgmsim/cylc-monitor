#!/usr/bin/env python3
"""Print the Cylc monitor's workflow status as text, from its /-/cylc.json.

    cylc_monitor_status.py [WORKFLOW] [--url URL] [--token-file PATH] [--json]

Reads only the monitor (a Datasette off the cluster), with the reader token: never the
cluster. For each workflow, its scheduler's health and heartbeat, its tasks counted by
state, what is running, and what has failed or is retrying with the end of its job.err.
Exits 1 if a workflow is silent, its scheduler gone, stalled or aborted, or a task has
failed, so a check can act on it. Standard library only (Python 3.8 or later).

The URL defaults to $CYLC_MONITOR_URL; the token file to $CYLC_MONITOR_READER_TOKEN_FILE,
else ~/.config/cylc-monitor/reader-token. --json prints the JSON as it came.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import urllib.parse
import urllib.request
from pathlib import Path

TOKEN_FILE = "~/.config/cylc-monitor/reader-token"


def fetch(url: str, token: str, workflow: str | None = None) -> dict:
    query = "?" + urllib.parse.urlencode({"workflow": workflow}) if workflow else ""
    request = urllib.request.Request(f"{url.rstrip('/')}/-/cylc.json{query}", headers={"Authorization": f"Bearer {token}"})
    with urllib.request.urlopen(request, timeout=30) as response:
        return json.load(response)


def report(state: dict) -> tuple[list[str], bool]:
    """The status as lines of text, and whether anything needs a person."""
    lines = [f"Cylc monitor, {state['generated_at']}"]
    trouble = False
    for w in state["workflows"]:
        beat = w["heartbeat"]
        heard = f"heartbeat {beat['status']} {beat['age']} ago from {beat['host']}" if beat else "no heartbeat"
        lines += ["", f"{w['workflow']}: {w['health'].upper()} ({heard})"]
        if w["stalled"]:
            last = w["last_workflow_event"]
            lines.append(f"  ! {last['message'] or last['event']} at {last['received_at']}")
        if w["counts"]:
            lines.append("  " + ", ".join(f"{n} {s}" for s, n in sorted(w["counts"].items())))
        for r in w["running"]:
            lines.append(f"  {r['state']:9} {r['label'] or r['cycle']}: {r['task']} for {r['running_for'] or r['for']}, job {r['job_id']}")
        for p in w["problems"]:
            lines.append(f"  {p['state'].upper():9} {p['label'] or p['cycle']}: {p['task']}, try {p['try_num']}, job {p['job_id']} ({p['since']})")
            lines += [f"      | {line}" for line in (p["log_tail"] or "").splitlines()[-8:]]
        trouble |= w["health"] in ("silent", "scheduler gone") or w["stalled"] or any(p["state"] == "failed" for p in w["problems"])
    return lines, trouble


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("workflow", nargs="?")
    parser.add_argument("--url", default=os.environ.get("CYLC_MONITOR_URL"))
    parser.add_argument("--token-file", type=Path, default=Path(os.environ.get("CYLC_MONITOR_READER_TOKEN_FILE") or TOKEN_FILE))
    parser.add_argument("--json", action="store_true", help="print the JSON as it came")
    args = parser.parse_args(argv)
    if not args.url:
        parser.error("give --url or set CYLC_MONITOR_URL")
    state = fetch(args.url, args.token_file.expanduser().read_text().strip(), args.workflow)
    if args.json:
        print(json.dumps(state, indent=2))
        return 0
    lines, trouble = report(state)
    print("\n".join(lines))
    return 1 if trouble else 0


def run() -> None:
    sys.exit(main())


if __name__ == "__main__":
    run()
