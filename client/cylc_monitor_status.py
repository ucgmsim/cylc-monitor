#!/usr/bin/env python3
"""Print the Cylc monitor's workflows and their jobs as text.

    cylc_monitor_status.py [WORKFLOW] [--url URL] [--token-file PATH] [--all]

Reads only the monitor (a Datasette off the cluster), with the reader token: never the
cluster. For each workflow (by default those whose scheduler is running or silent; --all
for every one): its health and last upload, its jobs counted by state, core-hours and
RSUs, the jobs queued or running, and the failed jobs of tasks not since succeeded, with
the end of their job.err. Exits 1 if a workflow is silent or its scheduler gone, or a
task's latest job failed, so a check can act on it. Standard library only (Python 3.8+).

The URL defaults to $CYLC_MONITOR_URL; the token file to $CYLC_MONITOR_READER_TOKEN_FILE,
else ~/.config/cylc-monitor/reader-token.
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
WORKFLOWS = "SELECT * FROM workflows WHERE (:all OR health IN ('running', 'silent')) AND (:workflow = '' OR workflow = :workflow) ORDER BY workflow"
ACTIVE = "SELECT * FROM jobs WHERE workflow = :workflow AND state IN ('submitting', 'submitted', 'running') ORDER BY time_submit"
# Each task's latest job, when that job failed: a retry that went on to succeed isn't one.
FAILED = """
SELECT j.* FROM jobs j
JOIN (SELECT cycle, name, max(submit_num) AS submit_num FROM jobs WHERE workflow = :workflow GROUP BY cycle, name) latest
  USING (cycle, name, submit_num)
WHERE j.workflow = :workflow AND j.state IN ('failed', 'submit-failed')
"""


def query(url: str, token: str, sql: str, **params) -> list[dict]:
    query_string = urllib.parse.urlencode({"sql": sql, "_shape": "array", **params})
    request = urllib.request.Request(
        f"{url.rstrip('/')}/cylc.json?{query_string}",
        headers={"Authorization": f"Bearer {token}"},
    )
    with urllib.request.urlopen(request, timeout=30) as response:
        return json.load(response)


def report(
    url: str, token: str, workflow: str = "", every: bool = False
) -> tuple[list[str], bool]:
    """The status as lines of text, and whether anything needs a person."""
    lines, trouble = [], False
    for w in query(url, token, WORKFLOWS, workflow=workflow, all=int(every)):
        lines += [
            "",
            f"{w['workflow']}: {w['health'].upper()} (last upload {w['uploaded_at']} from {w['host']})",
            (
                f"  {w['submitted'] or 0} queued, {w['running'] or 0} running, {w['succeeded'] or 0} succeeded, "
                f"{w['failed'] or 0} failed jobs; {w['core_hours'] or 0} core-hours, {w['rsu'] or 0} RSUs"
            ),
        ]
        for j in query(url, token, ACTIVE, workflow=w["workflow"]):
            waited = (
                f"running {j['run_s']:.0f} s"
                if j["state"] == "running"
                else f"queued {j['queue_s'] or 0:.0f} s"
            )
            lines.append(
                f"  {j['state']:9} {j['cycle']}/{j['name']} try {j['try_num']}, job {j['job_id']}, {waited}"
            )
        failed = query(url, token, FAILED, workflow=w["workflow"])
        for j in failed:
            lines.append(
                f"  FAILED    {j['cycle']}/{j['name']} try {j['try_num']}, job {j['job_id']} ({j['time_run_exit'] or j['time_submit']})"
            )
            lines += [
                f"      | {line}" for line in (j["job_err"] or "").splitlines()[-8:]
            ]
        trouble |= w["health"] in ("silent", "scheduler gone") or bool(failed)
    return lines or ["No workflows."], trouble


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("workflow", nargs="?", default="")
    parser.add_argument("--url", default=os.environ.get("CYLC_MONITOR_URL"))
    parser.add_argument(
        "--token-file",
        type=Path,
        default=Path(os.environ.get("CYLC_MONITOR_READER_TOKEN_FILE") or TOKEN_FILE),
    )
    parser.add_argument("--all", action="store_true", help="stopped workflows too")
    args = parser.parse_args(argv)
    if not args.url:
        parser.error("give --url or set CYLC_MONITOR_URL")
    lines, trouble = report(
        args.url,
        args.token_file.expanduser().read_text().strip(),
        args.workflow,
        args.all,
    )
    print("\n".join(lines).lstrip("\n"))
    return 1 if trouble else 0


def run() -> None:
    sys.exit(main())


if __name__ == "__main__":
    run()
