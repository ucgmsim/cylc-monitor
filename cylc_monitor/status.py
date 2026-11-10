"""Print the Cylc monitor's workflows and their jobs as text.

    cylc-monitor-status [WORKFLOW] [--url URL] [--token-file PATH] [--all]

Reads only the monitor (a Datasette off the cluster), with the reader token: never the
cluster. For each workflow (by default those not stopped; --all for every one): its
health and last upload, its jobs counted by state, core-hours and RSUs, the jobs queued
or running, and the failed jobs of tasks not since succeeded, with the end of their
job.err. Exits 1 if a workflow is silent or stalled, or a task's latest job failed, so
a check can act on it. Standard library only.

The URL is --url, else $CYLC_MONITOR_URL, else the first word of
~/.config/cylc-monitor/url. The token is read from --token-file, else
$CYLC_MONITOR_READER_TOKEN_FILE, else ~/.config/cylc-monitor/reader-token.
"""

import argparse
import json
import os
import sys
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Any

from cylc_monitor.upload import URL_FILE

TOKEN_FILE = "~/.config/cylc-monitor/reader-token"
WORKFLOWS = (
    "SELECT * FROM workflows WHERE (:all OR health != 'stopped') AND (:workflow = '' OR workflow = :workflow)"
    " ORDER BY workflow"
)
ACTIVE = (
    "SELECT * FROM jobs WHERE workflow = :workflow AND state IN ('submitting', 'submitted', 'running')"
    " ORDER BY time_submit"
)
# Each task's latest job, when that job failed: a retry that went on to succeed isn't one.
FAILED = """
SELECT j.* FROM jobs j
JOIN (SELECT cycle, name, max(submit_num) AS submit_num FROM jobs WHERE workflow = :workflow GROUP BY cycle, name) latest
  USING (cycle, name, submit_num)
WHERE j.workflow = :workflow AND j.state IN ('failed', 'submit-failed')
"""


def query(url: str, token: str, sql: str, **params: str | int) -> list[dict[str, Any]]:
    """Run read-only SQL on the monitor's database.

    Parameters
    ----------
    url : str
        The monitor's URL.
    token : str
        The monitor's cylc-reader token.
    sql : str
        The query, with :named parameters.
    **params : str or int
        The parameters' values.

    Returns
    -------
    list[dict[str, Any]]
        The rows.
    """
    query_string = urllib.parse.urlencode({"sql": sql, "_shape": "array", **params})
    request = urllib.request.Request(
        f"{url.rstrip('/')}/cylc.json?{query_string}", headers={"Authorization": f"Bearer {token}"}
    )
    with urllib.request.urlopen(request, timeout=30) as response:
        return json.load(response)


def report(url: str, token: str, workflow: str = "", every: bool = False) -> tuple[list[str], bool]:
    """Describe the workflows' state.

    Parameters
    ----------
    url : str
        The monitor's URL.
    token : str
        The monitor's cylc-reader token.
    workflow : str, optional
        Only this workflow (default: all).
    every : bool, optional
        Stopped workflows too.

    Returns
    -------
    tuple[list[str], bool]
        The lines of text, and whether anything needs a person: a workflow silent or
        stalled, or a task whose latest job failed.
    """
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
            waited = f"running {j['run_s']:.0f} s" if j["state"] == "running" else f"queued {j['queue_s'] or 0:.0f} s"
            lines.append(f"  {j['state']:9} {j['cycle']}/{j['name']} try {j['try_num']}, job {j['job_id']}, {waited}")
        failed = query(url, token, FAILED, workflow=w["workflow"])
        for j in failed:
            when = j["time_run_exit"] or j["time_submit"]
            lines.append(f"  FAILED    {j['cycle']}/{j['name']} try {j['try_num']}, job {j['job_id']} ({when})")
            lines += [f"      | {line}" for line in (j["job_err"] or "").splitlines()[-8:]]
        trouble |= w["health"] in ("silent", "stalled") or bool(failed)
    return lines or ["No workflows."], trouble


def default_url() -> str | None:
    """Find the monitor's URL when not given.

    Returns
    -------
    str or None
        $CYLC_MONITOR_URL, else the first word of URL_FILE, else None.
    """
    try:
        return os.environ.get("CYLC_MONITOR_URL") or Path(URL_FILE).expanduser().read_text().split()[0]
    except (OSError, IndexError):
        return None


def main(argv: list[str] | None = None) -> int:
    """Print the monitor's workflows.

    Parameters
    ----------
    argv : list[str], optional
        The command line's arguments (default: sys.argv's).

    Returns
    -------
    int
        1 if anything needs a person, else 0.
    """
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("workflow", nargs="?", default="")
    parser.add_argument("--url", default=default_url())
    parser.add_argument(
        "--token-file", type=Path, default=Path(os.environ.get("CYLC_MONITOR_READER_TOKEN_FILE") or TOKEN_FILE)
    )
    parser.add_argument("--all", action="store_true", help="stopped workflows too")
    args = parser.parse_args(argv)
    if not args.url:
        parser.error("give --url, set CYLC_MONITOR_URL or write it to ~/.config/cylc-monitor/url")
    token = args.token_file.expanduser().read_text().strip()
    lines, trouble = report(args.url, token, args.workflow, args.all)
    print("\n".join(lines).lstrip("\n"))
    return 1 if trouble else 0


def run() -> None:
    """Run `main` as the cylc-monitor-status command, exiting with its status."""
    sys.exit(main())


if __name__ == "__main__":
    run()
