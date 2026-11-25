"""The realtime simulations' job queue: a table on the monitor, read and written over HTTP.

    cylc-monitor-queue submit EVENT_ID [--cmt FILE] [FIELD=VALUE ...]
    cylc-monitor-queue claim [--task-id ID]
    cylc-monitor-queue set EVENT_ID FIELD=VALUE ...
    cylc-monitor-queue get EVENT_ID
    cylc-monitor-queue list [--status STATUS]

The queue is the table ``jobs`` in the monitor's ``queue`` database (server/queue.sql):
one row per event, with ``event_id``, ``task_id``, ``cmt`` (JSON), ``status`` and
``detail``, plus ``created`` and ``updated``. SeismicNow submits events; the realtime
workflow claims the oldest queued one and sets its status as the run goes. Both use
Datasette's JSON write API with the queue-writer token. Submitting an event already in
the queue changes nothing. Re-queue an event by setting its status back to ``queued``.

Claiming is a read then a write, not atomic: fine for the one worker the queue has. Two
workers could claim the same event.

The URL is --url, else $CYLC_MONITOR_URL, else the first word of
~/.config/cylc-monitor/url. The token is read from --token-file, else
$CYLC_MONITOR_QUEUE_TOKEN_FILE, else ~/.config/cylc-monitor/queue-token. Standard
library only.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import urllib.parse
import urllib.request
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from cylc_monitor.status import default_url

TOKEN_FILE = "~/.config/cylc-monitor/queue-token"
TABLE = "queue/jobs"
# Datasette's tilde encoding of a primary key in a row's URL: these bytes stay as they are.
SAFE = frozenset(b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789_-")


def tilde_encode(value: str) -> str:
    """Encode a primary key for a row's URL the way Datasette expects.

    Parameters
    ----------
    value : str
        The primary key.

    Returns
    -------
    str
        The key with each unsafe byte as ``~XX`` and spaces as ``+``.
    """
    return "".join(chr(b) if b in SAFE else "+" if b == ord(" ") else f"~{b:02X}" for b in value.encode())


@dataclass
class Queue:
    """The job queue on the monitor.

    Parameters
    ----------
    url : str
        The monitor's URL.
    token : str
        The monitor's queue-writer token (or, to only read, its reader token).
    timeout : float, optional
        Seconds to wait for each request (default 30).
    """

    url: str
    token: str
    timeout: float = 30

    def _request(self, path: str, body: dict[str, Any] | None = None) -> Any:
        """Send a request to the monitor and decode its JSON reply.

        Parameters
        ----------
        path : str
            The path and query string after the monitor's URL.
        body : dict[str, Any], optional
            The JSON body to POST; a GET without one.

        Returns
        -------
        Any
            The decoded reply.
        """
        headers = {"Authorization": f"Bearer {self.token}"}
        data = None
        if body is not None:
            headers["Content-Type"] = "application/json"
            data = json.dumps(body).encode()
        request = urllib.request.Request(f"{self.url.rstrip('/')}/{path}", data=data, headers=headers)
        with urllib.request.urlopen(request, timeout=self.timeout) as response:
            return json.load(response)

    @staticmethod
    def _decode(row: dict[str, Any]) -> dict[str, Any]:
        """Give a row's ``cmt`` back as JSON rather than text.

        Parameters
        ----------
        row : dict[str, Any]
            A row as Datasette returns it.

        Returns
        -------
        dict[str, Any]
            The row, with ``cmt`` decoded when it holds JSON.
        """
        if isinstance(row.get("cmt"), str):
            try:
                row["cmt"] = json.loads(row["cmt"])
            except json.JSONDecodeError:
                pass
        return row

    def jobs(self, status: str | None = None, limit: int = 100) -> list[dict[str, Any]]:
        """List jobs, oldest first.

        Parameters
        ----------
        status : str, optional
            Only jobs with this status (default: all).
        limit : int, optional
            At most this many (default 100).

        Returns
        -------
        list[dict[str, Any]]
            The jobs.
        """
        params: dict[str, str | int] = {"_shape": "array", "_sort": "created", "_size": limit}
        if status is not None:
            params["status"] = status
        rows = self._request(f"{TABLE}.json?{urllib.parse.urlencode(params)}")
        return [self._decode(row) for row in rows]

    def get(self, event_id: str) -> dict[str, Any] | None:
        """Look up one event's job.

        Parameters
        ----------
        event_id : str
            The event.

        Returns
        -------
        dict[str, Any] or None
            The job, or None if the event was never submitted.
        """
        query = urllib.parse.urlencode({"_shape": "array", "event_id": event_id})
        rows = self._request(f"{TABLE}.json?{query}")
        return self._decode(rows[0]) if rows else None

    def submit(self, event_id: str, cmt: Any = None, **fields: Any) -> dict[str, Any]:
        """Queue an event, unless it is in the queue already.

        Parameters
        ----------
        event_id : str
            The event.
        cmt : Any, optional
            Its CMT solution: anything JSON can encode.
        **fields : Any
            Other columns to set, e.g. ``detail``.

        Returns
        -------
        dict[str, Any]
            The job as stored: the existing one, unchanged, if the event was already there.
        """
        row = {"event_id": event_id, **fields}
        if cmt is not None:
            row["cmt"] = json.dumps(cmt)
        self._request(f"{TABLE}/-/insert", {"row": row, "ignore": True})
        job = self.get(event_id)
        assert job is not None
        return job

    def update(self, event_id: str, **fields: Any) -> None:
        """Set columns of an event's job, e.g. ``status="failed", detail="sw4 timed out"``.

        Parameters
        ----------
        event_id : str
            The event.
        **fields : Any
            The columns and their new values. ``cmt`` is encoded as JSON.
        """
        if "cmt" in fields and not isinstance(fields["cmt"], str):
            fields["cmt"] = json.dumps(fields["cmt"])
        self._request(f"{TABLE}/{tilde_encode(event_id)}/-/update", {"update": fields})

    def claim(self, task_id: str | None = None) -> dict[str, Any] | None:
        """Take the oldest queued job: set it running (and its task_id) and return it.

        Parameters
        ----------
        task_id : str, optional
            What the worker calls the run, e.g. ``realtime/run1//12``.

        Returns
        -------
        dict[str, Any] or None
            The job, now running, or None if nothing is queued.
        """
        queued = self.jobs(status="queued", limit=1)
        if not queued:
            return None
        job = queued[0]
        fields = {"status": "running"} | ({"task_id": task_id} if task_id is not None else {})
        self.update(job["event_id"], **fields)
        return job | fields


def connect(url: str | None = None, token_file: str | None = None) -> Queue:
    """Make a Queue from the usual settings.

    Parameters
    ----------
    url : str, optional
        The monitor's URL; else $CYLC_MONITOR_URL, else ~/.config/cylc-monitor/url.
    token_file : str, optional
        The token's file; else $CYLC_MONITOR_QUEUE_TOKEN_FILE, else TOKEN_FILE.

    Returns
    -------
    Queue
        The queue.

    Raises
    ------
    ValueError
        If no URL is set.
    """
    url = url or default_url()
    if not url:
        raise ValueError("no monitor URL: give --url, set CYLC_MONITOR_URL or write ~/.config/cylc-monitor/url")
    path = token_file or os.environ.get("CYLC_MONITOR_QUEUE_TOKEN_FILE") or TOKEN_FILE
    return Queue(url, Path(path).expanduser().read_text().strip())


def fields(pairs: list[str]) -> dict[str, str]:
    """Parse FIELD=VALUE arguments.

    Parameters
    ----------
    pairs : list[str]
        The arguments.

    Returns
    -------
    dict[str, str]
        The fields and their values.
    """
    parsed = {}
    for pair in pairs:
        key, sep, value = pair.partition("=")
        if not sep or not key:
            raise argparse.ArgumentTypeError(f"expected FIELD=VALUE, got {pair!r}")
        parsed[key] = value
    return parsed


def main(argv: list[str] | None = None) -> int:
    """Run the command line.

    Parameters
    ----------
    argv : list[str], optional
        The command line's arguments (default: sys.argv's).

    Returns
    -------
    int
        0, or 1 if there was nothing to claim or get.
    """
    parser = argparse.ArgumentParser(prog="cylc-monitor-queue", description=__doc__.split("\n\n")[0])
    parser.add_argument("--url", help="the monitor's URL")
    parser.add_argument("--token-file", help="the queue-writer token's file")
    commands = parser.add_subparsers(dest="command", required=True)
    submit = commands.add_parser("submit", help="queue an event (no-op if already queued)")
    submit.add_argument("event_id")
    submit.add_argument("--cmt", type=Path, help="a JSON file with the CMT solution")
    submit.add_argument("fields", nargs="*", metavar="FIELD=VALUE")
    claim = commands.add_parser("claim", help="take the oldest queued job and print it")
    claim.add_argument("--task-id", help="what the worker calls the run")
    update = commands.add_parser("set", help="set columns of an event's job")
    update.add_argument("event_id")
    update.add_argument("fields", nargs="+", metavar="FIELD=VALUE")
    get = commands.add_parser("get", help="print an event's job")
    get.add_argument("event_id")
    listing = commands.add_parser("list", help="print the jobs, oldest first")
    listing.add_argument("--status", help="only jobs with this status")
    args = parser.parse_args(argv)

    try:
        queue = connect(args.url, args.token_file)
        if args.command == "submit":
            cmt = json.loads(args.cmt.read_text()) if args.cmt else None
            result: Any = queue.submit(args.event_id, cmt, **fields(args.fields))
        elif args.command == "claim":
            result = queue.claim(args.task_id)
        elif args.command == "set":
            queue.update(args.event_id, **fields(args.fields))
            return 0
        elif args.command == "get":
            result = queue.get(args.event_id)
        else:
            result = queue.jobs(args.status)
    except (ValueError, OSError, argparse.ArgumentTypeError) as error:
        parser.exit(2, f"cylc-monitor-queue: {error}\n")
    if result is None:
        return 1
    print(json.dumps(result, indent=2))
    return 0


def run() -> None:
    """Entry point: exit with main's status."""
    sys.exit(main())
