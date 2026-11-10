"""Datasette plugin: the Cylc workflows' current state, from the monitor's events and heartbeats.

    /-/cylc                    every workflow: its scheduler's health, a grid of cycles by
                               tasks coloured by each task's latest event, what's running,
                               what has failed (with the end of its job.err), recent events
    /-/cylc?workflow=NAME      one workflow
    /-/cylc.json               the same, as JSON (what cylc_monitor_status.py reads)

Only actors who may view the events table see it (the browser login, the reader token).
The scheduler is `alive` while its heartbeat is `running` and younger than `stale_intervals`
heartbeat intervals, `silent` if older (the scheduler, its host or the network may be down),
or whatever its last heartbeat said (`stopped`, `scheduler gone`); `unknown` with none.

Settings, in datasette.yaml under `plugins: cylc-status:`:

    columns:            rules folding tasks into grid columns, first match wins:
      - match: 'sw4_.*'     a regular expression the whole task name must match
        column: sw4         the column; may use the match's groups (\\1, \\g<name>)
                        Default: none, so every task is its own column.
    stale_intervals: 3  heartbeat intervals after which a `running` scheduler is `silent`
"""

from __future__ import annotations

import datetime as dt
import re

from datasette import Response, hookimpl
from datasette.resources import TableResource

DB = "cylc"
PLUGIN = "cylc-status"
STALE_INTERVALS = 3
RECENT_EVENTS = 40
# The task's latest event -> the state shown, its order of urgency.
STATES = {
    "failed": "failed",
    "submission failed": "failed",
    "retry": "retrying",
    "submission retry": "retrying",
    "started": "running",
    "submitted": "submitted",
    "succeeded": "succeeded",
}


def parse(timestamp: str | None) -> dt.datetime | None:
    if not timestamp:
        return None
    return dt.datetime.fromisoformat(timestamp.replace("Z", "+00:00"))


def ago(seconds: float | None) -> str:
    if seconds is None:
        return ""
    seconds = int(seconds)
    if seconds < 90:
        return f"{seconds} s"
    if seconds < 5400:
        return f"{seconds // 60} min"
    if seconds < 172800:
        return f"{seconds / 3600:.1f} h"
    return f"{seconds / 86400:.1f} d"


def column_rules(config: dict | None) -> list[tuple[re.Pattern, str]]:
    """The `columns` setting's rules, compiled: (pattern, column template)."""
    return [(re.compile(rule["match"]), str(rule["column"])) for rule in (config or {}).get("columns") or []]


def column(task: str, rules: list[tuple[re.Pattern, str]] = ()) -> str:
    """The grid column a task goes in: the first rule whose pattern matches the whole name, else its own."""
    for pattern, template in rules:
        match = pattern.fullmatch(task)
        if match:
            return match.expand(template)
    return task


def cycle_key(cycle: str) -> tuple:
    return (0, int(cycle), "") if re.fullmatch(r"\d+", cycle or "") else (1, 0, cycle or "")


async def workflow_state(datasette, only: str | None = None) -> dict:
    db = datasette.get_database(DB)
    config = datasette.plugin_config(PLUGIN) or {}
    rules = column_rules(config)
    stale_intervals = config.get("stale_intervals", STALE_INTERVALS)
    now = dt.datetime.now(dt.timezone.utc)
    where, params = ("WHERE workflow = :w", {"w": only}) if only else ("", {})
    heartbeats = {r["workflow"]: dict(r) for r in await db.execute(f"SELECT * FROM heartbeats {where}", params)}
    tasks = [dict(r) for r in await db.execute(f"SELECT * FROM task_states {where}", params)]
    first_seen = {
        (r["workflow"], r["task"]): r["first"]
        for r in await db.execute(f"SELECT workflow, task, min(id) AS first FROM events {where} {'AND' if where else 'WHERE'} task IS NOT NULL GROUP BY workflow, task", params)
    }
    workflow_events = {
        r["workflow"]: dict(r)
        for r in await db.execute(
            f"SELECT workflow, event, received_at, message FROM events WHERE id IN (SELECT max(id) FROM events WHERE task IS NULL GROUP BY workflow) {'AND workflow = :w' if only else ''}",
            params,
        )
    }
    started = {
        (r["workflow"], r["cycle"], r["task"]): r["at"]
        for r in await db.execute(
            f"SELECT workflow, cycle, task, max(received_at) AS at FROM events WHERE event = 'started' {'AND workflow = :w' if only else ''} GROUP BY workflow, cycle, task",
            params,
        )
    }
    names = sorted({t["workflow"] for t in tasks} | set(heartbeats) | set(workflow_events))
    result = []
    for name in names:
        beat = heartbeats.get(name)
        if beat is None:
            health, beat_age = "unknown", None
        else:
            beat_age = (now - parse(beat["seen_at"])).total_seconds()
            stale = beat["status"] == "running" and beat_age > stale_intervals * (beat["interval_s"] or 300)
            health = "silent" if stale else ("alive" if beat["status"] == "running" else beat["status"])
        mine = [t for t in tasks if t["workflow"] == name]
        columns = sorted({column(t["task"], rules) for t in mine}, key=lambda c: min(first_seen.get((name, t["task"]), 0) for t in mine if column(t["task"], rules) == c))
        cycles = {}
        for t in sorted(mine, key=lambda t: cycle_key(t["cycle"])):
            row = cycles.setdefault(t["cycle"], {"cycle": t["cycle"], "label": t["label"], "cells": {}})
            since = parse(t["since"])
            run_from = parse(started.get((name, t["cycle"], t["task"])))
            row["cells"][column(t["task"], rules)] = {
                "task": t["task"],
                "state": STATES.get(t["state"], t["state"]),
                "event": t["state"],
                "since": t["since"],
                "for": ago((now - since).total_seconds()) if since else "",
                "running_for": ago((now - run_from).total_seconds()) if run_from and t["state"] == "started" else "",
                "try_num": t["try_num"],
                "job_id": t["job_id"],
            }
        counts = {}
        for t in mine:
            state = STATES.get(t["state"], t["state"])
            counts[state] = counts.get(state, 0) + 1
        problems = [
            {k: t[k] for k in ("cycle", "label", "task", "state", "try_num", "job_id", "since", "log_tail")}
            for t in mine
            if STATES.get(t["state"]) in ("failed", "retrying")
        ]
        running = [
            {"cycle": c["cycle"], "label": c["label"], **cell}
            for c in cycles.values()
            for cell in c["cells"].values()
            if cell["state"] in ("running", "submitted")
        ]
        last = workflow_events.get(name)
        result.append({
            "workflow": name,
            "health": health,
            "heartbeat": beat and {**beat, "age": ago(beat_age)},
            "last_workflow_event": last,
            "stalled": bool(last and last["event"] in ("stall", "abort")),
            "counts": counts,
            "columns": columns,
            "cycles": list(cycles.values()),
            "running": running,
            "problems": problems,
            "last_activity": max([t["since"] for t in mine] + ([beat["seen_at"]] if beat else []), default=None),
        })
    result.sort(key=lambda w: w["last_activity"] or "", reverse=True)
    recent = [
        dict(r)
        for r in await db.execute(
            f"SELECT id, received_at, workflow, cycle, label, task, event, try_num, job_id, message FROM events {where} ORDER BY id DESC LIMIT {RECENT_EVENTS}",
            params,
        )
    ]
    return {"generated_at": now.strftime("%Y-%m-%dT%H:%M:%SZ"), "workflows": result, "recent_events": recent}


async def allowed(datasette, request) -> None:
    await datasette.ensure_permission(action="view-table", resource=TableResource(database=DB, table="events"), actor=request.actor)


async def status_page(datasette, request):
    await allowed(datasette, request)
    state = await workflow_state(datasette, request.args.get("workflow"))
    return Response.html(await datasette.render_template("cylc_status.html", {"state": state, "only": request.args.get("workflow")}, request=request))


async def status_json(datasette, request):
    await allowed(datasette, request)
    return Response.json(await workflow_state(datasette, request.args.get("workflow")))


@hookimpl
def register_routes():
    return [(r"^/-/cylc$", status_page), (r"^/-/cylc\.json$", status_json)]


@hookimpl
def menu_links(datasette, actor):
    async def links():
        if await datasette.allowed(action="view-table", resource=TableResource(database=DB, table="events"), actor=actor):
            return [{"href": datasette.urls.path("/-/cylc"), "label": "Workflow status"}]
        return []

    return links
