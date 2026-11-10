"""Cylc main-loop plugin `cylc monitor`: upload the workflow's own database to the Cylc monitor.

Install it into the Python environment Cylc runs from (it registers the `cylc.main_loop`
entry point `cylc_monitor`), then turn it on in global.cylc (or one workflow's flow.cylc):

    [scheduler]
        [[main loop]]
            plugins = cylc monitor
            [[[cylc monitor]]]
                interval = PT30S

Every `interval` the scheduler calls `check`: if log/db or the scheduler's state
(running, paused, stalled) has changed since the last upload, or HEARTBEAT_S (300 s)
have passed, it builds a snapshot and uploads it in a thread, so the scheduler never
waits on the network (a check while an upload is still going is skipped). At a clean
shutdown it uploads once more, marked `stopped`. A crash or kill sends nothing: the
monitor shows the workflow as `silent` once three heartbeats pass without an upload.

The URL and token come from cylc_monitor.upload.settings(): $CYLC_MONITOR_URL or
~/.config/cylc-monitor/url, and ~/.config/cylc-monitor/token (or
$CYLC_MONITOR_TOKEN_FILE). Without them the plugin logs that once and does nothing.
A failed upload is logged and retried at the next check; it never troubles the workflow.
"""

import asyncio
import os
import socket
import tempfile
import time
from pathlib import Path
from typing import Any, Protocol

from cylc.flow import LOG
from cylc.flow.main_loop import periodic, shutdown, startup

from cylc_monitor import upload as client

PREFIX = "[cylc monitor]"


class Scheduler(Protocol):
    """What the plugin uses of Cylc's scheduler (cylc.flow.scheduler.Scheduler)."""

    workflow: str
    workflow_run_dir: str
    is_stalled: bool
    is_paused: bool


def _upload(state: dict[str, Any], scheduler: Scheduler, status: str) -> None:
    """Snapshot the run directory and upload it: blocking, so run it in a thread.

    Parameters
    ----------
    state : dict[str, Any]
        The plugin's state, as `begin` set it up.
    scheduler : Scheduler
        The scheduler.
    status : str
        The scheduler's state to record.
    """
    path = Path(state["tmp"].name) / "snapshot.db"
    path.unlink(missing_ok=True)
    state["snapshot"].build(path, status, socket.gethostname(), os.getpid())
    client.upload(*state["settings"], scheduler.workflow, path)


def _status(scheduler: Scheduler) -> str:
    """Return the scheduler's state.

    Parameters
    ----------
    scheduler : Scheduler
        The scheduler.

    Returns
    -------
    str
        One of stalled, paused or running.
    """
    if getattr(scheduler, "is_stalled", False):
        return "stalled"
    return "paused" if getattr(scheduler, "is_paused", False) else "running"


@startup
async def begin(scheduler: Scheduler, state: dict[str, Any]) -> None:
    """Find the monitor's settings and get ready to upload, at the scheduler's start.

    Parameters
    ----------
    scheduler : Scheduler
        The scheduler.
    state : dict[str, Any]
        The plugin's state, kept by Cylc between calls.
    """
    state["settings"] = client.settings()
    if state["settings"] is None:
        LOG.warning(f"{PREFIX} no monitor URL or token (see ~/.config/cylc-monitor/): not uploading")
        return
    run_dir = Path(scheduler.workflow_run_dir)
    state.update(
        db=run_dir / "log" / "db",
        snapshot=client.Snapshot(run_dir, scheduler.workflow, client.HEARTBEAT_S),
        tmp=tempfile.TemporaryDirectory(prefix="cylc-monitor-"),
        mtime=None,
        status=None,
        last=0.0,
        task=None,
    )
    LOG.info(f"{PREFIX} uploading to {state['settings'][0]}")


@periodic
async def check(scheduler: Scheduler, state: dict[str, Any]) -> None:
    """Start an upload if the database or the scheduler's state changed, or one is due.

    Parameters
    ----------
    scheduler : Scheduler
        The scheduler.
    state : dict[str, Any]
        The plugin's state, kept by Cylc between calls.
    """
    if not state.get("settings"):
        return
    task = state["task"]
    if task is not None:
        if not task.done():
            return  # the last upload is still going
        error = task.exception()
        if error is not None:
            LOG.warning(f"{PREFIX} upload failed: {error}")
        else:
            state.update(mtime=state["pending"][0], status=state["pending"][1], last=state["pending"][2])
        state["task"] = None
    try:
        mtime = state["db"].stat().st_mtime_ns
    except OSError:
        return
    status = _status(scheduler)
    if mtime == state["mtime"] and status == state["status"] and time.monotonic() - state["last"] < client.HEARTBEAT_S:
        return
    state["pending"] = (mtime, status, time.monotonic())
    loop = asyncio.get_running_loop()
    state["task"] = asyncio.ensure_future(loop.run_in_executor(None, _upload, state, scheduler, status))


@shutdown
async def end(scheduler: Scheduler, state: dict[str, Any]) -> None:
    """Upload once more, marked stopped, at a clean shutdown.

    Parameters
    ----------
    scheduler : Scheduler
        The scheduler.
    state : dict[str, Any]
        The plugin's state, kept by Cylc between calls.
    """
    if not state.get("settings"):
        return
    if state["task"] is not None:
        await asyncio.wait([state["task"]], timeout=60)
    loop = asyncio.get_running_loop()
    try:
        await asyncio.wait_for(loop.run_in_executor(None, _upload, state, scheduler, "stopped"), 60)
    except Exception as error:  # noqa: BLE001 - shutting down regardless
        LOG.warning(f"{PREFIX} final upload failed: {error}")
    state["tmp"].cleanup()
