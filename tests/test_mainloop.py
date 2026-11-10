"""The `cylc monitor` main-loop plugin, driven as the scheduler drives it, with a stand-in
scheduler over tests/data/run and the upload captured.


"""

from __future__ import annotations

import asyncio
import os
import shutil
from pathlib import Path
from types import SimpleNamespace

import pytest

from cylc_monitor import mainloop as plugin
from cylc_monitor import upload as client

RUN = Path(__file__).resolve().parent / "data" / "run"


@pytest.fixture
def scheduler(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    run = tmp_path / "run"
    shutil.copytree(RUN, run)
    token = tmp_path / "token"
    token.write_text("t")
    monkeypatch.setenv("CYLC_MONITOR_URL", "http://monitor")
    monkeypatch.setenv("CYLC_MONITOR_TOKEN_FILE", str(token))
    uploads = []

    def upload(url, token, workflow, path):
        import sqlite3

        status = sqlite3.connect(path).execute("select status from monitor").fetchone()[0]
        uploads.append((url, workflow, status))

    monkeypatch.setattr(client, "upload", upload)
    return SimpleNamespace(
        workflow="w/run1", workflow_run_dir=str(run), is_stalled=False, is_paused=False, uploads=uploads
    )


async def drive(scheduler, state, checks):
    """Run `check` as the main loop does, letting each upload finish before the next."""
    for _ in range(checks):
        await plugin.check(scheduler, state)
        if state.get("task") is not None:
            await asyncio.wait([state["task"]])
    await plugin.check(scheduler, state)  # collect the last one


def test_uploads_on_change_and_at_shutdown(scheduler) -> None:
    async def run():
        state = {}
        await plugin.begin(scheduler, state)
        await drive(scheduler, state, 3)  # unchanged: one upload, not three
        db = Path(scheduler.workflow_run_dir) / "log" / "db"
        later = db.stat().st_mtime_ns + 10**9
        os.utime(db, ns=(later, later))
        await drive(scheduler, state, 1)  # changed
        scheduler.is_stalled = True
        await drive(scheduler, state, 1)  # stalled: a new status
        await plugin.end(scheduler, state)

    asyncio.run(run())
    assert scheduler.uploads == [
        ("http://monitor", "w/run1", "running"),
        ("http://monitor", "w/run1", "running"),
        ("http://monitor", "w/run1", "stalled"),
        ("http://monitor", "w/run1", "stopped"),
    ]


def test_heartbeat(scheduler, monkeypatch: pytest.MonkeyPatch) -> None:
    async def run():
        state = {}
        await plugin.begin(scheduler, state)
        await drive(scheduler, state, 1)
        state["last"] -= client.HEARTBEAT_S
        await drive(scheduler, state, 1)

    asyncio.run(run())
    assert len(scheduler.uploads) == 2


def test_a_failed_upload_is_retried(scheduler, monkeypatch: pytest.MonkeyPatch) -> None:
    calls = []

    def flaky(*args):
        calls.append(args)
        if len(calls) == 1:
            raise OSError("network down")

    monkeypatch.setattr(client, "upload", flaky)

    async def run():
        state = {}
        await plugin.begin(scheduler, state)
        await drive(scheduler, state, 2)

    asyncio.run(run())
    assert len(calls) == 2


def test_without_settings_it_does_nothing(scheduler, monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.delenv("CYLC_MONITOR_URL")
    monkeypatch.setattr(client, "URL_FILE", str(tmp_path / "no-url"))

    async def run():
        state = {}
        await plugin.begin(scheduler, state)
        await plugin.check(scheduler, state)
        await plugin.end(scheduler, state)

    asyncio.run(run())
    assert scheduler.uploads == []
