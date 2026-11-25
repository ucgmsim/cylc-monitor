"""Create the job queue's table (queue.sql) in the queue database at startup.

The queue has no endpoint of its own: SeismicNow and the realtime workflow add and update
rows with Datasette's JSON write API (``/queue/jobs/-/insert`` and
``/queue/jobs/<id>/-/update``), with the queue-writer token, as datasette.yaml
allows. queue.sql only creates what is missing, so restarts keep the queue.
"""

import sqlite3
from collections.abc import Awaitable, Callable
from pathlib import Path

from datasette import hookimpl
from datasette.app import Datasette

DB = "queue"
SCHEMA = Path(__file__).resolve().parent.parent / "queue.sql"


def create(conn: sqlite3.Connection) -> None:
    """Create the queue's table, index and trigger if missing.

    Parameters
    ----------
    conn : sqlite3.Connection
        A connection to the queue database.
    """
    conn.executescript(SCHEMA.read_text())


@hookimpl
def startup(datasette: Datasette) -> Callable[[], Awaitable[None]]:
    """Create the queue's table when the queue database is served.

    Parameters
    ----------
    datasette : Datasette
        The Datasette instance.

    Returns
    -------
    Callable[[], Awaitable[None]]
        The coroutine Datasette awaits.
    """

    async def inner() -> None:
        """Create the table, if the queue database is among those served."""
        if DB in datasette.databases:
            await datasette.get_database(DB).execute_write_fn(create, transaction=False)

    return inner
