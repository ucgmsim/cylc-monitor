"""Datasette plugin: take a snapshot of a Cylc workflow's database and merge it into the monitor's.

    POST /-/cylc/upload?workflow=NAME     body: the snapshot (SQLite, optionally gzipped)

Sent by the `cylc monitor` main-loop plugin on the scheduler host, with the cylc-writer
token; any other actor gets 403. The snapshot is Cylc's public database (log/db) plus the
uploader's own monitor, job_resources and job_logs tables. Each of the TABLES below that
it holds replaces that workflow's rows of the same table in the monitor's `cylc`
database, with a `workflow` column added, in one transaction: an upload is idempotent,
and readers see the old rows or the new, never a mixture. A column a later Cylc adds is
added to the merged table. Then schema.sql (the views) is run again, as it is at startup.

The snapshot never becomes a Datasette database of its own, so re-uploading updates the
workflow rather than adding a second copy, and every workflow can be queried together.
"""

import gzip
import re
import sqlite3
import tempfile
from collections.abc import Awaitable, Callable
from pathlib import Path

from datasette import Forbidden, Response, hookimpl
from datasette.app import Datasette
from datasette.utils.asgi import Request

DB = "cylc"
WRITER = "cylc-writer"
SCHEMA = Path(__file__).resolve().parent.parent / "schema.sql"
# Cylc's tables worth keeping (its public database has others), then the uploader's own.
TABLES = [
    "task_jobs",
    "task_states",
    "task_pool",
    "task_events",
    "workflow_params",
    "monitor",
    "job_resources",
    "job_logs",
]
WORKFLOW = re.compile(r"[\w][\w.\-/]{0,200}")
MAX_BYTES = 256 * 1024 * 1024
SQLITE_HEADER = b"SQLite format 3\x00"


def quote(name: str) -> str:
    """Quote an SQL identifier.

    Parameters
    ----------
    name : str
        A table or column name.

    Returns
    -------
    str
        The name in double quotes, any inside doubled.
    """
    return '"' + name.replace('"', '""') + '"'


def merge(conn: sqlite3.Connection, snapshot: str, workflow: str) -> dict[str, int]:
    """Replace the workflow's rows of each table with the snapshot's, in one transaction.

    Parameters
    ----------
    conn : sqlite3.Connection
        The monitor database's write connection.
    snapshot : str
        The uploaded snapshot's path.
    workflow : str
        The workflow's ID.

    Returns
    -------
    dict[str, int]
        The rows now held for the workflow, by table.

    Raises
    ------
    ValueError
        If the snapshot isn't one of a Cylc workflow's database.
    """
    conn.execute("attach database ? as up", (f"file:{snapshot}?mode=ro",))
    try:
        uploaded = {r[0] for r in conn.execute("select name from up.sqlite_master where type = 'table'")}
        if "task_jobs" not in uploaded or "monitor" not in uploaded:
            raise ValueError("not a snapshot of a Cylc workflow database")
        counts = {}
        conn.execute("begin immediate")
        try:
            for table in TABLES:
                if table not in uploaded:
                    continue
                columns = [r[1] for r in conn.execute(f"pragma up.table_info({quote(table)})") if r[1] != "workflow"]
                existing = [r[1] for r in conn.execute(f"pragma main.table_info({quote(table)})")]
                if not existing:
                    conn.execute(
                        f"create table main.{quote(table)} (workflow text not null, {', '.join(map(quote, columns))})"
                    )
                    conn.execute(f"create index main.{quote(table + '_workflow')} on {quote(table)} (workflow)")
                for column in columns:
                    if existing and column not in existing:
                        conn.execute(f"alter table main.{quote(table)} add column {quote(column)}")
                names = ", ".join(map(quote, columns))
                conn.execute(f"delete from main.{quote(table)} where workflow = ?", (workflow,))
                cursor = conn.execute(
                    f"insert into main.{quote(table)} (workflow, {names}) select ?, {names} from up.{quote(table)}",
                    (workflow,),
                )
                counts[table] = cursor.rowcount
            conn.execute("commit")
        except BaseException:
            conn.execute("rollback")
            raise
    finally:
        conn.execute("detach database up")
    views(conn)
    return counts


async def upload(request: Request, datasette: Datasette) -> Response:
    """Handle POST /-/cylc/upload: merge the snapshot in the body.

    Parameters
    ----------
    request : Request
        The request: the cylc-writer's, with ?workflow= and the snapshot as its body.
    datasette : Datasette
        The Datasette instance.

    Returns
    -------
    Response
        JSON: ok, and the rows held by table, or the error.

    Raises
    ------
    Forbidden
        If the actor isn't the cylc-writer.
    """
    if (request.actor or {}).get("id") != WRITER:
        raise Forbidden("only the cylc-writer token may upload")
    if request.method != "POST":
        return Response.json({"ok": False, "error": "POST a snapshot"}, status=405)
    workflow = request.args.get("workflow", "")
    if not WORKFLOW.fullmatch(workflow):
        return Response.json({"ok": False, "error": "bad or missing ?workflow="}, status=400)
    body = await request.post_body()
    if body[:2] == b"\x1f\x8b":
        body = gzip.decompress(body)
    if len(body) > MAX_BYTES or not body.startswith(SQLITE_HEADER):
        return Response.json({"ok": False, "error": "not an SQLite database, or too big"}, status=400)
    db = datasette.get_database(DB)
    with tempfile.NamedTemporaryFile(suffix=".db", dir=Path(db.path).parent) as snapshot:
        snapshot.write(body)
        snapshot.flush()
        try:
            counts = await db.execute_write_fn(lambda conn: merge(conn, snapshot.name, workflow), transaction=False)
        except (sqlite3.Error, ValueError) as error:
            return Response.json({"ok": False, "error": str(error)}, status=400)
    return Response.json({"ok": True, "workflow": workflow, "rows": counts})


def views(conn: sqlite3.Connection) -> None:
    """(Re)make the views, once a workflow has uploaded (they need its tables).

    Parameters
    ----------
    conn : sqlite3.Connection
        The monitor database's write connection.
    """
    if conn.execute("select 1 from sqlite_master where name = 'monitor'").fetchone():
        with conn:
            for statement in SCHEMA.read_text().split(";\n"):
                if statement.strip():
                    conn.execute(statement)


@hookimpl
def startup(datasette: Datasette) -> Callable[[], Awaitable[None]]:
    """Remake the views at startup, so a redeploy's schema.sql takes effect at once.

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
        await datasette.get_database(DB).execute_write_fn(views, transaction=False)

    return inner


@hookimpl
def register_routes() -> list[tuple[str, Callable]]:
    """Register the upload route.

    Returns
    -------
    list[tuple[str, Callable]]
        The route's pattern and view.
    """
    return [(r"^/-/cylc/upload$", upload)]


@hookimpl
def skip_csrf(scope: dict) -> bool:
    """Skip the CSRF check for uploads: token-authenticated calls carry no CSRF cookie.

    Parameters
    ----------
    scope : dict
        The request's ASGI scope.

    Returns
    -------
    bool
        Whether this is an upload.
    """
    return scope["path"] == "/-/cylc/upload"
