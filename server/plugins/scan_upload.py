"""Datasette plugin: take a snapshot of one of auto_scan's databases and make it the served one.

    POST /-/scan/upload         body: a snapshot of scan.db (SQLite, optionally gzipped)
    POST /-/scan/upload/cmt     body: a snapshot of cmt.db, the CMT review's

Sent by ``auto-scan push`` with the scan-writer token; any other actor gets 403. The
snapshot must be an SQLite database whose ``meta.schema_version`` this plugin knows
(both databases share auto_scan's schema). Every table of the target database
(``scan`` or ``cmt``) is replaced by the snapshot's, in one transaction that also
remakes its views (``scan_views.sql`` or ``cmt_views.sql``): readers see the old
database or the new, never a mixture. ``scan.db`` is kept whole elsewhere (the state store), so this is
a copy, not a merge: tables the snapshot lacks are dropped.
"""

import gzip
import io
import math
import re
import sqlite3
import tempfile
from collections.abc import Awaitable, Callable
from pathlib import Path
from typing import Any

from datasette import Forbidden, Response, hookimpl
from datasette.app import Datasette
from datasette.utils.asgi import Request

DB = "scan"
WRITER = "scan-writer"
HERE = Path(__file__).resolve().parent.parent
VIEWS = HERE / "scan_views.sql"
# The databases an upload may replace, with the views made over each.
TARGETS = {"scan": VIEWS, "cmt": HERE / "cmt_views.sql"}
# The scan.db layouts this server's views understand (auto_scan.db.SCHEMA_VERSION).
SCHEMA_VERSIONS = {"1"}
# Both before and after gunzip. scan.db is tens of MB; this leaves room to grow.
MAX_BYTES = 200 * 1024 * 1024
SQLITE_HEADER = b"SQLite format 3\x00"
NAME = re.compile(r"[A-Za-z_][A-Za-z0-9_]*")
# The tables a scan.db always has: a snapshot without them is something else.
REQUIRED = {"meta", "events", "runs", "metrics", "findings", "evaluations"}


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


def gunzip_limited(body: bytes, limit: int = MAX_BYTES) -> bytes:
    """Decompress a gzipped body (an uncompressed one is returned as is), refusing more than ``limit`` bytes.

    Parameters
    ----------
    body : bytes
        The request body.
    limit : int, optional
        The most bytes the result may hold.

    Returns
    -------
    bytes
        The decompressed body.

    Raises
    ------
    ValueError
        If the result would exceed ``limit`` or the gzip stream is corrupt.
    """
    if body[:2] != b"\x1f\x8b":
        if len(body) > limit:
            raise ValueError("snapshot too big")
        return body
    out = io.BytesIO()
    try:
        with gzip.GzipFile(fileobj=io.BytesIO(body)) as stream:
            while chunk := stream.read(1024 * 1024):
                out.write(chunk)
                if out.tell() > limit:
                    raise ValueError("snapshot too big")
    except (OSError, EOFError) as error:
        raise ValueError(f"bad gzip: {error}") from error
    return out.getvalue()


def statements(sql: str) -> list[str]:
    """Split an SQL file into statements at a ``;`` that ends a line.

    Parameters
    ----------
    sql : str
        The file's text.

    Returns
    -------
    list of str
        The non-empty statements.
    """
    chunks = sql.split(";\n")
    return [c for c in chunks if any(line.strip() and not line.strip().startswith("--") for line in c.splitlines())]


def _validate(conn: sqlite3.Connection) -> list[tuple[str, str, str]]:
    """Check the attached snapshot ``up`` and list what to copy: (type, name, sql).

    Raises
    ------
    ValueError
        If it is damaged, not a scan.db, or of a schema version not known here.
    """
    check = conn.execute("pragma up.quick_check").fetchone()
    if not check or check[0] != "ok":
        raise ValueError(f"snapshot failed quick_check: {check[0] if check else 'no answer'}")
    objects = conn.execute(
        "select type, name, sql from up.sqlite_master where type in ('table', 'index') and sql is not null"
        " and name not like 'sqlite_%' order by type desc, name"
    ).fetchall()
    tables = {name for kind, name, _ in objects if kind == "table"}
    missing = REQUIRED - tables
    if missing:
        raise ValueError(f"not a scan.db snapshot: no {', '.join(sorted(missing))} table")
    row = conn.execute("select value from up.meta where key = 'schema_version'").fetchone()
    version = None if row is None else str(row[0])
    if version not in SCHEMA_VERSIONS:
        raise ValueError(
            f"schema_version {version!r} is not one this server knows ({', '.join(sorted(SCHEMA_VERSIONS))})"
        )
    keep = []
    for kind, name, sql in objects:
        head = " ".join(sql.split()[:3]).upper()
        if not NAME.fullmatch(name):
            raise ValueError(f"unexpected {kind} name {name!r}")
        if kind == "table" and not head.startswith("CREATE TABLE"):
            continue  # virtual tables and the like: not part of scan.db
        if kind == "index" and not (head.startswith(("CREATE INDEX", "CREATE UNIQUE INDEX"))):
            continue
        keep.append((kind, name, sql))
    return keep


def merge(conn: sqlite3.Connection, snapshot: str, views_sql: str | None = None, database: str = DB) -> dict[str, int]:
    """Replace every table of the database with the snapshot's, and remake the views, in one transaction.

    Parameters
    ----------
    conn : sqlite3.Connection
        The scan database's write connection.
    snapshot : str
        The uploaded snapshot's path.
    views_sql : str, optional
        The views to make, by default the text of ``database``'s views file.
    database : str, optional
        Which database this is (a key of ``TARGETS``), by default ``scan``.

    Returns
    -------
    dict[str, int]
        The rows now held, by table.

    Raises
    ------
    ValueError
        If the snapshot is damaged, isn't a scan.db, or has an unknown schema version.
    """
    views_sql = TARGETS[database].read_text() if views_sql is None else views_sql
    conn.execute("attach database ? as up", (snapshot,))
    try:
        objects = _validate(conn)
        counts: dict[str, int] = {}
        conn.execute("begin immediate")
        try:
            existing = conn.execute(
                "select type, name from main.sqlite_master where type in ('view', 'table') and name not like 'sqlite_%'"
            ).fetchall()
            for kind, name in sorted(existing, key=lambda item: item[0] != "view"):
                conn.execute(f"drop {kind} if exists main.{quote(name)}")
            for kind, name, sql in objects:
                # The stored CREATE statement has no schema name, so it creates in main.
                conn.execute(sql)
                if kind == "table":
                    conn.execute(f"insert into main.{quote(name)} select * from up.{quote(name)}")
                    counts[name] = conn.execute(f"select count(*) from main.{quote(name)}").fetchone()[0]
            for statement in statements(views_sql):
                conn.execute(statement)
            conn.execute("commit")
        except BaseException:
            conn.execute("rollback")
            raise
    finally:
        conn.execute("detach database up")
    return counts


def views(conn: sqlite3.Connection, views_sql: str | None = None, database: str = DB) -> None:
    """(Re)make the views, if the database holds a scan (they need its tables).

    Parameters
    ----------
    conn : sqlite3.Connection
        The database's write connection.
    views_sql : str, optional
        The views to make, by default the text of ``database``'s views file.
    database : str, optional
        Which database this is (a key of ``TARGETS``), by default ``scan``.
    """
    if not conn.execute("select 1 from sqlite_master where type = 'table' and name = 'findings'").fetchone():
        return
    views_sql = TARGETS[database].read_text() if views_sql is None else views_sql
    conn.execute("begin immediate")
    try:
        for statement in statements(views_sql):
            conn.execute(statement)
        conn.execute("commit")
    except BaseException:
        conn.execute("rollback")
        raise


class StdDev:
    """The SQL aggregate ``stdev(x)``: the sample standard deviation (Welford), NULL for fewer than 2 values."""

    def __init__(self) -> None:
        """Start with no values."""
        self.n = 0
        self.mean = 0.0
        self.m2 = 0.0

    def step(self, value: Any) -> None:
        """Add a value; NULLs and non-numbers are skipped.

        Parameters
        ----------
        value : Any
            The value.
        """
        if not isinstance(value, int | float) or isinstance(value, bool) or math.isnan(value):
            return
        self.n += 1
        delta = value - self.mean
        self.mean += delta / self.n
        self.m2 += delta * (value - self.mean)

    def finalize(self) -> float | None:
        """The standard deviation.

        Returns
        -------
        float or None
            The sample standard deviation, or None for fewer than 2 values.
        """
        return math.sqrt(self.m2 / (self.n - 1)) if self.n > 1 else None


def add_functions(conn: sqlite3.Connection) -> None:
    """Register the SQL functions the views use (``stdev``).

    Parameters
    ----------
    conn : sqlite3.Connection
        Any connection that will query the views.
    """
    conn.create_aggregate("stdev", 1, StdDev)  # ty: ignore[invalid-argument-type]


async def upload(request: Request, datasette: Datasette) -> Response:
    """Handle POST /-/scan/upload[/cmt]: make the snapshot in the body the scan (or cmt) database.

    Parameters
    ----------
    request : Request
        The scan-writer's request, with the snapshot as its body.
    datasette : Datasette
        The Datasette instance.

    Returns
    -------
    Response
        JSON: ok and the rows held by table, or the error.

    Raises
    ------
    Forbidden
        If the actor isn't the scan-writer.
    """
    if (request.actor or {}).get("id") != WRITER:
        raise Forbidden("only the scan-writer token may upload")
    name = request.url_vars.get("database") or DB
    if name not in TARGETS:
        return Response.json({"ok": False, "error": f"unknown database {name!r}"}, status=404)
    if request.method != "POST":
        return Response.json({"ok": False, "error": "POST a snapshot"}, status=405)
    length = request.headers.get("content-length")
    if length and length.isdigit() and int(length) > MAX_BYTES:
        return Response.json({"ok": False, "error": "snapshot too big"}, status=413)
    body = await request.post_body()
    try:
        body = gunzip_limited(body)
    except ValueError as error:
        return Response.json({"ok": False, "error": str(error)}, status=400)
    if not body.startswith(SQLITE_HEADER):
        return Response.json({"ok": False, "error": "not an SQLite database"}, status=400)
    try:
        db = datasette.get_database(name)
    except KeyError:
        return Response.json({"ok": False, "error": f"no {name} database: serve /data/{name}.db"}, status=500)
    with tempfile.NamedTemporaryFile(suffix=".db", dir=Path(str(db.path)).parent) as snapshot:
        snapshot.write(body)
        snapshot.flush()
        try:
            counts = await db.execute_write_fn(
                lambda conn: merge(conn, snapshot.name, database=name), transaction=False
            )
        except (sqlite3.Error, ValueError) as error:
            return Response.json({"ok": False, "error": str(error)}, status=400)
    return Response.json({"ok": True, "rows": counts})


@hookimpl
def prepare_connection(conn: sqlite3.Connection) -> None:
    """Give every connection the views' SQL functions.

    Parameters
    ----------
    conn : sqlite3.Connection
        A new connection.
    """
    add_functions(conn)


@hookimpl
def startup(datasette: Datasette) -> Callable[[], Awaitable[None]]:
    """Remake the views at startup, so a redeploy's scan_views.sql takes effect at once.

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
        """Remake the views of each served database the plugin knows."""
        for name in TARGETS:
            if name in datasette.databases:
                await datasette.get_database(name).execute_write_fn(
                    lambda conn, name=name: views(conn, database=name), transaction=False
                )

    return inner


@hookimpl
def register_routes() -> list[tuple[str, Callable]]:
    """Register the upload route, ``/-/scan/upload`` (scan) and ``/-/scan/upload/<database>``.

    Returns
    -------
    list[tuple[str, Callable]]
        The route's pattern and view.
    """
    return [(r"^/-/scan/upload(?:/(?P<database>[a-z]+))?$", upload)]


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
    return scope["path"] == "/-/scan/upload" or scope["path"].startswith("/-/scan/upload/")
