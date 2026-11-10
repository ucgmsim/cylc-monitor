"""Print a Datasette API token for the Cylc monitor, signed with DATASETTE_SECRET.

    python /app/make_token.py writer   # for the event handlers on the scheduler host
    python /app/make_token.py reader   # for whoever follows the runs off the cluster

The writer may only add events and upsert heartbeats; the reader may only read
(tables, views and read-only SQL, hence the status page). Neither expires; rotate
DATASETTE_SECRET to revoke every token at once.
"""

import os
import subprocess
import sys

DB = "cylc"
TOKENS = {
    "writer": [
        "cylc-writer",
        "--resource", DB, "events", "insert-row",
        "--resource", DB, "heartbeats", "insert-row",
        "--resource", DB, "heartbeats", "update-row",
    ],
    "reader": [
        "cylc-reader",
        "--all", "view-instance", "--all", "view-database", "--all", "view-table",
        "--all", "view-query", "--all", "execute-sql",
    ],
}  # fmt: skip


def main() -> None:
    if len(sys.argv) != 2 or sys.argv[1] not in TOKENS:
        sys.exit(__doc__)
    secret = os.environ["DATASETTE_SECRET"]
    subprocess.run(["datasette", "create-token", *TOKENS[sys.argv[1]], "--secret", secret], check=True)


if __name__ == "__main__":
    main()
