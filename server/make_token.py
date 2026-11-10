"""Print a Datasette API token for the Cylc monitor, signed with DATASETTE_SECRET.

    python /app/make_token.py writer   # for cylc_monitor_upload.py on the scheduler host
    python /app/make_token.py reader   # for whoever follows the runs off the cluster

The writer's only use is POST /-/cylc/upload (the plugin checks its actor id; the token
itself may only view the instance); the reader may only read (tables, views and
read-only SQL, hence the dashboard). Neither expires; rotate
DATASETTE_SECRET to revoke every token at once.
"""

import os
import subprocess
import sys

TOKENS = {
    "writer": ["cylc-writer", "--all", "view-instance"],
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
    subprocess.run(
        ["datasette", "create-token", *TOKENS[sys.argv[1]], "--secret", secret],
        check=True,
    )


if __name__ == "__main__":
    main()
