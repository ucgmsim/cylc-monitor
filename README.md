# cylc-monitor

A place outside the cluster where a Cylc 8 workflow's event handlers record every task
and workflow event, plus a heartbeat from the scheduler host. It is a
[Datasette](https://datasette.io) with a status page, and anyone with a read token can
follow the run there without logging in to the cluster.

## Why

On REANNZ (NeSI), the AI-agent rules allow an agent on the cluster only while a person
supervises it live. So an agent such as Claude Code may not log in to check on a
long-running workflow. The workflow can still report its own state: Cylc's event
handlers run on the scheduler host as part of the workflow and post each event out to
a server you control. The agent then reads that server with a token that can only read.
There are no push alerts: you look at the page, or run `cylc-monitor-status`.

Nothing here is specific to REANNZ or to one workflow. It suits any cluster where you
want to see a workflow's state from somewhere else.

## How it fits together

```
scheduler host (cluster)                         server (e.g. a VPS; reverse proxy -> 127.0.0.1:8017)
  cylc_monitor_event.py      every task and  --->  Datasette, cylc.db
                             workflow event          events, heartbeats          (tables)
  cylc_monitor_heartbeat.py  every 5 min     --->    task_states, workflow_summary (views)
                                                     /-/cylc       status page (plugins/cylc_status.py)
                                                     /-/cylc.json  the same as JSON
                                                          ^
  anywhere: cylc_monitor_status.py, a browser  -----------+  (read only)
```

- `client/`: three single-file Python scripts that use only the standard library and
  run on Python 3.8 or later. You can copy them into a workflow's `bin/`, or install them.
  - `cylc_monitor_event.py` (`cylc-monitor-event`) is the event handler. Each call posts
    one row to `events`. A workflow `shutdown` also marks the heartbeat `stopped`.
  - `cylc_monitor_heartbeat.py` (`cylc-monitor-heartbeat`) is started by the startup
    handler. It detaches, then upserts the workflow's `heartbeats` row every 300 s. It
    imports `cylc_monitor_event.py`, so copy the two together.
  - `cylc_monitor_status.py` (`cylc-monitor-status`) is the reader. It prints each
    workflow's state from `/-/cylc.json` and exits 1 if something needs a person.
- `server/`: the Datasette side, run with Docker Compose. It has the schema, the
  permissions (`datasette.yaml`), a token maker, the status page plugin and its
  template, and an example Caddy site.

Datasette is pinned to 1.0a41, because the stable 0.65 has no write API, and
datasette-auth-passwords to 1.1.1.

## What the status page shows

- **Each workflow's scheduler:**
  - `alive`: a `running` heartbeat younger than three intervals (15 min by default).
  - `silent`: a `running` heartbeat older than that. The scheduler, its host or the
    network may be down.
  - `stopped`: a clean shutdown.
  - `scheduler gone`: the contact file remains but the scheduler process doesn't.
  - `unknown`: no heartbeat yet.

  It also shows the last workflow event, and flags a `stall` or `abort`.
- **A grid of cycles by tasks.** Each cell shows that task's latest event: succeeded,
  running, submitted, retrying, failed, or not yet. Each cell links to that task's
  events.
- **What is running or submitted,** for how long, and its job ID.
- **What has failed or is retrying,** with the end of its `job.err`.
- **The latest events,** and every table and view for SQL.

By default each task has its own column. To fold tasks into one column, for example
one `sw4_<event>` task per cycle, add rules under `plugins: cylc-status: columns:` in
`server/datasette.yaml`. Each rule's `match` must match the whole task name. The first
matching rule wins, and its `column` may use the match's groups:

```yaml
plugins:
  cylc-status:
    columns:
      - match: 'sw4_.*'
        column: sw4
      - match: '(?P<stage>\w+?)_member\d+'   # forecast_member07 -> forecast
        column: '\g<stage>'
    stale_intervals: 3
```

## Who can see what

Datasette lets in no anonymous visitors. There are three actors:

| Actor | How | May |
|---|---|---|
| `admin` | a browser, at `/-/login` (password hash in `.env`) | read everything |
| `cylc-writer` | a token on the scheduler host, `~/.config/cylc-monitor/token` | insert into `events`, and insert or update `heartbeats`; it may not read, delete, drop or create anything |
| `cylc-reader` | a token wherever you read from, `~/.config/cylc-monitor/reader-token` | read tables and views, run read-only SQL, and see the status page and JSON; it may not write |

Tokens are signed with `DATASETTE_SECRET` and don't expire. Changing the secret
revokes every token.

## What leaves the cluster

Only what the handlers post:

- the workflow ID, cycle point, task name and event
- try and submit numbers, and the batch job ID
- the scheduler host's name and the scheduler's PID
- an optional label and message
- for failures and retries, the last 20 lines (at most 1500 characters) of the
  task's `job.err`

Nothing else is read or sent. If a task's `job.err` may hold something that must not
leave the cluster, leave out that task's failed and retry handlers.

## Deploying the server

These steps assume a host with Docker Compose and a reverse proxy that handles TLS
(Caddy here). Copy `server/` to the host, for example to `~/cylc-monitor`. Then:

1. **DNS:** point a hostname, such as `cylc.example.org`, at the host with `A` and
   `AAAA` records.
2. **Secret and password:** `cp env.example .env && chmod 600 .env`.
   - Set `DATASETTE_SECRET` with
     `python3 -c 'import secrets; print(secrets.token_hex(32))'`.
   - Make the `admin` login's hash on your own machine and paste it, single-quoted, as
     `ADMIN_PASSWORD_HASH`:
     ```sh
     uvx --from datasette-auth-passwords==1.1.1 --with datasette==1.0a41 datasette hash-password
     ```
3. **Start it:** `sudo docker compose up -d --build`. To check, run
   `curl -s -o /dev/null -w '%{http_code}\n' http://127.0.0.1:8017/-/cylc`. It should
   print 403, which means the server is running and closed to anonymous visitors. The
   database is `data/cylc.db`. The `init` service creates the tables and views if they
   are missing, and keeps the existing data.
4. **Reverse proxy:** adapt `Caddyfile.example` to your hostname, append it to
   `/etc/caddy/Caddyfile`, then run
   `sudo caddy validate --config /etc/caddy/Caddyfile && sudo systemctl reload caddy`.
5. **Tokens:** write each token to a file on the server, then move it to where it's
   used:
   ```sh
   cd ~/cylc-monitor
   sudo docker compose exec -T datasette python /app/make_token.py writer > writer.token
   sudo docker compose exec -T datasette python /app/make_token.py reader > reader.token
   chmod 600 *.token
   # From your own machine, through your own sessions (server and cluster are ssh hosts):
   ssh server cat cylc-monitor/writer.token | ssh cluster 'umask 077; mkdir -p ~/.config/cylc-monitor; cat > ~/.config/cylc-monitor/token'
   mkdir -p ~/.config/cylc-monitor
   ssh server cat cylc-monitor/reader.token > ~/.config/cylc-monitor/reader-token
   chmod 600 ~/.config/cylc-monitor/reader-token
   ssh server rm cylc-monitor/writer.token cylc-monitor/reader.token
   ```
6. **Check:** `cylc-monitor-status --url https://cylc.example.org` prints
   `Cylc monitor, <time>` and no workflows yet.

To update the server, copy the changed files over and run
`sudo docker compose up -d --build` again.

## Wiring it into a workflow

Copy `client/cylc_monitor_event.py` and `client/cylc_monitor_heartbeat.py` into the
workflow's `bin/`. The workflow can then run them as
`"$CYLC_WORKFLOW_RUN_DIR/bin/..."` on any host with `python3`. You can instead install
the package where the scheduler runs and call `cylc-monitor-event` and
`cylc-monitor-heartbeat`, if they are on the scheduler's `PATH`.

Some limits on the handler lines:

- Handler lists are comma-separated, so no argument may contain a comma. That is why
  `--labels` uses colons.
- Cylc shell-quotes the `%(...)s` values in task event handlers, but not in workflow
  event handlers. In workflow handlers, pass only values without spaces, such as
  `%(event)s` and `%(workflow)s`.
- Cylc fills in unknown values with `None`, which the scripts treat as not given.

A worked example for `flow.cylc`:

```jinja
#!Jinja2
{# The monitor's URL; --set 'MONITOR_URL=""' posts nothing. #}
{% set MONITOR_URL = MONITOR_URL | default("https://cylc.example.org") %}
{% set BIN = "\"$CYLC_WORKFLOW_RUN_DIR/bin" %}
{% set EVENT = "python3 " ~ BIN ~ "/cylc_monitor_event.py\" --url " ~ MONITOR_URL %}
{# Workflow events: single words only. #}
{% set WORKFLOW_EVENT = EVENT ~ " --event %(event)s --workflow %(workflow)s" %}
{% set HEARTBEAT = "python3 " ~ BIN ~ "/cylc_monitor_heartbeat.py\" --daemon --url " ~ MONITOR_URL ~ " --workflow %(workflow)s" %}
{# Task events. Optionally add: --labels first:second:third (integer cycling) or --label TEXT #}
{% set TASK_EVENT = EVENT ~ " --event %(event)s --workflow %(workflow)s --task %(name)s --point %(point)s --job-id %(job_id)s --try-num %(try_num)s --submit-num %(submit_num)s" %}

[scheduler]
    [[events]]
{% if MONITOR_URL %}
        startup handlers = {{ WORKFLOW_EVENT }}, {{ HEARTBEAT }}
        shutdown handlers = {{ WORKFLOW_EVENT }}
        stall handlers = {{ WORKFLOW_EVENT }}
        abort handlers = {{ WORKFLOW_EVENT }}
{% endif %}

[runtime]
    [[root]]
{% if MONITOR_URL %}
        [[[events]]]
            # Each task's progress...
            handlers = {{ TASK_EVENT }}
            handler events = submitted, started, succeeded
            # ...and its problems; these rows carry the end of job.err.
            failed handlers = {{ TASK_EVENT }}
            submission failed handlers = {{ TASK_EVENT }}
            retry handlers = {{ TASK_EVENT }}
            submission retry handlers = {{ TASK_EVENT }}
{% endif %}
```

A task that sets its own `[[[events]]]` replaces root's `handler events`. If you
change it, keep `submitted, started, succeeded` in the list.

The scripts read the writer token from `~/.config/cylc-monitor/token` on the scheduler
host. You can point them elsewhere with `--token-file` or `CYLC_MONITOR_TOKEN_FILE`.
Without the token they post nothing, and the workflow runs as before. They never fail
the workflow: an event that can't be posted is printed to the handler's log and
dropped.

The heartbeat logs to `log/cylc-monitor-heartbeat.log` in the run directory, and
keeps its PID in `log/cylc-monitor-heartbeat.pid`. Only one runs per run directory. If
the scheduler restarts on the same host while the old heartbeat is alive, the new one
exits. If the scheduler runs on a different host from the one in the contact file, the
heartbeat can't check the scheduler's process. It reports `running`, and the page
relies on the heartbeat's age.

## Reading it

```sh
cylc-monitor-status --url https://cylc.example.org            # every workflow
cylc-monitor-status my-flow/run1 --url https://cylc.example.org
cylc-monitor-status --json --url https://cylc.example.org     # the raw JSON
```

Set `CYLC_MONITOR_URL` to drop `--url`. The reader token comes from
`~/.config/cylc-monitor/reader-token`, `--token-file`, or
`CYLC_MONITOR_READER_TOKEN_FILE`. Datasette's own JSON and SQL work with the same
token, for example
`curl -H "Authorization: Bearer $(cat ~/.config/cylc-monitor/reader-token)" 'https://cylc.example.org/cylc/-/query.json?sql=select+*+from+workflow_summary'`.

## Development

```sh
uv run --with pytest pytest
```

The dev dependency group includes Datasette 1.0a41, for `tests/test_server.py`. That
test starts the server on a temporary database and checks the permissions, the client
against it, column folding, and the four heartbeat states. It is skipped when
Datasette can't be imported, which means on Python < 3.10.
