# cylc-monitor

Follow a Cylc 8 workflow from outside the cluster. A small daemon on the scheduler host
uploads the workflow's own database (`log/db`) to a [Datasette](https://datasette.io)
whenever it changes. The Datasette merges every workflow's uploads into one database,
with a dashboard of the jobs queued, running and finished: what each asked for, what
it used, and core-hours and RSUs. Anyone with a read token can follow the run there
without logging in to the cluster.

## Why

On REANNZ (NeSI), the AI-agent rules allow an agent on the cluster only while a person
supervises it live. So an agent such as Claude Code may not log in to check on a
long-running workflow. The workflow can still report its own state. Cylc already
records everything about its jobs in its public database, so the workflow only has to
send a copy of that out to a server you control. The agent then reads that server with
a token that can only read. There are no push alerts: you look at the dashboard, or
run `cylc-monitor-status`.

Nothing here is specific to REANNZ or to one workflow.

## How it fits together

```
scheduler host (cluster)                         server (e.g. a VPS; reverse proxy -> 127.0.0.1:8017)
  cylc_monitor_upload.py --daemon                  Datasette, cylc.db
    every 30 s: has log/db changed?   --- POST -->   /-/cylc/upload   merge the snapshot   (plugins/cylc_upload.py)
    if so, or 5 min since the last:   snapshot       task_jobs, task_events, ... + workflow column (tables)
    snapshot it and upload (gzip)                    jobs, workflows                               (views, schema.sql)
                                                     /-/dashboards/cylc                            (datasette-dashboards)
                                                          ^
  anywhere: cylc_monitor_status.py, a browser  -----------+  (read only)
```

- `client/`: two single-file Python scripts that use only the standard library and run
  on Python 3.8 or later. Copy them into a workflow's `bin/`, or install the package.
  - `cylc_monitor_upload.py` (`cylc-monitor-upload`), started by the startup handler.
  - `cylc_monitor_status.py` (`cylc-monitor-status`), the reader. It prints each active
    workflow's state and exits 1 if something needs a person.
- `server/`: the Datasette side, run with Docker Compose. It has the views
  (`schema.sql`), the permissions and dashboard (`datasette.yaml`), the upload plugin,
  a token maker and an example Caddy site.

Datasette is pinned to 1.0a41 (its tokens and write API), datasette-auth-passwords to
1.1.1 and datasette-dashboards to 0.8.0. datasette-dashboards 0.8.0 still calls an API
that Datasette 1.0 removed, so `plugins/permission_compat.py` maps it onto the new one.

## The upload

The daemon looks at `log/db`'s modification time every `--check` seconds (30). When it
has changed, or `--heartbeat` seconds (300) have passed, it builds a snapshot and posts
it to `/-/cylc/upload?workflow=<id>`. The snapshot is a consistent copy of `log/db`,
made with SQLite's backup API on a read-only connection, plus three tables:

- `monitor`: the scheduler's state from its contact file (`running`, `stopped` or
  `scheduler gone`), host, PID, the upload time and the heartbeat interval.
- `job_resources`: each job's `#SBATCH` directives, read from its job script
  (`log/job/<cycle>/<task>/<NN>/job`): cores (ntasks × cpus-per-task), nodes (the
  minimum, for a range), memory, time limit, account and partition.
- `job_logs`: the last 20 lines of `job.err` for each failed job.

The server merges the snapshot into its own `cylc` database in one transaction. Each
table (`task_jobs`, `task_states`, `task_pool`, `task_events`, `workflow_params` and the
three above) gets a `workflow` column, and the workflow's old rows are replaced by the
new. An upload is idempotent, a lost one is repaired by the next, and every workflow
can be queried together.

[datasette-upload-dbs](https://datasette.io/plugins/datasette-upload-dbs) looks like
it does this job, but it doesn't. It fails on Datasette 1.0. On 0.65 a second upload of
the same name truncates the live file in place and registers it as a second database,
`<name>_2`.

The daemon stops after the upload that sees the scheduler stopped or gone. It logs to
`log/cylc-monitor-upload.log` in the run directory and keeps its PID in
`log/cylc-monitor-upload.pid`. Only one runs per run directory.

### What it costs the cluster

A `stat` of one file every 30 s. When the file has changed: a copy of a database of a
few hundred KB (200 KB for a 19-job workflow), a read of each new job script's header,
and one HTTPS POST of the gzipped snapshot. It makes no Slurm calls: what a job asked
for comes from its job script, and what it used from Cylc's profiler.

## What each job used: Cylc's profiler

Cylc 8.7 has a job profiler. Turn it on per platform in `global.cylc`:

```ini
[platforms]
    [[mahuika]]
        [[[profiler]]]
            activate = True
```

Each job then runs `cylc profile` beside its script. The profiler reads the job's
cgroup every 10 s and, at the end, sends the scheduler
`_cylc_profiler: {"max_rss": bytes, "cpu_time": ms, "memory_allocated": bytes}`. Cylc
stores that in `task_events`, and the `jobs` view turns it into `max_rss_gb`,
`cpu_hours` and `mem_allocated_gb`. Some limits:

- It runs on the node where the batch script runs. For a multi-node job its memory and
  CPU time cover that first node only, so `cpu_efficiency` is given only for jobs that
  asked for `--nodes=1`.
- It finds the job's cgroup by a path with a six-or-more-digit number in it, such as
  Slurm's `job_<id>`. Background jobs on a desktop don't have one.
- It applies to jobs submitted after the change.

## The views and the dashboard

- `jobs`: one row a job (a task's submission). It has the state (`submitting`,
  `submitted`, `running`, `succeeded`, `failed`, `submit-failed`), time queued and
  running, what it asked for and the profiler's figures. It also has
  `core_hours` = cores × hours running and `rsu` = (1.4 × cores + 0.20 × GB requested)
  × hours, REANNZ's rates. A job's time so far stops at its workflow's last upload.
- `workflows`: one row a workflow. `health` is the scheduler's last state, or `silent`
  when a `running` scheduler hasn't uploaded for three heartbeats. It also counts the
  jobs by state and totals core-hours and RSUs.

`/-/dashboards/cylc` shows:
- for active workflows (`running` or `silent`): jobs running, queued and failed, and
  core-hours and RSUs;
- the workflows;
- the submitted and running jobs;
- the latest finished jobs, asked for against used;
- core-hours by task.

It has a filter for one workflow. The charts are in `server/datasette.yaml`. Any SQL
over the tables and views works too.

## Who can see what

Datasette lets in no anonymous visitors. There are three actors:

| Actor | How | May |
|---|---|---|
| `admin` | a browser, at `/-/login` (password hash in `.env`) | read everything |
| `cylc-writer` | a token on the scheduler host, `~/.config/cylc-monitor/token` | upload snapshots; the plugin checks the actor's id, and the token may otherwise only view the instance |
| `cylc-reader` | a token wherever you read from, `~/.config/cylc-monitor/reader-token` | read tables and views, run read-only SQL, see the dashboard |

Tokens are signed with `DATASETTE_SECRET` and don't expire. Changing the secret
revokes every token.

## What leaves the cluster

- the workflow's public database (`log/db`): task names, cycle points, job IDs and
  times, task outputs and messages, and workflow parameters;
- the `#SBATCH` lines of each job script;
- the last 20 lines of `job.err` of each failed job;
- the scheduler host's name and the scheduler's PID.

If a task's messages or `job.err` may hold something that must not leave the cluster,
don't run the uploader for that workflow.

## Deploying the server

These steps assume a host with Docker Compose and a reverse proxy that handles TLS
(Caddy here). Copy `server/` to the host, for example to `~/cylc-monitor`. Then:

1. **DNS:** point a hostname, such as `cylc.example.org`, at the host.
2. **Secret and password:** `cp env.example .env && chmod 600 .env`.
   - Set `DATASETTE_SECRET` with
     `python3 -c 'import secrets; print(secrets.token_hex(32))'`.
   - Make the `admin` login's hash on your own machine and paste it, single-quoted, as
     `ADMIN_PASSWORD_HASH`:
     ```sh
     uvx --from datasette-auth-passwords==1.1.1 --with datasette==1.0a41 datasette hash-password
     ```
3. **Start it:** `sudo docker compose up -d --build`. To check, run
   `curl -s -o /dev/null -w '%{http_code}\n' http://127.0.0.1:8017/`. It should print
   403: the server is running and closed to anonymous visitors. The database is
   `data/cylc.db`. The dashboard errors until the first workflow uploads.
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

To update the server, copy the changed files over and run
`sudo docker compose up -d --build` again. The views are remade at startup.

## Wiring it into a workflow

Copy `client/cylc_monitor_upload.py` into the workflow's `bin/` and start it from the
startup handler:

```ini
[scheduler]
    [[events]]
        startup handlers = python3 "$CYLC_WORKFLOW_RUN_DIR/bin/cylc_monitor_upload.py" --daemon --url https://cylc.example.org --workflow %(workflow)s
```

That is the only handler. A restart starts the daemon again: the startup event fires
then too. The daemon reads the writer token from `~/.config/cylc-monitor/token`, or
from `--token-file` or `CYLC_MONITOR_TOKEN_FILE`. Without a URL or a token it uploads
nothing, and the workflow runs as before. An upload that fails is logged and retried
at the next check.

To start it by hand for a workflow already running (it doesn't need the scheduler):

```sh
python3 cylc_monitor_upload.py --daemon --url https://cylc.example.org \
    --workflow my-flow/run1 --run-dir ~/cylc-run/my-flow/run1
```

`--once` uploads one snapshot and exits.

## Reading it

```sh
cylc-monitor-status --url https://cylc.example.org            # active workflows
cylc-monitor-status my-flow/run1 --url https://cylc.example.org
cylc-monitor-status --all --url https://cylc.example.org      # stopped ones too
```

Set `CYLC_MONITOR_URL` to drop `--url`. The reader token comes from
`~/.config/cylc-monitor/reader-token`, `--token-file`, or
`CYLC_MONITOR_READER_TOKEN_FILE`. Datasette's own JSON and SQL work with the same
token, for example
`curl -H "Authorization: Bearer $(cat ~/.config/cylc-monitor/reader-token)" 'https://cylc.example.org/cylc.json?sql=select+*+from+workflows&_shape=array'`.

## Development

```sh
uv run pytest
```

`tests/data/run` is a real Cylc 8.7.0 run directory, its `log/db` and job scripts.
The run had the profiler on and a stand-in Slurm. `tests/test_server.py` starts the
server on a temporary database and checks:
- the permissions;
- that a re-upload replaces rather than adds;
- the `jobs` view;
- that every dashboard query runs;
- the status client.

It is skipped when Datasette can't be imported, which means on Python < 3.10.
