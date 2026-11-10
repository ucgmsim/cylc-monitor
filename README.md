# cylc-monitor

Follow a Cylc 8 workflow from outside the cluster. A Cylc main-loop plugin, running inside
the scheduler, uploads the workflow's own database (`log/db`) to a
[Datasette](https://datasette.io) whenever it changes. The Datasette merges every
workflow's uploads into one database, with a dashboard of the jobs queued, running and
finished: what each asked for, what it used, and its core-hours and RSUs. Anyone with a
read token can follow the run there without logging in to the cluster.

## Why

On REANNZ (NeSI), the AI-agent rules allow an agent on the cluster only while a person
supervises it live. So an agent such as Claude Code may not log in to check on a
long-running workflow. The workflow can still report its own state. Cylc already records
everything about its jobs in its public database, so the scheduler only has to send a
copy of that out to a server you control. The agent then reads that server with a token
that can only read. There are no push alerts: you look at the dashboard, or run
`cylc-monitor-status`.

Nothing here is specific to REANNZ or to one workflow.

## How it fits together

```
scheduler (cluster)                              server (e.g. a VPS; reverse proxy -> 127.0.0.1:8017)
  `cylc monitor` main-loop plugin                  Datasette, cylc.db
    every 30 s: has log/db or the       -- POST -->  /-/cylc/upload   merge the snapshot   (server/plugins/cylc_upload.py)
    scheduler's state changed? If so,   snapshot     task_jobs, task_events, ... + workflow column (tables)
    or 5 min since the last: snapshot   (gzip)       jobs, workflows                               (views, server/schema.sql)
    it and upload, in a thread                       /-/dashboards/cylc                            (datasette-dashboards)
                                                          ^
  anywhere: cylc-monitor-status, a browser  --------------+  (read only)
```

- `cylc_monitor/`: the Python package, on PyPI as `cylc-monitor`. It uses only the
  standard library, plus Cylc's own API for the plugin.
  - `mainloop.py`: the `cylc monitor` main-loop plugin.
  - `upload.py`: the snapshot and upload. Also the command `cylc-monitor-upload`, which
    uploads one snapshot by hand.
  - `status.py`: the command `cylc-monitor-status`, the reader. It prints each active
    workflow's state and exits 1 if something needs a person.
- `server/`: the Datasette side, run with Docker Compose. It has the views
  (`schema.sql`), the permissions and dashboard (`datasette.yaml`), the upload plugin,
  a token maker and an example Caddy site.
- `examples/global.cylc`: the Cylc configuration that turns the plugin on.

Datasette is pinned to 1.0a41 (for its tokens), datasette-auth-passwords to 1.1.1 and
datasette-dashboards to 0.8.0. datasette-dashboards 0.8.0 still calls an API that
Datasette 1.0 removed, so `server/plugins/permission_compat.py` maps it onto the new one.

## Installing the plugin

The plugin runs inside the scheduler, so it must be installed into the Python
environment Cylc runs from. Cylc finds main-loop plugins through Python entry points:
putting it on `PYTHONPATH` doesn't work. It needs Cylc 8.7 or later, on Python 3.12 or
later.

1. **Install it beside Cylc.** If you installed Cylc with uv, reinstall it with the
   plugin:
   ```sh
   uv tool install cylc-flow==8.7.0 --with cylc-monitor
   ```
   Keep the Cylc version you already run. With pip, in Cylc's own environment:
   `pip install cylc-monitor`. Either way, check that Cylc can see the plugin:
   ```sh
   "$(dirname "$(readlink -f "$(command -v cylc)")")/python" -c \
       "from importlib.metadata import entry_points; print(entry_points(group='cylc.main_loop')['cylc_monitor'])"
   ```
   If Cylc is a shared install you can't change, such as a site module, ask its
   maintainers. You can also run your own Cylc with uv, as above.
2. **Tell it where the monitor is.** Write the server's URL, and the writer token from
   the server (see [Deploying the server](#deploying-the-server)), on the scheduler
   host:
   ```sh
   mkdir -p ~/.config/cylc-monitor && chmod 700 ~/.config/cylc-monitor
   echo https://cylc.example.org > ~/.config/cylc-monitor/url
   # the token: see step 5 of Deploying the server
   chmod 600 ~/.config/cylc-monitor/token
   ```
   `CYLC_MONITOR_URL` and `CYLC_MONITOR_TOKEN_FILE`, set where the scheduler starts,
   override the two files.
3. **Turn it on** in `~/.cylc/flow/global.cylc`, as in
   [`examples/global.cylc`](examples/global.cylc):
   ```ini
   [scheduler]
       [[main loop]]
           plugins = cylc monitor
           [[[cylc monitor]]]
               interval = PT30S
   ```
   Every workflow started after this uploads. To monitor only some workflows, put the
   same `[scheduler][[main loop]]` section in their `flow.cylc` instead. A workflow
   that lists the plugin won't start where the plugin isn't installed, so the
   `global.cylc` route keeps a workflow's `flow.cylc` portable. `cylc config -i
   '[scheduler][main loop]'` shows what Cylc reads.
4. **Optionally, turn on Cylc's job profiler** for your Slurm platform (see
   [What each job used](#what-each-job-used-cylcs-profiler)).

A scheduler already running before this picks the plugin up when it restarts.
`cylc stop --now` leaves the jobs running, and `cylc play` restarts the scheduler.
Or upload once by hand: `cylc-monitor-upload --workflow my-flow/run1 --run-dir
~/cylc-run/my-flow/run1`.

The scheduler's log says `[cylc monitor] uploading to <url>` at startup. Without a URL
or token it logs a warning instead and uploads nothing; the workflow runs as before. A
failed upload is logged and retried at the next check.

## The upload

Every `interval`, the scheduler calls the plugin. The plugin uploads in these cases:
- `log/db`'s modification time has changed;
- the scheduler has gone between running, paused and stalled;
- 5 minutes have passed since the last upload.

The plugin builds the snapshot and posts it to `/-/cylc/upload?workflow=<id>` in a
thread, so the scheduler never waits on the network. If an upload is still going at the
next check, that check is skipped.

At a clean shutdown it uploads once more, marked `stopped`. A crash or kill sends
nothing. The dashboard then shows the workflow as `silent` once three heartbeats (15
min) pass without an upload.

The snapshot is a consistent copy of `log/db`, made with SQLite's backup API on a
read-only connection, plus three tables:

- `monitor`: the scheduler's state (`running`, `paused`, `stalled` or `stopped`),
  host, PID, the upload time and the heartbeat interval.
- `job_resources`: each job's `#SBATCH` directives, read from its job script
  (`log/job/<cycle>/<task>/<NN>/job`). That covers:
  - cores (ntasks × cpus-per-task);
  - nodes (the minimum, for a range);
  - memory, time limit, account and partition.
- `job_logs`: the last 20 lines of `job.err` for each failed job.

The server merges the snapshot into its own `cylc` database in one transaction. Each
table (`task_jobs`, `task_states`, `task_pool`, `task_events`, `workflow_params` and the
three above) gets a `workflow` column, and the workflow's old rows are replaced by the
new. An upload is idempotent, a lost one is repaired by the next, and every workflow
can be queried together.

[datasette-upload-dbs](https://datasette.io/plugins/datasette-upload-dbs) looks like
it would do this job, but it doesn't. It fails on Datasette 1.0. On 0.65 a second upload
of the same name truncates the live file in place and registers it as a second
database, `<name>_2`.

### What it costs the cluster

A `stat` of one file every 30 s, from inside the scheduler. When the file has changed:
- a copy of a database of a few hundred KB (200 KB for a 19-job workflow);
- a read of each new job script's header;
- one HTTPS POST of the gzipped snapshot.

It makes no Slurm calls. What a job asked for comes from its job script, and what it
used from Cylc's profiler.

## What each job used: Cylc's profiler

Cylc 8.7 has a job profiler. Turn it on per platform in `global.cylc`:

```ini
[platforms]
    [[my-slurm-platform]]
        [[[profiler]]]
            activate = True
```

Each job then runs `cylc profile` beside its script. The profiler reads the job's
cgroup every 10 s. At the end it sends the scheduler
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

- `jobs`: one row a job (a task's submission). It has:
  - the state: `submitting`, `submitted`, `running`, `succeeded`, `failed` or
    `submit-failed`;
  - time queued and time running;
  - what it asked for, and the profiler's figures;
  - `core_hours` = cores × hours running;
  - `rsu` = (1.4 × cores + 0.20 × GB requested) × hours, REANNZ's rates.

  A job's time so far stops at its workflow's last upload.
- `workflows`: one row a workflow. `health` is the scheduler's last state, or `silent`
  when a scheduler not stopped hasn't uploaded for three heartbeats. It also counts the
  jobs by state and totals core-hours and RSUs.

`/-/dashboards/cylc` shows:
- for active workflows (not stopped): jobs running, queued and failed, and core-hours
  and RSUs;
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
| `cylc-writer` | a token on the scheduler host, `~/.config/cylc-monitor/token` | upload snapshots; the upload plugin checks the actor's id, and the token may otherwise only view the instance |
| `cylc-reader` | a token wherever you read from, `~/.config/cylc-monitor/reader-token` | read tables and views, run read-only SQL, see the dashboard |

Tokens are signed with `DATASETTE_SECRET` and don't expire. Changing the secret revokes
every token.

## What leaves the cluster

- The workflow's public database (`log/db`). That holds task names, cycle points, job
  IDs and times, task outputs and messages, and workflow parameters.
- The `#SBATCH` lines of each job script.
- The last 20 lines of `job.err` of each failed job.
- The scheduler host's name and the scheduler's PID.

If a task's messages or `job.err` may hold something that must not leave the cluster,
don't run the plugin for that workflow.

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

## Reading it

Install the reader anywhere with `uv tool install cylc-monitor` (or `uvx --from
cylc-monitor cylc-monitor-status`), then:

```sh
cylc-monitor-status --url https://cylc.example.org            # active workflows
cylc-monitor-status my-flow/run1 --url https://cylc.example.org
cylc-monitor-status --all --url https://cylc.example.org      # stopped ones too
```

Instead of `--url`, set `CYLC_MONITOR_URL` or write `~/.config/cylc-monitor/url`. The
reader token comes from one of:
- `~/.config/cylc-monitor/reader-token`;
- `--token-file`;
- `CYLC_MONITOR_READER_TOKEN_FILE`.

Datasette's own JSON and SQL work with the same token, for example:
```sh
curl -H "Authorization: Bearer $(cat ~/.config/cylc-monitor/reader-token)" 'https://cylc.example.org/cylc.json?sql=select+*+from+workflows&_shape=array'
```

## Development

```sh
uv sync --all-groups --all-extras
uv run pytest
uv run ruff check . && uv run ruff format --check . && uv run deptry . && uv run ty check
```

CI runs the same checks, plus numpydoc linting, through
[ucgmsim/meta-ci-action](https://github.com/ucgmsim/meta-ci-action). Publishing a GitHub
release builds the package and publishes it to PyPI (`.github/workflows/publish-PyPI.yml`).
The version comes from the release's tag (setuptools-scm).

`tests/data/run` is a real Cylc 8.7.0 run directory, its `log/db` and job scripts.
The run had the profiler on and a stand-in Slurm. The tests cover:
- `tests/test_mainloop.py`: the plugin, driven as the scheduler drives it.
- `tests/test_server.py`: it starts the server on a temporary database and checks:
  - the permissions;
  - that a re-upload replaces rather than adds;
  - the `jobs` view;
  - that every dashboard query runs;
  - the status reader.
