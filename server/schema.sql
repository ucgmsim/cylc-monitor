-- The monitor's database: Cylc workflow events and scheduler heartbeats, posted
-- from the scheduler host by cylc_monitor_event.py and cylc_monitor_heartbeat.py.
PRAGMA journal_mode = WAL;

-- One row a Cylc event: task events (submitted, started, succeeded, failed,
-- retry, ...) and workflow events (startup, shutdown, stall, abort).
CREATE TABLE IF NOT EXISTS events (
    id INTEGER PRIMARY KEY,
    received_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
    sent_at TEXT,          -- the scheduler host's clock, UTC
    workflow TEXT NOT NULL,
    cycle TEXT,            -- task events only
    task TEXT,
    label TEXT,            -- what the cycle is, if the workflow says (--label, --labels)
    event TEXT NOT NULL,
    try_num INTEGER,
    submit_num INTEGER,
    job_id TEXT,           -- the batch system's (e.g. Slurm's)
    log_tail TEXT,         -- the end of job.err, for failures and retries
    message TEXT
);
CREATE INDEX IF NOT EXISTS events_workflow_task ON events (workflow, cycle, task, id);
CREATE INDEX IF NOT EXISTS events_received ON events (received_at);

-- One row a workflow: the latest heartbeat from its scheduler host.
CREATE TABLE IF NOT EXISTS heartbeats (
    workflow TEXT PRIMARY KEY,
    seen_at TEXT NOT NULL, -- the scheduler host's clock, UTC
    status TEXT NOT NULL,  -- running, stopped, scheduler gone
    host TEXT,
    pid INTEGER,
    interval_s INTEGER     -- how often it posts while running
);

-- Each task's latest event: where every task of every workflow is now.
CREATE VIEW IF NOT EXISTS task_states AS
SELECT e.workflow, e.cycle, e.label, e.task, e.event AS state, e.try_num, e.submit_num, e.job_id,
       e.received_at AS since, e.log_tail
FROM events e
JOIN (
    SELECT workflow, cycle, task, max(id) AS id FROM events WHERE task IS NOT NULL GROUP BY workflow, cycle, task
) latest ON latest.id = e.id;

-- Each workflow's tasks counted by state, and its heartbeat.
CREATE VIEW IF NOT EXISTS workflow_summary AS
SELECT s.workflow,
       sum(s.state = 'succeeded') AS succeeded,
       sum(s.state IN ('started', 'submitted')) AS active,
       sum(s.state IN ('failed', 'submission failed')) AS failed,
       sum(s.state IN ('retry', 'submission retry')) AS retrying,
       max(s.since) AS last_task_event,
       h.status AS scheduler, h.seen_at AS heartbeat_at, h.host
FROM task_states s
LEFT JOIN heartbeats h ON h.workflow = s.workflow
GROUP BY s.workflow;
