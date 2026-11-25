-- The queue database: one row per earthquake to simulate. SeismicNow adds a row when it
-- deems an event simulatable; the realtime workflow takes the oldest queued row and sets
-- its status as the run goes. Both use Datasette's JSON write API with the queue-writer
-- token (see README.md, "The job queue"). Run at every startup (plugins/queue_setup.py),
-- so it only ever creates what is missing.
--
--   event_id  the GeoNet PublicID; inserting an event already queued is a no-op
--   task_id   whatever the worker running it calls it, e.g. realtime/run1//12
--   cmt       the CMT solution, as JSON
--   status    free text; by convention queued, running, succeeded, failed or cancelled
--   detail    free text for the worker: progress, an error, a link to the results
CREATE TABLE IF NOT EXISTS jobs (
    event_id TEXT PRIMARY KEY,
    task_id TEXT,
    cmt TEXT,
    status TEXT NOT NULL DEFAULT 'queued',
    detail TEXT,
    created TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
    updated TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
);

CREATE INDEX IF NOT EXISTS jobs_status_created ON jobs (status, created);

-- Stamp `updated` on any change. Only on changes to the other columns: Datasette turns on
-- recursive_triggers, so a trigger on its own column would fire itself. A column added
-- later must be added to this list (DROP the trigger first; it is only created if missing).
CREATE TRIGGER IF NOT EXISTS jobs_updated AFTER UPDATE OF event_id, task_id, cmt, status, detail, created ON jobs
BEGIN
    UPDATE jobs SET updated = strftime('%Y-%m-%dT%H:%M:%fZ', 'now') WHERE event_id = NEW.event_id;
END;
