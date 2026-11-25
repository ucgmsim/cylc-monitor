-- The queue database: one row per submission of an earthquake to simulate (a job).
-- SeismicNow adds a job when it deems an event simulatable, and again whenever it wants
-- the event simulated again (with a new CMT, say); the realtime workflow takes the
-- oldest queued job and sets its status as the run goes. Both use Datasette's JSON
-- write API with the queue-writer token (see README.md, "The job queue"). Run at every
-- startup (plugins/queue_setup.py), so it only ever creates what is missing.
--
--   id        the job's, given by the database; oldest first is the queue's order
--   event_id  the GeoNet PublicID; an event has a job for each time it was submitted
--   task_id   whatever the worker running it calls it, e.g. realtime/run1//12
--   cmt       the CMT solution, as JSON
--   status    free text; by convention queued, running, succeeded, failed or cancelled
--   detail    free text for the worker: progress, an error, a link to the results
CREATE TABLE IF NOT EXISTS jobs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    event_id TEXT NOT NULL,
    task_id TEXT,
    cmt TEXT,
    status TEXT NOT NULL DEFAULT 'queued',
    detail TEXT,
    created TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
    updated TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
);

CREATE INDEX IF NOT EXISTS jobs_status ON jobs (status, id);
CREATE INDEX IF NOT EXISTS jobs_event ON jobs (event_id, id);

-- Stamp `updated` on any change. Only on changes to the other columns: Datasette turns on
-- recursive_triggers, so a trigger on its own column would fire itself. A column added
-- later must be added to this list (DROP the trigger first; it is only created if missing).
CREATE TRIGGER IF NOT EXISTS jobs_updated AFTER UPDATE OF event_id, task_id, cmt, status, detail, created ON jobs
BEGIN
    UPDATE jobs SET updated = strftime('%Y-%m-%dT%H:%M:%fZ', 'now') WHERE id = NEW.id;
END;
