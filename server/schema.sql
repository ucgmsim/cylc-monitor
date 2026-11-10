-- The monitor's views, over the tables merged from each workflow's uploads (plugins/cylc_upload.py
-- runs this after every merge). The tables are Cylc's own (task_jobs, task_states, task_pool,
-- task_events, workflow_params) and the uploader's (monitor, job_resources, job_logs), each
-- with a `workflow` column. Statements are separated by ";" at the end of a line.
--
-- A job's time so far stops at its workflow's last upload: the snapshot can't know more.
-- RSUs are REANNZ's rates: 1.4 a core-hour plus 0.20 a GB-hour of memory requested.

DROP VIEW IF EXISTS workflows;
DROP VIEW IF EXISTS jobs;

-- One row a job (a task's submission): its state, what it asked for, what it used.
-- Asked for: the job script's #SBATCH or #PBS directives (job_resources; `runner` says which).
-- For PBS, cores is the select's total ncpus, as PBS counts them: on a site whose ncpus
-- counts both SMT siblings of a core (ESNZ Cascade), that is twice the physical cores, and
-- core_hours, rsu and cpu_efficiency follow it. partition is PBS's queue, and chunks the
-- select's chunk count (nodes only when place=scatter or one chunk). Used: Cylc's job profiler
-- (task_events, when the platform activates it), which reads the cgroup of the node the
-- batch script runs on, so for a multi-node job max_rss_gb and cpu_hours cover the first
-- node only, and cpu_efficiency is given only for jobs that asked for --nodes=1.
CREATE VIEW jobs AS
WITH profile AS (
    SELECT workflow, cycle, name, submit_num,
           json_extract(substr(message, length('_cylc_profiler: ') + 1), '$.max_rss') AS max_rss,
           json_extract(substr(message, length('_cylc_profiler: ') + 1), '$.cpu_time') AS cpu_time_ms,
           json_extract(substr(message, length('_cylc_profiler: ') + 1), '$.memory_allocated') AS memory_allocated
    FROM task_events
    WHERE message LIKE '\_cylc\_profiler: %' ESCAPE '\'
), timed AS (
    SELECT j.workflow, j.cycle, j.name, j.submit_num, j.try_num, j.job_id, j.platform_name AS platform,
           CASE
               WHEN j.submit_status > 0 THEN 'submit-failed'
               WHEN j.submit_status IS NULL THEN 'submitting'
               WHEN j.run_status = 0 THEN 'succeeded'
               WHEN j.run_status IS NOT NULL THEN 'failed'
               WHEN j.time_run IS NOT NULL THEN 'running'
               ELSE 'submitted'
           END AS state,
           j.time_submit, j.time_run, j.time_run_exit,
           round((julianday(coalesce(j.time_run, CASE WHEN j.run_status IS NULL THEN m.uploaded_at END))
                  - julianday(j.time_submit_exit)) * 86400) AS queue_s,
           round((julianday(coalesce(j.time_run_exit, m.uploaded_at)) - julianday(j.time_run)) * 86400) AS run_s,
           r.runner, r.cores, r.nodes, r.chunks, round(r.mem_mb / 1024.0, 2) AS mem_gb, r.time_limit_min,
           r.account, r.partition,
           p.max_rss, p.cpu_time_ms, p.memory_allocated, l.job_err
    FROM task_jobs j
    JOIN monitor m ON m.workflow = j.workflow
    LEFT JOIN job_resources r USING (workflow, cycle, name, submit_num)
    LEFT JOIN profile p USING (workflow, cycle, name, submit_num)
    LEFT JOIN job_logs l USING (workflow, cycle, name, submit_num)
)
SELECT workflow, cycle, name, submit_num, try_num, job_id, platform, runner, state,
       time_submit, time_run, time_run_exit, queue_s, run_s,
       cores, nodes, chunks, mem_gb, time_limit_min, account, partition,
       round(cores * run_s / 3600.0, 2) AS core_hours,
       round((1.4 * cores + 0.20 * coalesce(mem_gb, 0)) * run_s / 3600.0, 2) AS rsu,
       round(max_rss / 1073741824.0, 2) AS max_rss_gb,
       round(nullif(memory_allocated, 0) / 1073741824.0, 2) AS mem_allocated_gb,
       round(cpu_time_ms / 3600000.0, 3) AS cpu_hours,
       CASE WHEN nodes = 1 THEN round(cpu_time_ms / 1000.0 / (cores * run_s), 3) END AS cpu_efficiency,
       job_err
FROM timed;

-- One row a workflow: its scheduler, its jobs by state, what it has used.
-- health: the scheduler's last word (running, paused, stalled, stopped), or `silent` when a
-- scheduler not stopped hasn't uploaded for three heartbeats (it, its host or the network
-- may be down). Active workflows are those not stopped.
CREATE VIEW workflows AS
SELECT m.workflow,
       CASE WHEN m.status != 'stopped' AND (julianday('now') - julianday(m.uploaded_at)) * 86400 > 3 * m.interval_s
            THEN 'silent' ELSE m.status END AS health,
       m.uploaded_at, m.host,
       (SELECT value FROM workflow_params w WHERE w.workflow = m.workflow AND key = 'is_paused') AS is_paused,
       sum(j.state = 'submitted') AS submitted, sum(j.state = 'running') AS running,
       sum(j.state = 'succeeded') AS succeeded, sum(j.state IN ('failed', 'submit-failed')) AS failed,
       round(sum(j.core_hours), 1) AS core_hours, round(sum(j.rsu), 1) AS rsu
FROM monitor m
LEFT JOIN jobs j ON j.workflow = m.workflow
GROUP BY m.workflow;
