-- the migration runner holds this lock through commit. writers already in
-- flight finish before the backfill; later writers see the trigger.
LOCK TABLE jobs IN SHARE ROW EXCLUSIVE MODE;

CREATE FUNCTION record_queued_dispatch() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF TG_OP = 'UPDATE' AND OLD.status = NEW.status
        AND OLD.execution_generation = NEW.execution_generation THEN
        RETURN NULL;
    END IF;

    -- current services record the intent themselves. older schedulers copy
    -- theirs later in the same transaction. defer this check until commit so
    -- either path can supply it. recheck the job in case it was cancelled or
    -- deleted again by another statement in this transaction.
    INSERT INTO scheduled_job_outbox AS pending (job_id, execution_generation)
    SELECT j.id, j.execution_generation FROM jobs j
    WHERE j.id = NEW.id AND j.status = 'QUEUED'
      AND j.execution_generation = NEW.execution_generation
      AND NOT EXISTS (
          SELECT 1 FROM scheduled_job_outbox o
          WHERE o.job_id = j.id AND o.execution_generation >= j.execution_generation
      )
    ON CONFLICT (job_id) DO UPDATE
        SET execution_generation = EXCLUDED.execution_generation,
            queued_at = EXCLUDED.queued_at
        WHERE pending.execution_generation < EXCLUDED.execution_generation;
    RETURN NULL;
END $$;

CREATE CONSTRAINT TRIGGER jobs_queued_dispatch
    AFTER INSERT OR UPDATE OF status ON jobs
    DEFERRABLE INITIALLY DEFERRED FOR EACH ROW
    WHEN (NEW.status = 'QUEUED')
    EXECUTE FUNCTION record_queued_dispatch();

-- a legacy queued row might already be on the broker. replaying its current
-- generation is safe under the worker claim fence.
INSERT INTO scheduled_job_outbox AS pending (job_id, execution_generation)
SELECT id, execution_generation FROM jobs WHERE status = 'QUEUED'
ON CONFLICT (job_id) DO UPDATE
    SET execution_generation = EXCLUDED.execution_generation,
        queued_at = EXCLUDED.queued_at
    WHERE pending.execution_generation < EXCLUDED.execution_generation;
