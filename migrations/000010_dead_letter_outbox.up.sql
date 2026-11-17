CREATE TABLE job_dead_letter_outbox (
    job_id text NOT NULL,
    execution_generation integer NOT NULL CHECK (execution_generation > 0),
    job_snapshot jsonb NOT NULL,
    queued_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (job_id, execution_generation)
);

CREATE INDEX idx_job_dead_letter_outbox_order
    ON job_dead_letter_outbox (queued_at, job_id, execution_generation);

CREATE FUNCTION record_dead_letter() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    -- a manual requeue can change the job before the broker comes back.
    -- keep the fields needed to publish this terminal generation.
    INSERT INTO job_dead_letter_outbox (job_id, execution_generation, job_snapshot)
    SELECT NEW.id, NEW.execution_generation, to_jsonb(snapshot)
    FROM (
        SELECT NEW.id, NEW.type, NEW.payload, NEW.status, NEW.priority,
               NEW.attempts, NEW.max_attempts, NEW.owner_id, NEW.created_at,
               NEW.started_at, NEW.finished_at, NEW.error, NEW.worker_id,
               NEW.execution_generation
    ) AS snapshot;
    RETURN NULL;
END $$;

CREATE TRIGGER jobs_record_dead_letter
    AFTER UPDATE OF status ON jobs FOR EACH ROW
    WHEN (NEW.status = 'DEAD' AND OLD.status IS DISTINCT FROM NEW.status)
    EXECUTE FUNCTION record_dead_letter();
