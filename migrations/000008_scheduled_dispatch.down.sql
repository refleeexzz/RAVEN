-- the migration runner executes this file in a transaction. keep writers
-- out between the empty check and drop so rollback cannot discard work.
LOCK TABLE scheduled_job_outbox IN ACCESS EXCLUSIVE MODE;
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM scheduled_job_outbox) THEN
        RAISE EXCEPTION 'drain scheduled_job_outbox before rolling back migration 000008';
    END IF;
END $$;
DROP TABLE scheduled_job_outbox;
