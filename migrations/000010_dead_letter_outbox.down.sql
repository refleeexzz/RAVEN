-- lock in writer order so rollback cannot discard an in-flight transition.
LOCK TABLE jobs IN SHARE ROW EXCLUSIVE MODE;
LOCK TABLE job_dead_letter_outbox IN ACCESS EXCLUSIVE MODE;
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM job_dead_letter_outbox) THEN
        RAISE EXCEPTION 'drain job_dead_letter_outbox before rolling back migration 000010';
    END IF;
END $$;

DROP TRIGGER jobs_record_dead_letter ON jobs;
DROP FUNCTION record_dead_letter();
DROP TABLE job_dead_letter_outbox;
