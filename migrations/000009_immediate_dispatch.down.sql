-- a service using 000008 can finish every pending publication. keep the
-- rows and the table when rolling back coverage for legacy writers.
DROP TRIGGER jobs_queued_dispatch ON jobs;
DROP FUNCTION record_queued_dispatch();
