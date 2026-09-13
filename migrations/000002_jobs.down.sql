-- Migration 000002 down: drop the jobs schema.

DROP TABLE IF EXISTS job_attempts;
DROP TABLE IF EXISTS jobs;
