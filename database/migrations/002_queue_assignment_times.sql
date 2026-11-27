BEGIN;

ALTER TABLE queue_entries
ADD COLUMN assignment_start_time TIMESTAMPTZ,
ADD COLUMN assignment_end_time TIMESTAMPTZ;

COMMIT;