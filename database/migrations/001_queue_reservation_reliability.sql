BEGIN;

ALTER TABLE queue_entries ADD COLUMN assignment_id UUID;
ALTER TABLE reservations ADD COLUMN assignment_id UUID;

ALTER TABLE queue_entries
ADD COLUMN reservation_id INTEGER REFERENCES reservations(reservation_id);

ALTER TABLE queue_entries
DROP CONSTRAINT queue_entries_status_check;

ALTER TABLE queue_entries
ADD CONSTRAINT queue_entries_status_check
CHECK (status IN ('waiting', 'assigning', 'assigned', 'cancelled'));

CREATE UNIQUE INDEX unique_queue_assignment_id
ON queue_entries (assignment_id)
WHERE assignment_id IS NOT NULL;

CREATE UNIQUE INDEX unique_reservation_assignment_id
ON reservations (assignment_id)
WHERE assignment_id IS NOT NULL;

DROP INDEX unique_waiting_user_station;

CREATE UNIQUE INDEX unique_active_user_station
ON queue_entries (user_id, station_id)
WHERE status IN ('waiting', 'assigning');

COMMIT;
