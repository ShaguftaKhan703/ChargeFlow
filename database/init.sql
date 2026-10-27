
-- ChargeFlow database schema

CREATE TABLE IF NOT EXISTS users (
    user_id SERIAL PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    email VARCHAR(150) UNIQUE NOT NULL
);

CREATE TABLE IF NOT EXISTS stations (
    station_id SERIAL PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    location VARCHAR(200) NOT NULL
);

CREATE TABLE IF NOT EXISTS chargers (
    charger_id SERIAL PRIMARY KEY,
    station_id INTEGER NOT NULL
        REFERENCES stations(station_id),
    status VARCHAR(20) NOT NULL DEFAULT 'available',
    CHECK (status IN ('available', 'reserved', 'charging', 'offline'))
);

CREATE TABLE IF NOT EXISTS reservations (
    reservation_id SERIAL PRIMARY KEY,
    user_id INTEGER NOT NULL REFERENCES users(user_id),
    charger_id INTEGER NOT NULL REFERENCES chargers(charger_id),
    start_time TIMESTAMPTZ NOT NULL,
    end_time TIMESTAMPTZ NOT NULL,
    status VARCHAR(20) NOT NULL DEFAULT 'confirmed',
    CHECK (end_time > start_time),
    CHECK (status IN ('confirmed', 'cancelled', 'completed'))
);

CREATE TABLE IF NOT EXISTS queue_entries (
    queue_id SERIAL PRIMARY KEY,
    user_id INTEGER NOT NULL REFERENCES users(user_id),
    station_id INTEGER NOT NULL REFERENCES stations(station_id),
    joined_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    status VARCHAR(20) NOT NULL DEFAULT 'waiting',
    CHECK (status IN ('waiting', 'assigned', 'cancelled'))
);

-- Prevent two active queue entries for the same
-- driver at the same station.
CREATE UNIQUE INDEX IF NOT EXISTS unique_waiting_user_station
ON queue_entries (user_id, station_id)
WHERE status = 'waiting';

-- Prevent overlapping confirmed reservations
-- for the same charger.
CREATE EXTENSION IF NOT EXISTS btree_gist;

ALTER TABLE reservations
ADD CONSTRAINT no_overlapping_confirmed_reservations
EXCLUDE USING gist (
    charger_id WITH =,
    tstzrange(start_time, end_time, '[)') WITH &&
)
WHERE (status = 'confirmed');
