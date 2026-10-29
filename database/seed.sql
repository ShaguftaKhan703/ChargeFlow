
-- ChargeFlow sample charging stations

INSERT INTO stations (name, location)
VALUES
    ('Downtown Charging Hub', 'San Jose'),
    ('University Charging Station', 'San Jose'),
    ('Central EV Charging Hub', 'Santa Clara');

-- Create two chargers for each station

INSERT INTO chargers (station_id, status)
SELECT station_id, 'available'
FROM stations
CROSS JOIN generate_series(1, 2)
WHERE name IN (
    'Downtown Charging Hub',
    'University Charging Station',
    'Central EV Charging Hub'
);

-- Sample drivers (needed because reservations reference users)

INSERT INTO users (name, email)
SELECT 'Driver ' || n, 'driver' || n || '@example.com'
FROM generate_series(1, 20) AS n;
