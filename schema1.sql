-- Add the columns to the parent table; PostgreSQL handles the partitions automatically.
ALTER TABLE ping_logs
ADD COLUMN latitude NUMERIC(9,6) NOT NULL DEFAULT 0.0,
ADD COLUMN longitude NUMERIC(9,6) NOT NULL DEFAULT 0.0;

-- Optional: Add an index for faster geospatial filtering later
CREATE INDEX idx_ping_logs_geo ON ping_logs (latitude, longitude);
