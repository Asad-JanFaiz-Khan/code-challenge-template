-- SQL schema for the Weather API (generated to reflect `submission/models.py`)
-- Works for SQLite; adapt types for other RDBMS if needed.

BEGIN TRANSACTION;

CREATE TABLE IF NOT EXISTS weather_stations (
    id INTEGER PRIMARY KEY,
    station_id TEXT NOT NULL UNIQUE,
    state TEXT
);

CREATE TABLE IF NOT EXISTS weather_records (
    id INTEGER PRIMARY KEY,
    station_id INTEGER NOT NULL,
    observation_date DATE NOT NULL,
    max_temperature_tenths_celsius INTEGER,
    min_temperature_tenths_celsius INTEGER,
    precipitation_tenths_mm INTEGER,
    FOREIGN KEY(station_id) REFERENCES weather_stations(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_station_date ON weather_records(station_id, observation_date);
CREATE INDEX IF NOT EXISTS idx_observation_date ON weather_records(observation_date);
CREATE INDEX IF NOT EXISTS idx_station_id ON weather_records(station_id);

CREATE TABLE IF NOT EXISTS crop_yield (
    id INTEGER PRIMARY KEY,
    year INTEGER NOT NULL UNIQUE,
    yield_amount INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS yearly_station_stats (
    id INTEGER PRIMARY KEY,
    station_id INTEGER NOT NULL,
    year INTEGER NOT NULL,
    avg_max_celsius REAL,
    avg_min_celsius REAL,
    total_precip_cm REAL,
    FOREIGN KEY(station_id) REFERENCES weather_stations(id) ON DELETE CASCADE
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_stats_station_year ON yearly_station_stats(station_id, year);

COMMIT;
