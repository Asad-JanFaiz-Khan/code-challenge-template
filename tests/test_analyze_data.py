import sys
from pathlib import Path
from datetime import date

import pytest

# Ensure submission modules are importable
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'submission'))

import database
import analyze_data
import models


def test_compute_and_store_stats(tmp_path):
    db_file = tmp_path / 'test_analysis.db'
    db_url = f'sqlite:///{db_file}'

    dbm = database.get_database_manager(db_url)
    dbm.init_db()

    session = dbm.get_session()
    try:
        station = models.WeatherStation(station_id='TEST001')
        session.add(station)
        session.flush()
        station_id_val = station.id

        session.add_all([
            models.WeatherRecord(
                station_id=station.id,
                observation_date=date(2020, 1, 1),
                max_temperature_tenths_celsius=250,
                min_temperature_tenths_celsius=50,
                precipitation_tenths_mm=100,
            ),
            models.WeatherRecord(
                station_id=station.id,
                observation_date=date(2020, 1, 2),
                max_temperature_tenths_celsius=300,
                min_temperature_tenths_celsius=100,
                precipitation_tenths_mm=200,
            ),
            models.WeatherRecord(
                station_id=station.id,
                observation_date=date(2020, 1, 3),
                max_temperature_tenths_celsius=-9999,
                min_temperature_tenths_celsius=-9999,
                precipitation_tenths_mm=-9999,
            ),
        ])
        session.commit()
    finally:
        session.close()

    count = analyze_data.compute_and_store_stats(db_url)
    assert count == 1

    session = dbm.get_session()
    try:
        stats = session.query(models.YearlyStationStats).filter_by(station_id=station_id_val, year=2020).one()
        assert pytest.approx(stats.avg_max_celsius, rel=1e-3) == 27.5
        assert pytest.approx(stats.avg_min_celsius, rel=1e-3) == 7.5
        assert pytest.approx(stats.total_precip_cm, rel=1e-3) == 3.0
    finally:
        session.close()
