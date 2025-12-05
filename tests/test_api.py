import sys
from pathlib import Path
from datetime import date

import pytest

# Ensure submission modules are importable
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'submission'))

from app import create_app
import database
import models


@pytest.fixture
def client(tmp_path):
    db_file = tmp_path / 'api_test.db'
    db_url = f'sqlite:///{db_file}'

    # prepare DB and small sample data
    dbm = database.get_database_manager(db_url)
    dbm.init_db()
    session = dbm.get_session()
    try:
        s = models.WeatherStation(station_id='TESTST01')
        session.add(s)
        session.flush()
        session.add_all([
            models.WeatherRecord(
                station_id=s.id,
                observation_date=date(2020, 1, 1),
                max_temperature_tenths_celsius=250,
                min_temperature_tenths_celsius=50,
                precipitation_tenths_mm=100,
            ),
            models.WeatherRecord(
                station_id=s.id,
                observation_date=date(2020, 1, 2),
                max_temperature_tenths_celsius=300,
                min_temperature_tenths_celsius=100,
                precipitation_tenths_mm=200,
            ),
        ])
        session.commit()
    finally:
        session.close()

    # run analysis to create stats
    from analyze_data import compute_and_store_stats
    compute_and_store_stats(db_url)

    app = create_app(database_url=db_url)
    app.testing = True
    with app.test_client() as client:
        yield client


def test_health(client):
    r = client.get('/openapi.json')
    assert r.status_code == 200


def test_get_weather(client):
    r = client.get('/api/weather?limit=2')
    assert r.status_code == 200
    payload = r.get_json()
    assert 'data' in payload
    assert payload['pagination']['returned'] <= 2


def test_get_stats(client):
    r = client.get('/api/weather/stats?limit=5')
    assert r.status_code == 200
    payload = r.get_json()
    assert 'data' in payload
    # at least one stat row should be present
    assert len(payload['data']) >= 1
