import sys
from pathlib import Path
import sqlite3

import pytest

# Ensure submission modules are importable
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'submission'))

import database
import models


def write_wx_file(path, lines):
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, 'w') as f:
        for ln in lines:
            f.write(ln + '\n')


def write_yld_file(path, lines):
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, 'w') as f:
        for ln in lines:
            f.write(ln + '\n')


def test_ingest_weather_and_crop_yield(tmp_path):
    # Setup directories
    wx_dir = tmp_path / 'wx_data'
    yld_dir = tmp_path / 'yld_data'

    # Create a sample weather station file with 2 valid lines and 1 missing sentinel
    wx_file = wx_dir / 'TEST001.txt'
    write_wx_file(wx_file, [
        '20200101\t250\t50\t100',
        '20200102\t300\t100\t200',
        '20200103\t-9999\t-9999\t-9999',
    ])

    # Create a crop yield file
    yld_file = yld_dir / 'yield.txt'
    write_yld_file(yld_file, [
        '2020 12345',
        '2021 23456',
    ])

    db_file = tmp_path / 'test_ingest.db'
    db_url = f'sqlite:///{db_file}'

    dbm = database.get_database_manager(db_url)
    dbm.init_db()

    # Ingest weather
    records = dbm.ingest_weather_data(str(wx_dir))
    # ingestion stores sentinel rows as records (analysis ignores sentinel values)
    assert records == 3

    # Ingest crop yield
    yrecords = dbm.ingest_crop_yield_data(str(yld_dir))
    assert yrecords == 2

    # Re-run ingestion (should be idempotent - stations already present)
    records2 = dbm.ingest_weather_data(str(wx_dir))
    assert records2 == 0

    # Verify DB contents directly via sqlite3
    conn = sqlite3.connect(str(db_file))
    cur = conn.cursor()
    cur.execute('SELECT COUNT(*) FROM weather_stations')
    stations = cur.fetchone()[0]
    assert stations == 1

    cur.execute('SELECT COUNT(*) FROM weather_records')
    rows = cur.fetchone()[0]
    assert rows == 3

    cur.execute('SELECT COUNT(*) FROM crop_yield')
    cy = cur.fetchone()[0]
    assert cy == 2

    conn.close()
