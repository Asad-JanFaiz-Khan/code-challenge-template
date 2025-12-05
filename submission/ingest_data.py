#!/usr/bin/env python
"""
Ingestion script for weather and crop yield data (Problem 2).

Usage:
    python ingest_data.py [--reset] [--db DATABASE_URL]

This script initializes the DB and ingests data from `data/wx_data` and
`data/yld_data` located at the repository root.
"""

import sys
import argparse
import logging
from datetime import datetime
from pathlib import Path

from database import get_database_manager

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)


def main():
    parser = argparse.ArgumentParser(description='Ingest weather and crop yield data.')
    parser.add_argument('--reset', action='store_true', help='Reset database (drop and recreate tables)')
    parser.add_argument('--db', default='sqlite:///weather.db', help='Database URL (default: sqlite:///weather.db)')
    args = parser.parse_args()

    script_dir = Path(__file__).parent
    project_root = script_dir.parent
    wx_data_dir = project_root / 'data' / 'wx_data'
    yld_data_dir = project_root / 'data' / 'yld_data'

    start_time = datetime.now()
    logger.info('=' * 70)
    logger.info('DATA INGESTION PROCESS STARTED')
    logger.info('=' * 70)
    logger.info(f'Start time: {start_time.strftime("%Y-%m-%d %H:%M:%S")}')
    logger.info(f'Database: {args.db}')
    logger.info(f'Reset: {args.reset}')
    logger.info(f'Weather data directory: {wx_data_dir}')
    logger.info(f'Crop yield data directory: {yld_data_dir}')

    try:
        db_manager = get_database_manager(args.db)
    except Exception as e:
        logger.error(f'Failed to initialize database manager: {e}')
        return False

    if args.reset:
        logger.info('Dropping existing tables (--reset flag set)...')
        try:
            db_manager.drop_db()
            logger.info('✓ Existing tables dropped successfully')
        except Exception as e:
            logger.error(f'Failed to drop tables: {e}')
            return False

    logger.info('Creating database tables...')
    try:
        db_manager.init_db()
        logger.info('✓ Database tables created successfully')
    except Exception as e:
        logger.error(f'Failed to create tables: {e}')
        return False

    logger.info('')

    weather_success = False
    try:
        logger.info('Ingesting weather data...')
        weather_start = datetime.now()
        records_ingested = db_manager.ingest_weather_data(str(wx_data_dir))
        weather_end = datetime.now()
        weather_duration = (weather_end - weather_start).total_seconds()

        logger.info('✓ Weather data ingestion completed')
        logger.info(f'  - Records ingested: {records_ingested:,}')
        logger.info(f'  - Duration: {weather_duration:.2f} seconds')
        if records_ingested > 0 and weather_duration > 0:
            logger.info(f'  - Rate: {records_ingested / weather_duration:.0f} records/second')
        weather_success = True
    except Exception as e:
        logger.error(f'✗ Failed to ingest weather data: {e}')
        weather_success = False

    logger.info('')

    yield_success = False
    try:
        logger.info('Ingesting crop yield data...')
        yield_start = datetime.now()
        records_ingested = db_manager.ingest_crop_yield_data(str(yld_data_dir))
        yield_end = datetime.now()
        yield_duration = (yield_end - yield_start).total_seconds()

        logger.info('✓ Crop yield data ingestion completed')
        logger.info(f'  - Records ingested: {records_ingested:,}')
        logger.info(f'  - Duration: {yield_duration:.2f} seconds')
        yield_success = True
    except Exception as e:
        logger.error(f'✗ Failed to ingest crop yield data: {e}')
        yield_success = False

    logger.info('')

    end_time = datetime.now()
    total_duration = (end_time - start_time).total_seconds()

    logger.info('=' * 70)
    if weather_success and yield_success:
        logger.info('✓ DATA INGESTION COMPLETED SUCCESSFULLY')
    else:
        logger.info('✗ DATA INGESTION COMPLETED WITH ERRORS')
    logger.info('=' * 70)
    logger.info(f'End time: {end_time.strftime("%Y-%m-%d %H:%M:%S")}')
    logger.info(f'Total duration: {total_duration:.2f} seconds')

    return weather_success and yield_success


if __name__ == '__main__':
    success = main()
    sys.exit(0 if success else 1)
