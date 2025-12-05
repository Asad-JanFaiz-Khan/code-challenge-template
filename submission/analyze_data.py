#!/usr/bin/env python
"""
Compute per-year per-station aggregated statistics and store them in the DB (Problem 3).

Usage:
    python analyze_data.py [--db DATABASE_URL]

This file is a standalone copy of the analysis logic adapted to the
`submission/` layout where `database.py` and `models.py` are sibling modules.
"""

import argparse
import logging
from datetime import datetime
from pathlib import Path

from sqlalchemy import func, case

from database import get_database_manager
from models import WeatherRecord, YearlyStationStats

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)


def compute_and_store_stats(database_url: str = 'sqlite:///weather.db') -> int:
    dbm = get_database_manager(database_url)
    dbm.init_db()

    session = dbm.get_session()
    try:
        dialect = dbm.engine.dialect.name
        if dialect == 'sqlite':
            year_expr = func.strftime('%Y', WeatherRecord.observation_date)
        else:
            year_expr = func.extract('year', WeatherRecord.observation_date)

        avg_max_expr = func.avg(case((WeatherRecord.max_temperature_tenths_celsius != -9999, WeatherRecord.max_temperature_tenths_celsius), else_=None)).label('avg_max_tenths')
        avg_min_expr = func.avg(case((WeatherRecord.min_temperature_tenths_celsius != -9999, WeatherRecord.min_temperature_tenths_celsius), else_=None)).label('avg_min_tenths')
        sum_precip_expr = func.sum(case((WeatherRecord.precipitation_tenths_mm != -9999, WeatherRecord.precipitation_tenths_mm), else_=None)).label('sum_precip_tenths')

        query = (
            session.query(
                WeatherRecord.station_id.label('station_id'),
                year_expr.label('year'),
                avg_max_expr,
                avg_min_expr,
                sum_precip_expr,
            )
            .group_by(WeatherRecord.station_id, year_expr)
        )

        logger.info('Executing aggregate query...')
        rows = query.yield_per(1000)

        upsert_count = 0
        BATCH_SIZE = 500

        for r in rows:
            year_val = int(r.year) if isinstance(r.year, str) else int(r.year)

            avg_max_tenths = r.avg_max_tenths
            avg_min_tenths = r.avg_min_tenths
            sum_precip_tenths = r.sum_precip_tenths

            avg_max_c = (avg_max_tenths / 10.0) if avg_max_tenths is not None else None
            avg_min_c = (avg_min_tenths / 10.0) if avg_min_tenths is not None else None
            total_precip_cm = (sum_precip_tenths / 100.0) if sum_precip_tenths is not None else None

            existing = session.query(YearlyStationStats).filter_by(station_id=r.station_id, year=year_val).first()
            if existing:
                existing.avg_max_celsius = avg_max_c
                existing.avg_min_celsius = avg_min_c
                existing.total_precip_cm = total_precip_cm
            else:
                new = YearlyStationStats(
                    station_id=r.station_id,
                    year=year_val,
                    avg_max_celsius=avg_max_c,
                    avg_min_celsius=avg_min_c,
                    total_precip_cm=total_precip_cm,
                )
                session.add(new)
            upsert_count += 1

            if upsert_count % BATCH_SIZE == 0:
                session.commit()
                logger.info(f'Committed {upsert_count} stat rows...')

        session.commit()
        logger.info(f'Finished upserting {upsert_count} yearly-station stat rows')
        return upsert_count

    except Exception:
        session.rollback()
        raise
    finally:
        session.close()


def main():
    parser = argparse.ArgumentParser(description='Compute yearly per-station stats and store them in DB')
    parser.add_argument('--db', default='sqlite:///weather.db', help='Database URL')
    args = parser.parse_args()

    start = datetime.now()
    logger.info('Starting analysis: computing yearly per-station statistics')
    count = compute_and_store_stats(args.db)
    duration = (datetime.now() - start).total_seconds()
    logger.info(f'Analysis complete: {count} rows upserted in {duration:.2f} seconds')


if __name__ == '__main__':
    main()
