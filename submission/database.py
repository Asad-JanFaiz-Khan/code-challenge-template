"""
Database manager and ingestion helpers (Problem 2).

Provides `DatabaseManager` with methods to init/drop DB and ingest data.
This is a slight reorganization of the original `src/database.py` adapted
to be self-contained inside `submission/`.
"""

import logging
from datetime import datetime
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from pathlib import Path

from models import Base, WeatherStation, WeatherRecord, CropYield

logger = logging.getLogger(__name__)


class DatabaseManager:
    def __init__(self, database_url: str = 'sqlite:///weather.db'):
        self.database_url = database_url
        self.engine = create_engine(database_url, echo=False)
        self.SessionLocal = sessionmaker(bind=self.engine)

    def init_db(self):
        Base.metadata.create_all(self.engine)
        print("Database tables created successfully.")

    def drop_db(self):
        Base.metadata.drop_all(self.engine)
        print("Database tables dropped successfully.")

    def get_session(self):
        return self.SessionLocal()

    def ingest_weather_data(self, wx_data_dir: str) -> int:
        session = self.get_session()
        total_records = 0

        try:
            wx_path = Path(wx_data_dir)
            if not wx_path.exists():
                raise FileNotFoundError(f"Weather data directory not found: {wx_data_dir}")

            txt_files = sorted(wx_path.glob('*.txt'))
            logger.info(f"Found {len(txt_files)} weather station files")

            for file_index, file_path in enumerate(txt_files, 1):
                station_id = file_path.stem

                existing_station = session.query(WeatherStation).filter_by(station_id=station_id).first()
                if existing_station:
                    logger.debug(f"[{file_index}/{len(txt_files)}] Skipping {station_id} - already in database")
                    continue

                station = WeatherStation(station_id=station_id)
                session.add(station)
                session.flush()

                record_count = 0
                error_count = 0

                with open(file_path, 'r') as f:
                    for line in f:
                        line = line.strip()
                        if not line:
                            continue

                        parts = line.split('\t')
                        if len(parts) < 4:
                            error_count += 1
                            continue

                        try:
                            date_str = parts[0]
                            max_temp = int(parts[1])
                            min_temp = int(parts[2])
                            precip = int(parts[3])

                            obs_date = datetime.strptime(date_str, '%Y%m%d').date()

                            record = WeatherRecord(
                                station_id=station.id,
                                observation_date=obs_date,
                                max_temperature_tenths_celsius=max_temp,
                                min_temperature_tenths_celsius=min_temp,
                                precipitation_tenths_mm=precip
                            )
                            session.add(record)
                            record_count += 1

                            if record_count % 10000 == 0:
                                session.commit()
                                logger.debug(f"  Batch commit: {record_count:,} records for {station_id}")

                        except (ValueError, IndexError):
                            error_count += 1
                            logger.warning(f"  Error parsing line in {station_id}: {line}")
                            continue

                session.commit()
                total_records += record_count

                status = ""
                if error_count > 0:
                    status = f" ({error_count} errors skipped)"
                logger.info(f"[{file_index}/{len(txt_files)}] {station_id}: {record_count:,} records{status}")

        except Exception as e:
            session.rollback()
            logger.error(f"Error during weather data ingestion: {e}")
            raise
        finally:
            session.close()

        return total_records

    def ingest_crop_yield_data(self, yld_data_dir: str) -> int:
        session = self.get_session()
        try:
            yld_path = Path(yld_data_dir)
            if not yld_path.exists():
                raise FileNotFoundError(f"Crop yield data directory not found: {yld_data_dir}")

            txt_files = sorted(yld_path.glob('*.txt'))
            logger.info(f"Found {len(txt_files)} crop yield data files")

            all_years = {}
            duplicates_found = 0
            error_count = 0

            for file_path in txt_files:
                logger.debug(f"Processing crop yield file: {file_path.name}")
                with open(file_path, 'r') as f:
                    for line in f:
                        line = line.strip()
                        if not line:
                            continue
                        parts = line.split()
                        if len(parts) < 2:
                            error_count += 1
                            continue
                        try:
                            year = int(parts[0])
                            yield_amount = int(parts[1])
                            if year not in all_years:
                                all_years[year] = yield_amount
                            else:
                                duplicates_found += 1
                        except ValueError:
                            error_count += 1
                            continue

            records_inserted = 0
            for year, yield_amount in sorted(all_years.items()):
                existing = session.query(CropYield).filter_by(year=year).first()
                if not existing:
                    crop_yield = CropYield(year=year, yield_amount=yield_amount)
                    session.add(crop_yield)
                    records_inserted += 1

            session.commit()

            status_parts = [f"{records_inserted:,} new records"]
            if duplicates_found > 0:
                status_parts.append(f"{duplicates_found} duplicates (kept first)")
            if error_count > 0:
                status_parts.append(f"{error_count} errors skipped")

            status = " (" + ", ".join(status_parts) + ")"
            logger.info(f"Crop yield data: {records_inserted:,} records ingested{status if duplicates_found > 0 or error_count > 0 else ''}")

            return records_inserted

        except Exception as e:
            session.rollback()
            logger.error(f"Error during crop yield data ingestion: {e}")
            raise
        finally:
            session.close()


def get_database_manager(database_url: str = 'sqlite:///weather.db') -> DatabaseManager:
    return DatabaseManager(database_url)
