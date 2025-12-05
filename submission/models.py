"""
Database models for weather and crop yield data (Problem 1).

SQLAlchemy ORM definitions. Measurements are stored in integer "tenths"
to avoid floating point storage issues; conversions are provided as
properties and performed at aggregation / API layers.
"""

from sqlalchemy import Column, Integer, String, Date, Float, ForeignKey, Index
from sqlalchemy.orm import declarative_base, relationship

Base = declarative_base()


class WeatherStation(Base):
    __tablename__ = 'weather_stations'

    id = Column(Integer, primary_key=True)
    station_id = Column(String(20), unique=True, nullable=False, index=True)
    state = Column(String(50), nullable=True)

    weather_records = relationship('WeatherRecord', back_populates='station', cascade='all, delete-orphan')

    def __repr__(self):
        return f'<WeatherStation {self.station_id} ({self.state})>'


class WeatherRecord(Base):
    __tablename__ = 'weather_records'

    id = Column(Integer, primary_key=True)
    station_id = Column(Integer, ForeignKey('weather_stations.id'), nullable=False)
    observation_date = Column(Date, nullable=False)
    max_temperature_tenths_celsius = Column(Integer, nullable=True)
    min_temperature_tenths_celsius = Column(Integer, nullable=True)
    precipitation_tenths_mm = Column(Integer, nullable=True)

    station = relationship('WeatherStation', back_populates='weather_records')

    __table_args__ = (
        Index('idx_station_date', 'station_id', 'observation_date'),
        Index('idx_observation_date', 'observation_date'),
        Index('idx_station_id', 'station_id'),
    )

    def __repr__(self):
        return f'<WeatherRecord {self.observation_date} Station={self.station_id}>'

    @property
    def max_temperature_celsius(self):
        if self.max_temperature_tenths_celsius is None or self.max_temperature_tenths_celsius == -9999:
            return None
        return self.max_temperature_tenths_celsius / 10.0

    @property
    def min_temperature_celsius(self):
        if self.min_temperature_tenths_celsius is None or self.min_temperature_tenths_celsius == -9999:
            return None
        return self.min_temperature_tenths_celsius / 10.0

    @property
    def precipitation_mm(self):
        if self.precipitation_tenths_mm is None or self.precipitation_tenths_mm == -9999:
            return None
        return self.precipitation_tenths_mm / 10.0


class CropYield(Base):
    __tablename__ = 'crop_yield'

    id = Column(Integer, primary_key=True)
    year = Column(Integer, unique=True, nullable=False, index=True)
    yield_amount = Column(Integer, nullable=False)

    def __repr__(self):
        return f'<CropYield {self.year}: {self.yield_amount}>'


class YearlyStationStats(Base):
    __tablename__ = 'yearly_station_stats'

    id = Column(Integer, primary_key=True)
    station_id = Column(Integer, ForeignKey('weather_stations.id'), nullable=False, index=True)
    year = Column(Integer, nullable=False, index=True)

    avg_max_celsius = Column(Float, nullable=True)
    avg_min_celsius = Column(Float, nullable=True)
    total_precip_cm = Column(Float, nullable=True)

    station = relationship('WeatherStation')

    __table_args__ = (
        Index('idx_stats_station_year', 'station_id', 'year', unique=True),
    )

    def __repr__(self):
        return f'<YearlyStationStats station={self.station_id} year={self.year} max={self.avg_max_celsius}>'
