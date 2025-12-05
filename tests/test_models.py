import sys
from pathlib import Path

# Ensure submission modules are importable
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'submission'))

import models


def test_weatherrecord_properties():
    wr = models.WeatherRecord(
        max_temperature_tenths_celsius=275,
        min_temperature_tenths_celsius=75,
        precipitation_tenths_mm=123,
    )

    assert wr.max_temperature_celsius == 27.5
    assert wr.min_temperature_celsius == 7.5
    assert wr.precipitation_mm == 12.3

    # sentinel values yield None
    wr2 = models.WeatherRecord(
        max_temperature_tenths_celsius=-9999,
        min_temperature_tenths_celsius=-9999,
        precipitation_tenths_mm=-9999,
    )
    assert wr2.max_temperature_celsius is None
    assert wr2.min_temperature_celsius is None
    assert wr2.precipitation_mm is None
