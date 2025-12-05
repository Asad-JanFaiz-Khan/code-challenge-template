```markdown
# Problem 1: Data Modeling

## Objective
Design a data model to represent weather data records from 1985-2014 across weather stations.

## Solution (see `models.py`)

- `WeatherStation`: station metadata
- `WeatherRecord`: daily observations (stored in tenths for precision)
- `CropYield`: yearly crop yield
- `YearlyStationStats`: precomputed yearly per-station aggregates

Indexes and sentinel handling are implemented as described in the code.
```
