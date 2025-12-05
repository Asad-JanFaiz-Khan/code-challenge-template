```markdown
# Problem 4: REST API

## Objective
Create a REST API exposing the ingested weather records and the precomputed yearly per-station statistics.

## Implementation

- **Framework**: Flask
- **File**: `submission/app.py`
- **Factory**: `create_app(database_url=None)` — allows tests to configure a temporary DB

## Endpoints

1. `GET /api/weather`
   - Description: Returns weather records ingested into the database.
   - Query parameters:
     - `station_id` (string): filter by station code (e.g. `USC00110072`)
     - `date` (YYYY-MM-DD): exact date filter
     - `start_date` / `end_date` (YYYY-MM-DD): date range filters
     - `limit` / `offset`: pagination (limit constrained to 1..10000)
   - Response: JSON object with `data` array and `pagination` metadata.

2. `GET /api/weather/stats`
   - Description: Returns precomputed yearly statistics per station.
   - Query parameters:
     - `station_id` (string): filter by station code
     - `year`, `start_year`, `end_year` (int): filter by year or year range
     - `limit` / `offset`: pagination
   - Response: JSON object with `data` array where each item contains
     `station_id`, `year`, `avg_max_celsius`, `avg_min_celsius`, `total_precip_cm`.

3. `GET /openapi.json`
   - Minimal OpenAPI spec describing the API (used by Swagger UI).

4. `GET /docs`
   - Serves a minimal Swagger UI page that points to `/openapi.json`.

## Implementation details

- The API is implemented in `submission/app.py` and uses the same `submission/models.py` and
  `submission/database.py` modules used for ingestion and analysis.
- Filtering and pagination are performed at the database level using SQLAlchemy queries.
- The `YearlyStationStats` table is populated by running `submission/analyze_data.py`.

## How to run locally

1. Ensure data have been ingested and analyzed (creates `weather.db`):
```bash
python submission/ingest_data.py --reset
python submission/analyze_data.py
```

2. Start the API server (default DB `weather.db`):
```bash
python -m submission.app
```

3. Example requests:
```bash
# Health / docs
curl http://localhost:5000/openapi.json
curl http://localhost:5000/docs

# Weather records for a station
curl "http://localhost:5000/api/weather?station_id=USC00110072&limit=5"

# Stats for a station and year range
curl "http://localhost:5000/api/weather/stats?station_id=USC00110072&start_year=1990&end_year=2000&limit=10"
```

## Testing

- Unit tests for the API live in `tests/test_api.py`. They use a temporary SQLite DB and the
  `create_app(database_url=...)` factory to keep tests isolated and fast.

## Notes and future improvements

- The `openapi.json` provided is intentionally minimal. For production usage, consider using
  `flask-smorest` or `Flask-RESTX` to automatically generate richer OpenAPI specs including
  parameter and response schemas.
- Add sorting, richer pagination links (next/prev), and rate limiting if exposing publicly.
- Consider adding caching for expensive queries and authentication for protected endpoints.

```
