# Weather Data Code Challenge

This repository contains a small data engineering / API project that ingests weather and crop-yield data, computes yearly per-station statistics, and exposes a simple REST API to query the results.

The `submission/` folder contains the final packaged solution for the assignment (models, ingestion, analysis, API, schema, tests, and documentation). A brief overview is below to help contributors run the code locally and understand the project layout.

## Highlights
- Ingests raw weather station files into a SQL database (SQLite by default).
- Converts and aggregates measurements (temperatures stored as "tenths" in the raw data).
- Computes yearly per-station statistics and upserts results into `yearly_station_stats`.
- Provides a small Flask-based REST API with an OpenAPI endpoint.
- Includes unit tests covering ingestion, analysis, models, and API behavior.

## Repository layout
- `submission/` — final submission code and docs
  - `models.py` — SQLAlchemy models
  - `database.py` — DB helper / ingestion helpers
  - `ingest_data.py` — CLI script to load raw files into DB
  - `analyze_data.py` — compute yearly stats and upsert
  - `api.py` / `app.py` — Flask app and OpenAPI generator
  - `schema.sql` — portable DDL for review
  - `Deployment(Extra Credit).txt` — deployment approach (Azure)
- `data/` — raw data files (wx_data / yld_data)
- `tests/` — pytest unit tests
- `requirements.txt` — Python dependencies

## Quickstart (local)
Prerequisites: Python 3.10+ and a virtual environment.

1. Create and activate a virtualenv:
   ```bash
   python -m venv .venv
   source .venv/bin/activate
   pip install -r requirements.txt
   ```

2. Ingest data (this will create `weather.db` and load raw files). This may take several minutes for the full dataset:
   ```bash
   python submission/ingest_data.py --reset
   ```

3. Run analysis to compute yearly-station stats:
   ```bash
   python submission/analyze_data.py
   ```

4. Run the API locally (development):
   ```bash
   # starts Flask dev server on http://127.0.0.1:5000
   python -m submission.app
   ```
   - Open `http://127.0.0.1:5000/openapi.json` for the generated OpenAPI spec
   - Query endpoints like `/api/weather` and `/api/weather/stats`

## Tests
Run the unit tests with:
```bash
python -m pytest -q
```

## Configuration
- The code uses SQLAlchemy and reads the database URL from environment variables when provided; by default it uses a local SQLite file `weather.db`.
- For production, replace SQLite with Postgres (set `DATABASE_URL` accordingly). See `submission/Deployment(Extra Credit).txt` for a cloud deployment plan.

## Deployment (cloud)
See `submission/Deployment(Extra Credit).txt` for a detailed plan that targets Azure (ACR, Container Apps / App Service, Azure Database for PostgreSQL, Azure Functions / Container Apps Job, Blob Storage, Key Vault, Application Insights). The repo includes notes and next steps for creating Dockerfiles, GitHub Actions, and Terraform templates.

## Contributing
- This repository is prepared as an assignment submission. If you want to extend it, please open an issue or pull request on your fork.

## License
This repository is for educational/demonstration purposes. Check with the repository owner for license and reuse policies.

----
If you want, I can also:
- Add Dockerfiles for the API and ingestion job
- Add a GitHub Actions workflow for CI/CD that builds, tests, and pushes images to ACR
- Scaffold Terraform templates for a minimal Azure deployment

If you'd like one of those, tell me which and I'll add it next.
