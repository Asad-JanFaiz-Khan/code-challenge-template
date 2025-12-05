```markdown
# Problem 2: Data Ingestion

## Objective
Ingest weather and crop yield data into the database, ensuring idempotency
and reporting start/end times and record counts.

## Usage
From repository root:
```
python submission/ingest_data.py --reset
```

The ingestion code and behavior are implemented in `submission/database.py`
and `submission/ingest_data.py`.
```
