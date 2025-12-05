This `submission/` folder contains the minimal files for Problems 1-3 of the
coding assignment.

Files:
- `models.py` : SQLAlchemy ORM definitions (Problem 1)
- `database.py` : Database manager + ingestion helpers (Problem 2)
- `ingest_data.py` : CLI for ingestion (Problem 2)
- `analyze_data.py` : Analysis / aggregation script (Problem 3)
- `PROBLEM_1_DATA_MODELING.md`, `PROBLEM_2_INGESTION.md`, `PROBLEM_3_ANALYSIS.md` : explanatory docs

To run end-to-end (from repository root):
```
python submission/ingest_data.py --reset
python submission/analyze_data.py
```

Data files are expected under `data/wx_data` and `data/yld_data` at the
repository root (unchanged).
