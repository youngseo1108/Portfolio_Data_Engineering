## Cloud-based Database Performance Benchmarking with LLM-assisted SQL Tuning

This project simulates enterprise-scale database performance engineering tasks in a cloud-like environment, using PostgreSQL, Docker, and Python for benchmarking and visualisation. It showcases how LLMs (ChatGPT) can assist in query tuning by suggesting rewrites and indexing strategies that improve performance.

### Goals
- Benchmark and optimise SQL queries in PostgreSQL
- Automate query benchmarking and log results with Python
- Apply LLM-assisted SQL tuning (via ChatGPT prompts) to generate query rewrites and indexing hints
- Visualise baseline vs tuned performance

### Tech Stack
- **Databases**: PostgreSQL  
- **Storage/Infra**: Docker, Ubuntu/WSL2
- **Automation**: Python (psycopg2, pandas)
- **Visualisation**: matplotlib  
- **GenAI**: ChatGPT (LLM suggestions for query tuning)

### Folder structure
```sh
Cloud_DB_Performance_Engineering/
├── data/
│   └── raw/                      # Sample dataset (NYC Taxi: https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page)
├── ingestion/
│   ├── postgres_init.sql         # Create schemas & tables for Postgres
│   └── postgres_load.py          # Load data into Postgres
├── benchmarks/
│   ├── baseline_queries.sql      # Original queries
│   ├── tuned_queries.sql         # Rewrites suggested by ChatGPT + indexes
│   ├── run_benchmarks.py         # Execute queries, log timings
│   └── results.csv               # Benchmark logs (query, time_ms)
├── plots/
│   └── plot_results.py           # Plot the comparison results
├── docker-compose.yaml
├── requirements.txt
└── Makefile
```

### Workflow
1. **Data Ingestion** – Loaded raw NYC Taxi data (~3.5M rows, January 2025) into PostgreSQL
2. **Baseline Queries** – Run representative SQL queries, capture timings
3. **LLM-assisted Tuning** – Asked ChatGPT to improve baseline queries (e.g., change BETWEEN to half-open ranges, suggest composite indexes). Incorporated these into tuned_queries.sql.
4. **Benchmarking** – Ran both baseline and tuned queries via run_benchmarks.py, saving execution times to results.csv
5. **Visualisation** – Generated bar chart from results.csv with matplotlib (example_plot.png)

### Outcomes
- LLM suggested improvements such as:
  - Rewriting date filters from BETWEEN to half-open ranges for index efficiency
  - Adding a composite index (pu_location_id, tpep_pickup_datetime)
- Benchmarks were run on ~3.5M rows of data and showed significant runtime improvements (see `benchmarks/results.csv` for raw timings):
  - Q1: 57% faster (2.2s → 0.9s)
  - Q2: 98% faster (155ms → 4ms)
  - Q3: 98% faster (8.4s → 0.2s)

#### Visual Comparison
![Visual Comparison](plots/example_plot.png)

### Learnings
- Built a reproducible benchmarking setup with Docker + PostgreSQL
- Logged and visualised query performance with Python
- Showed a practical use of GenAI: leveraging ChatGPT as a SQL tuning assistant rather than writing tuned queries entirely by hand
- Identified possible future extensions, such as:
  - Integrating MinIO (S3-compatible object storage) to simulate cloud-based ingestion
  - Automating LLM-assisted query rewrites via a script

### Potential Problems & Solutions
- If `make ingest` fails due to missing Python deps after `make up`, run:
```ubuntu
docker compose exec -T app pip install SQLAlchemy psycopg2-binary pandas matplotlib
```