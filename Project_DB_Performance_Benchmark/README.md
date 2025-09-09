## Cloud-based Database Performance Benchmarking with LLM-assisted SQL Tuning

This project simulates enterprise-scale database performance engineering tasks in a cloud-like environment, using MinIO (S3-compatible storage), PostgreSQL, Docker, and Python for benchmarking and visualisation. It showcases how LLMs (ChatGPT) can assist in query tuning by suggesting rewrites and indexing strategies that improve performance.


### Goals
- Benchmark and optimise SQL queries in PostgreSQL
- Automate query benchmarking and log results with Python
- Apply LLM-assisted SQL tuning (via ChatGPT prompts) to generate query rewrites and indexing hints
- Visualise baseline vs tuned performance


### Tech Stack
- **Databases**: PostgreSQL
- **Storage/Infra**: MinIO, Docker, Ubuntu/WSL2
- **Automation**: Python (psycopg2, pandas, SQLAlchemy)
- **Visualisation**: matplotlib
- **GenAI**: ChatGPT (LLM suggestions for query tuning)


### Folder structure
```sh
Cloud_DB_Performance_Engineering/
├── data/                         # raw datasets (gitignored due to size)
│   └── raw/                      # Dataset (source: NYC Taxi, https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page)
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
1. **Data Ingestion** – Uploaded raw NYC Taxi data (~3.5M rows, Jan 2025) to MinIO and ingested into PostgreSQL via Python.
2. **Baseline Queries** – Ran representative SQL queries from baseline_queries.sql and captured execution times.
3. **LLM-assisted Tuning** – Asked ChatGPT to improve baseline queries (prompt: *You are a SQL performance assistant for PostgreSQL. Given the schema and a slow query, propose: 1) a rewritten SQL 2) suggested indexes*). Incorporated the rewrites into tuned_queries.sql.
4. **Benchmarking** – Executed both baseline and tuned queries via run_benchmarks.py, logging execution times to results.csv.
5. **Visualisation** – Generated a comparison bar chart with matplotlib (plots/example_plot.png).


### Outcomes
- LLM suggested improvements such as:
  - Rewriting date filters from BETWEEN to half-open ranges for index efficiency
  - Adding a composite index (pu_location_id, tpep_pickup_datetime)
- Benchmarks were run on ~3.5M rows of data and showed significant runtime improvements (see `benchmarks/results.csv` for raw timings):
  - Q1: 30% faster (2.6s → 1.8s)
  - Q2: 97% faster (523.9ms → 14.4ms)
  - Q3: 99% faster (16.3s → 0.2s)


#### Visual Comparison
![Visual Comparison](plots/example_plot.png)


### Learnings
- Designed a cloud-like ingestion pipeline: raw data → MinIO bucket → PostgreSQL.
- Built an automated benchmarking setup with Docker and Python to measure query performance.
- Logged and visualised baseline vs tuned query performance, showing clear speedups on multi-million row datasets.
- Demonstrated a practical use of GenAI: leveraging ChatGPT as a SQL tuning assistant rather than manually crafting all optimisations.
- Identified possible future extensions:
  - Automating LLM-assisted query rewrites directly via a script.
  - Extending the ingestion flow to include more complex transformations or larger-scale object storage scenarios.


### Potential Problems & Solutions
- If `make ingest` fails due to missing Python deps after `make up`, run:
```ubuntu
docker compose exec -T app pip install SQLAlchemy psycopg2-binary pandas matplotlib
```