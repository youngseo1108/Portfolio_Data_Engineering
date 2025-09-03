# Portfolio: Data Engineering Projects

This portfolio showcases my personal and academic projects related to **data engineering**, with a focus on database performance, automation, and scalable processing in cloud-simulated environments, and algorithmic optimisation for large-scale data processing.

## 1. Cloud-based Database Performance Benchmarking with LLM-assisted SQL Tuning

This project simulates enterprise-scale database performance engineering tasks in a cloud-like environment, using PostgreSQL, Docker, and MinIO for data storage, with Python scripts for benchmarking and visualisation. It showcases how LLMs can assist in query tuning by suggesting rewrites and indexing strategies that improve performance.

### Goals
- Benchmark and optimise SQL queries in PostgreSQL
- Simulate cloud-native workflows with MinIO + Docker
- Automate query benchmarking and log results with Python
- Apply LLM-assisted SQL tuning to generate query rewrites and indexing hints
- Visualise baseline vs LLM-tuned performance

### Tech Stack
- **Databases**: PostgreSQL  
- **Storage/Infra**: MinIO (S3-compatible), Docker, Ubuntu/WSL2  
- **Automation**: Python (psycopg2, pandas)
- **Visualisation**: matplotlib  
- **GenAI**: OpenAI API for SQL tuning

### Folder structure
```sh
Cloud_DB_Performance_Engineering/
├── data/
│   └── raw/                      # Sample dataset (source: https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page)
├── ingestion/
│   └── postgres_load.sql         # Load data into Postgres
├── benchmarks/
│   ├── baseline_queries.sql      # Original slow queries
│   ├── tuned_queries.sql         # Hand-tuned & LLM-suggested rewrites
│   ├── run_benchmarks.py         # Execute queries, log timings
│   └── results.csv               # Benchmark logs (query, time_ms, rows)
├── genai/
│   └── llm_sql_tuning.py         # Prompt LLM, store suggested rewrites
├── docker-compose.yaml
└── Makefile
```

### Workflow
1. **Data Ingestion** – Load raw data into MinIO to PostgreSQL  
2. **Baseline Queries** – Run representative SQL queries, capture timings  
3. **LLM-assisted Tuning** – Use LLM to propose rewrites & indexes, rerun queries  
4. **Visualisation** – Compare baseline vs tuned performance with plots

### Outcomes
- Achieved significant query runtime improvements through indexing and LLM-assisted rewrites
- LLM detected inefficient joins and recommended multi-column indexes
- Visual comparison of baseline vs tuned queries (see example below)

### Learnings
- Designed a reproducible benchmarking setup with Docker + PostgreSQL
- Logged and visualised query performance with Python
- Demonstrated practical GenAI use case in database performance engineering

---


## 2. MSc Thesis Project – Inequality Join Optimisation

A research-driven project focused on improving the efficiency of inequality joins for interval-based data (e.g., time series or temporal ranges). Conducted as part of my Master's thesis.

### Overview
- Implemented and compared multiple join algorithms in Python:
  - **Brute-force baseline**: naive nested loop across all tuples  
  - **IEJoin**: index-based inequality join using sorted arrays and bitmap indexes
  - **RMJ (Range Merge Join)**: join based on scanning ranges for overlapping intervals  
  - **OMJ (Overlap Merge Join)**: symmetric extension of RMJ combining forward and backward scans  
- Built a benchmarking framework to evaluate runtime performance across varying dataset sizes (1k–10k rows) and overlap densities (10–90%+).  
- Analysed execution time, scalability, and trade-offs between filtering strategies.

### Outcomes
- Showed that **algorithm performance depends heavily on overlap density** in interval datasets  
- **IEJoin** achieved the best performance when overlap was low to moderate (e.g., 10–50%)  
- **Brute-force join** surprisingly outperformed other methods under very high overlap (>90%) due to reduced filtering overhead  
- **RMJ/OMJ**, while theoretically efficient, performed worse than expected and in some cases slower than the baseline, revealing gaps for further optimisation
- Overall, findings highlight the **need to adapt join strategy to dataset characteristics**, with implications for temporal and time-series query optimisation

### Folder structure
```sh
Thesis_Ineuqality_Join
├───Inequality_Join_Algo    # Core implementation of each join algorithm variant
├───Evaluation/             # Jupyter notebooks for performance experiments
│   └── src/                # Python scripts for data generation and benchmarking
└───Master_Thesis.pdf       # Full thesis with summary, processes and results
```