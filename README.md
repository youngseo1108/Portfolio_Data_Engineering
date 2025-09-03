# Portfolio: Data Engineering Projects

This portfolio showcases my personal and academic projects related to **data engineering**, with a focus on database performance, automation, and scalable processing in cloud-simulated environments, and algorithmic optimisation for large-scale data processing.

## 1. Cloud-based Database Performance Benchmarking with LLM-assisted SQL Tuning

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
![Visual Comparison](Project_DB_Performance_Benchmark/plots/example_plot.png)

### Learnings
- Built a reproducible benchmarking setup with Docker + PostgreSQL
- Logged and visualised query performance with Python
- Showed a practical use of GenAI: leveraging ChatGPT as a SQL tuning assistant rather than writing tuned queries entirely by hand
- Identified possible future extensions, such as:
  - Integrating MinIO (S3-compatible object storage) to simulate cloud-based ingestion
  - Automating LLM-assisted query rewrites via a script

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

#### Visual Comparison
![output](Thesis_Inequality_Join/output.png)

### Folder structure
```sh
Thesis_Ineuqality_Join
├───Inequality_Join_Algo    # Core implementation of each join algorithm variant
├───Evaluation/             # Jupyter notebooks for performance experiments
│   └── src/                # Python scripts for data generation and benchmarking
└───Master_Thesis.pdf       # Full thesis with summary, processes and results
```