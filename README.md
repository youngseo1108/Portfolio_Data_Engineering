# Portfolio: Data Engineering Projects

This portfolio showcases my personal and academic projects related to **data engineering**, with a focus on data pipeline orchestration, automation, and algorithmic optimisation for large-scale data processing.

## 1. Apache Airflow Project

An individual project developed by following this [Airflow tutorial](https://youtu.be/3xyoM28B40Y?feature=shared) and [Airflow 101: Building Your First Workflow](https://airflow.apache.org/docs/apache-airflow/stable/tutorial/fundamentals.html), aimed at understanding the fundamentals of DAG design, task scheduling, and pipeline management.


### Key features
- Extracts data from xxx
- Transforms and cleans data using Python
- Loads processed data into PostgreSQL
- Scheduled using Apache Airflow DAGs with modular task separation


### Folder
```sh
`Project_Airflow/`
```
---

## 2. MSc Thesis Project – Inequality Join Optimisation

A research-driven project focused on improving the efficiency of inequality joins for interval-based data with indexes (e.g., time series or temporal ranges). This work was submitted as part of my Master's thesis in Data Science at the University of Zurich.


### Overview
- Designed and implemented multiple inequality join algorithms
  - **Brute-force**: A naive nested loop implementation evaluating all possible pairs for three inequality predicates
  - **IEJoin**: An efficient inequality join algorithm using sorted indices to evaluate multiple predicates with reduced complexity
  - **RMJ (Range Merge Join)**: A join algorithm that scans sorted relations in a single direction (forward or backward) to efficiently return tuple pairs with overlapping intervals, based on inequality and overlap conditions
  - **OMJ (Overlap Merge Join)**: An extended version of RMJ that performs both forward and backward scans by executing RMJ twice with swapped input relations, effectively capturing the full symmetric overlap between tuples
- Conducted performance evaluation using Python to compare brute-force, interval-based filtering, and index-based filtering across varying data sizes and interval overlap ratios


### Folder structure
```sh
Thesis_Ineuqality_Join
├───Inequality_Join_Algo    # Core implementation of each join algorithm variant
├───Evaluation/             # Jupyter notebooks for performance experiments
│   └── src/                # Python scripts for data generation and benchmarking
└───Master_Thesis.pdf       # Full thesis with summary, processes and results
```