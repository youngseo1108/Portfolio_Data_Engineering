# Portfolio: Data Engineering Projects

This portfolio showcases my personal and academic projects related to **data engineering**, with a focus on data pipeline orchestration, automation, and algorithmic optimisation for large-scale data processing.

## 1. Apache Airflow Project

An individual project developed by following this [Airflow tutorial](https://youtu.be/3xyoM28B40Y?feature=shared), aimed at understanding the fundamentals of DAG design, task scheduling, and pipeline management.

**Key features**
- Extracts data from Amazon API
- Transforms and cleans data using Python
- Loads processed data into PostgreSQL
- Scheduled using Apache Airflow DAGs with modular task separation

### Folder
```sh
`Project_Airflow/`
```
---

## 2. MSc Thesis Project – Inequality Join Optimisation

A research-driven project focused on improving the efficiency of inequality joins for interval-based data with indexes, such as time series data. This work was submitted as part of my Master's thesis in Data Science at the University of Zurich.

### Overview
- Designed and implemented multiple inequality join algorithms, including Brute-force (baseline), IEJoin (Inequality Join), and OMJ/RMJ (Overlap/Range Merge Join), by modifying existing algorithmic structures and adding features for improved efficiency
- Conducted performance evaluation using Python to compare brute-force, interval-based filtering, and index-based filtering across varying data sizes and overlap ratios

### Folder structure

```sh
Thesis_Ineuqality_Join
├───Inequality_Join_Algo    # Core implementation of each join algorithm variant
├───Evaluation/             # Jupyter notebooks for performance experiments
│   └── src/                # Python scripts for data generation and benchmarking
└───Master_Thesis.pdf       # Full thesis with summary, processes and results
```