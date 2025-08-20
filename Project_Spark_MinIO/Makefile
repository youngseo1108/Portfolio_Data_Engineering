SHELL := /bin/bash

.PHONY: up buckets ingest features check run_all venv

# 0) Optional: create venv and install deps (run once)
venv:
	python3 -m venv venv
	. venv/bin/activate && python -m pip install -U pip
	. venv/bin/activate && pip install boto3 python-dotenv s3fs pandas pyarrow pyspark

# 1) Start MinIO (compose already reads ../config/.env)
up:
	docker compose -f infra/docker-compose.yaml up -d

# 2) Create buckets (once)
buckets:
	. venv/bin/activate && python scripts/create_buckets.py

# 3) Upload sample data to raw/
ingest:
	. venv/bin/activate && python scripts/ingest_to_s3.py

# 4) Build features with Spark (choose ONE of the two lines)
features:
	. venv/bin/activate && python scripts/build_features_spark.py

# 5) Quick check
check:
	. venv/bin/activate && python scripts/quick_check.py

# 6) All together
run_all: up buckets ingest features check