SHELL := /bin/bash

.PHONY: up buckets ingest features check run_all venv

# (run once) Create venv and install deps
venv:
	python3 -m venv venv
	. venv/bin/activate && python -m pip install -U pip
	. venv/bin/activate && pip install boto3 python-dotenv s3fs pandas pyarrow pyspark

# Start MinIO (compose reads ./config/.env)
up:
	docker compose -f docker-compose.yaml up -d

# (once) Create buckets 
buckets:
	. venv/bin/activate && python scripts/create_buckets.py

# Upload sample data to raw/
ingest:
	. venv/bin/activate && python scripts/ingest_to_s3.py

# Build features with Spark (choose ONE of the two lines)
features:
	. venv/bin/activate && python scripts/build_features_spark.py

# Quick check
check:
	. venv/bin/activate && python scripts/quick_check.py

# Run altogether
run_all: up buckets ingest features check