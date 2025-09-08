include .env
export

.PHONY: up down load_parquet_minio ingest bench llm plot
FILE ?= yellow_tripdata_2025-01.parquet

up:
	@docker compose up -d
	@echo "postgres → localhost:${POSTGRES_PORT} | minio console → http://localhost:${MINIO_CONSOLE_PORT}"

down:
	@docker compose down

load_parquet_minio:
	# Bucket: create if missing else skipping
	@docker compose run --rm --entrypoint /bin/sh aws -lc "aws --endpoint-url http://minio:${MINIO_PORT} s3api head-bucket --bucket ${MINIO_BUCKET} >/dev/null 2>&1 && echo 'Bucket ${MINIO_BUCKET} already exists. Skipping creation.' || (echo 'Bucket ${MINIO_BUCKET} does not exist. Creating...' && aws --endpoint-url http://minio:${MINIO_PORT} s3 mb s3://${MINIO_BUCKET})"
	# Object: upload if missing else skipping
	@docker compose run --rm --entrypoint /bin/sh aws -lc "aws --endpoint-url http://minio:${MINIO_PORT} s3api head-object --bucket ${MINIO_BUCKET} --key ${FILE} >/dev/null 2>&1 && echo 'File ${FILE} already exists in ${MINIO_BUCKET}. Skipping upload.' || (echo 'Uploading ${FILE} to ${MINIO_BUCKET}...' && aws --endpoint-url http://minio:${MINIO_PORT} s3 cp /work/data/raw/${FILE} s3://${MINIO_BUCKET}/${FILE})"
	# List
	@docker compose run --rm --entrypoint /bin/sh aws -lc "aws --endpoint-url http://minio:${MINIO_PORT} s3 ls s3://${MINIO_BUCKET}/"

ingest:
	@docker compose exec -T app python /work/ingestion/postgres_load.py

bench:
	@docker compose exec -T app python /work/benchmarks/run_benchmarks.py

plot:
	@docker compose exec -T app python /work/plots/plot_results.py