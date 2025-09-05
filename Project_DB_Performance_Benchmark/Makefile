include .env
export

up:
	docker compose up -d
	@echo "postgres → localhost:${POSTGRES_PORT} | minio → http://localhost:${MINIO_CONSOLE_PORT}"

down:
	docker compose down

bucket:
	docker compose exec -T app bash -lc "
  	export AWS_ACCESS_KEY_ID=${MINIO_ROOT_USER} \
         	 AWS_SECRET_ACCESS_KEY=${MINIO_ROOT_PASSWORD} \
         AWS_DEFAULT_REGION=us-east-1 && \
  aws --endpoint-url http://minio:${MINIO_PORT} s3 mb s3://raw || true && \
  aws --endpoint-url http://minio:${MINIO_PORT} s3 cp /work/data/raw/yellow_tripdata_2025-01.parquet s3://raw/yellow_tripdata_2025-01.parquet && \
  aws --endpoint-url http://minio:${MINIO_PORT} s3 ls s3://raw/
"

ingest:
	docker compose exec -T postgres psql -U ${POSTGRES_USER} -d ${POSTGRES_DB} -f /work/ingestion/postgres_load.py

bench:
	docker compose exec -T app python benchmarks/run_benchmarks.py

llm:
	docker compose exec -T app python genai/llm_sql_tuning.py

plot:
	docker compose exec -T app python plots/plot_results.py