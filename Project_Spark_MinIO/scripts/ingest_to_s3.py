from pathlib import Path
from dotenv import load_dotenv
import os
import pandas as pd
import s3fs
import sys

# Load .env from Project_Spark_MinIO/config/.env
ENV_PATH = Path(__file__).resolve().parents[1] / "config" / ".env"
if not ENV_PATH.exists():
    print(f"[ERR] .env not found at {ENV_PATH}")
    sys.exit(1)
load_dotenv(ENV_PATH)

S3_ENDPOINT = os.getenv("S3_ENDPOINT", "http://localhost:9000")
RAW_BUCKET = os.getenv("S3_BUCKET_RAW", "raw")
URL = os.getenv("DATASET_URL")
AK = os.getenv("MINIO_ROOT_USER", "admin")
SK = os.getenv("MINIO_ROOT_PASSWORD", "supersecret")

# sanity prints
print("S3_ENDPOINT:", S3_ENDPOINT)
print("RAW_BUCKET :", RAW_BUCKET)
print("DATASET_URL:", URL)

if not URL:
    print("[ERR] DATASET_URL is not set (got None). Check config/.env")
    sys.exit(1)

def main():
    # Load a manageable sample
    df = pd.read_parquet(URL)
    n = min(100_000, len(df))
    if len(df) > n:
        df = df.sample(n, random_state=42)

    fs = s3fs.S3FileSystem(
        client_kwargs={"endpoint_url": S3_ENDPOINT},
        key=AK, secret=SK,
    )
    dest = f"{RAW_BUCKET}/nyc_taxi/2024/01/data.parquet"
    with fs.open(dest, "wb") as f:
        df.to_parquet(f, index=False)

    print("uploaded:", dest, "rows:", len(df))

if __name__ == "__main__":
    main()
