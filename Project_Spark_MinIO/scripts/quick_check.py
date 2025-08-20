import os, s3fs, pandas as pd

EP = os.getenv("S3_ENDPOINT")
AK = os.getenv("MINIO_ROOT_USER")
SK = os.getenv("MINIO_ROOT_PASSWORD")
FEAT = os.getenv("S3_BUCKET_FEAT","feature")

def main():
  fs = s3fs.S3FileSystem(client_kwargs={"endpoint_url": EP}, key=AK, secret=SK)
  
  # Validate by reading one of the data
  files = fs.glob(f"{FEAT}/nyc_taxi/2024/01/agg/*.parquet")
  with fs.open(files[0],"rb") as f:
    agg = pd.read_parquet(f)
  assert {"pickup_hour","n","avg_fare","avg_dist"}.issubset(agg.columns)
  print("rows:", len(agg), "\nhead:\n", agg.head())

if __name__ == "__main__":
  main()
