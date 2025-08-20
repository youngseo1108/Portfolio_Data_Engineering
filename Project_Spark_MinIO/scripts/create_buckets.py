from pathlib import Path
from dotenv import load_dotenv
import os, boto3, botocore

# 1) .env 로드
ENV_PATH = Path(__file__).resolve().parents[1] / "config" / ".env"
load_dotenv(ENV_PATH)

ENDPOINT = os.getenv("S3_ENDPOINT", "http://localhost:9000")
AK = os.getenv("MINIO_ROOT_USER", "admin")
SK = os.getenv("MINIO_ROOT_PASSWORD", "supersecret")
RAW = os.getenv("S3_BUCKET_RAW", "raw")
FEAT = os.getenv("S3_BUCKET_FEAT", "feature")

s3 = boto3.client(
    "s3",
    endpoint_url=ENDPOINT,
    aws_access_key_id=AK,
    aws_secret_access_key=SK,
    region_name="us-east-1",  # MinIO는 리전 의미 없음(형식상 필요)
)

def ensure_bucket(name: str):
    try:
        s3.head_bucket(Bucket=name)
        print(f"exists: {name}")
    except botocore.exceptions.ClientError as e:
        code = e.response.get("Error", {}).get("Code", "")
        if code in ("404", "NoSuchBucket", "404 Not Found", "NotFound"):
            s3.create_bucket(Bucket=name)
            print(f"created: {name}")
        else:
            raise

def list_objects_safe(name: str, prefix: str = ""):
    try:
        resp = s3.list_objects_v2(Bucket=name, Prefix=prefix)
        contents = resp.get("Contents")
        if not contents:
            print(f"(no objects in {name}/{prefix})")
        else:
            for obj in contents:
                print(" -", obj["Key"], f"(size={obj['Size']})")
    except botocore.exceptions.ClientError as e:
        print(f"list failed for {name}: {e}")

if __name__ == "__main__":
    # 2) 버킷 보장
    for b in (RAW, FEAT):
        ensure_bucket(b)

    # 3) 최종 확인
    print("\nBuckets:")
    for b in s3.list_buckets().get("Buckets", []):
        print(" -", b["Name"])

    print(f"\nObjects in '{RAW}':")
    list_objects_safe(RAW)